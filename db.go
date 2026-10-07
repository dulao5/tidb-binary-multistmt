package binarymultistmt

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

// dialReqKey is the context key AcquireConn's dial uses to tell the shared
// dial hook which pending request a freshly dialed net.Conn belongs to.
// Needed because multiple AcquireConn calls can be dialing concurrently —
// a single shared capture channel can't tell them apart.
type dialReqKey struct{}

// DB is a pool of hijacked connections. Unlike Dial, which hands out one
// dedicated, single-use connection per call, DB keeps a released Conn
// around in its own idle list (see AcquireConn/release) so a long-running
// caller doesn't pay a fresh dial+auth for every batch. The underlying
// *sql.DB is used only as a dial/auth factory — a healthy Conn is never
// returned to *that* pool, since database/sql has no way to tell this
// package when it later hands such a connection back out, which is exactly
// the identity problem this design avoids.
type DB struct {
	factory  *sql.DB
	maxConns int

	pendingMu sync.Mutex
	pending   map[uint64]chan net.Conn
	nextReqID atomic.Uint64

	mu     sync.Mutex
	idle   []*Conn
	total  int // live hijacked connections: idle + currently checked out
	notify chan struct{}
	closed bool
}

// Open opens dsn (a standard go-sql-driver/mysql DSN) and returns a DB that
// AcquireConn draws hijacked connections from, capped at maxConns
// concurrently live physical connections.
func Open(dsn string, maxConns int) (*DB, error) {
	netName := dialNetworkName()
	db := &DB{
		maxConns: maxConns,
		pending:  make(map[uint64]chan net.Conn),
		notify:   make(chan struct{}),
	}

	mysql.RegisterDialContext(netName, func(ctx context.Context, addr string) (net.Conn, error) {
		d := net.Dialer{}
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		reqID, _ := ctx.Value(dialReqKey{}).(uint64)
		db.pendingMu.Lock()
		ch := db.pending[reqID]
		db.pendingMu.Unlock()
		if ch == nil {
			// Nothing is waiting for this dial — db.dial always registers
			// its channel before calling factory.Conn, so this would mean
			// some other, unexpected caller reached this network name.
			// Close it rather than leak the fd into a channel nobody reads.
			c.Close()
			return nil, fmt.Errorf("binarymultistmt: dial for unknown request id %d (no AcquireConn waiting)", reqID)
		}
		ch <- c
		return c, nil
	})

	hijackDSN := strings.Replace(dsn, "tcp(", netName+"(", 1)
	factory, err := sql.Open("mysql", hijackDSN)
	if err != nil {
		return nil, err
	}
	// factory.SetMaxOpenConns is deliberately left at its default (0,
	// unlimited): this package enforces its own maxConns cap in
	// AcquireConn instead. If factory itself enforced a cap, hitting it
	// would route the dial through database/sql's background connection
	// opener, which dials with the DB's own ambient context rather than
	// the caller's — breaking the context.WithValue correlation dial()
	// relies on to hand a freshly dialed net.Conn back to the right
	// AcquireConn call.
	//
	// MaxIdleConns(0) is a defensive default, not load-bearing: a healthy
	// Conn's poolConn is never Close()d back to factory's pool (see
	// release) in the first place, only idled in db.idle or destroyed.
	factory.SetMaxIdleConns(0)

	db.factory = factory
	return db, nil
}

// AcquireConn returns a hijacked Conn: an idle one from db's own pool if
// one is available, otherwise a freshly dialed (and authenticated) one if
// db is under maxConns, otherwise it blocks — respecting ctx — until
// either becomes true.
func (db *DB) AcquireConn(ctx context.Context) (*Conn, error) {
	for {
		db.mu.Lock()
		if db.closed {
			db.mu.Unlock()
			return nil, fmt.Errorf("binarymultistmt: DB is closed")
		}
		if n := len(db.idle); n > 0 {
			c := db.idle[n-1]
			db.idle = db.idle[:n-1]
			db.mu.Unlock()
			return c, nil
		}
		if db.total < db.maxConns {
			db.total++
			db.mu.Unlock()
			c, err := db.dial(ctx)
			if err != nil {
				db.mu.Lock()
				db.total--
				db.wakeLocked()
				db.mu.Unlock()
				return nil, err
			}
			return c, nil
		}
		wait := db.notify
		db.mu.Unlock()
		select {
		case <-wait:
			// Pool state changed (something idled or was destroyed) — loop
			// and re-check.
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// wakeLocked must be called with db.mu held, after any change to db.idle or
// db.total that a blocked AcquireConn call might be waiting on.
func (db *DB) wakeLocked() {
	close(db.notify)
	db.notify = make(chan struct{})
}

// dial performs one fresh dial+auth through factory, hijacking the result.
// db.total must already have been incremented by the caller for this slot.
func (db *DB) dial(ctx context.Context) (*Conn, error) {
	ch := make(chan net.Conn, 1)
	reqID := db.nextReqID.Add(1)
	db.pendingMu.Lock()
	db.pending[reqID] = ch
	db.pendingMu.Unlock()
	defer func() {
		db.pendingMu.Lock()
		delete(db.pending, reqID)
		db.pendingMu.Unlock()
	}()

	dctx := context.WithValue(ctx, dialReqKey{}, reqID)
	pc, err := db.factory.Conn(dctx)
	if err != nil {
		return nil, err
	}

	var nc net.Conn
	select {
	case nc = <-ch:
	case <-ctx.Done():
		pc.Close()
		return nil, ctx.Err()
	}
	// Same reasoning as Dial: the driver's handshake leaves an absolute
	// read/write deadline on nc; this package manages its own from here,
	// so clear it.
	nc.SetDeadline(time.Time{})

	return &Conn{
		raw:       nc,
		pool:      db,
		poolConn:  pc,
		stmtCache: make(map[string]preparedStmt),
	}, nil
}

// release is called by (*Conn).Close for an AcquireConn-sourced Conn.
func (db *DB) release(c *Conn, broken bool) {
	db.mu.Lock()
	if broken || db.closed {
		db.total--
		db.wakeLocked()
		db.mu.Unlock()
		c.destroy()
		return
	}
	db.idle = append(db.idle, c)
	db.wakeLocked()
	db.mu.Unlock()
}

// Close destroys every currently idle connection and closes the underlying
// dial/auth factory. A Conn already checked out via AcquireConn at the time
// of Close is unaffected until its own Close is called, at which point
// release notices db is closed and destroys it instead of re-idling it.
func (db *DB) Close() error {
	db.mu.Lock()
	db.closed = true
	idle := db.idle
	db.idle = nil
	db.wakeLocked()
	db.mu.Unlock()

	for _, c := range idle {
		c.destroy()
	}
	return db.factory.Close()
}
