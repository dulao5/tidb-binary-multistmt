// Package binarymultistmt pipelines a transaction's statements over MySQL's
// binary protocol (COM_STMT_PREPARE/COM_STMT_EXECUTE) with no per-statement
// network round trip — all of a batch's EXECUTE packets are written
// back-to-back before any response is read, instead of go-sql-driver's (and
// database/sql's) normal write-command-then-synchronously-read-its-response
// convention.
//
// This only works by stepping outside database/sql entirely once the
// connection is established: a custom dial function registered via
// go-sql-driver/mysql's own public mysql.RegisterDialContext hook captures
// the real net.Conn alongside letting the driver do its normal
// handshake/authentication over it. Once that completes, this package reads
// and writes raw bytes on the same socket directly — the *sql.Conn/*sql.DB
// pair is kept open only to hold the connection pool slot and must never be
// used through the driver API again, since this package's raw writes
// permanently desync the driver's internal per-connection sequence-number
// bookkeeping.
//
// See the package README for status, known limitations (most notably: TLS
// is not yet supported — see the TLS-support issue), and the semantics
// around failed batches (Conn.Execute auto-commits only on full success; on
// any failure it returns per-statement results and leaves the transaction
// open for the caller to explicitly Rollback).
package binarymultistmt

import (
	"context"
	"database/sql"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

var dialSeq atomic.Uint64

// dialNetworkName returns a process-wide-unique network name for this Dial
// call's custom dial hook, so concurrent Dial calls never race on a shared
// capture channel.
func dialNetworkName() string {
	return "tbms" + strconv.FormatUint(dialSeq.Add(1), 10)
}

// Conn is one physical connection, hijacked for direct binary-protocol
// pipelining after go-sql-driver/mysql establishes it. Not safe for
// concurrent use — mirrors a single *sql.Conn's single-writer assumption.
type Conn struct {
	raw       net.Conn
	db        *sql.DB
	poolConn  *sql.Conn
	stmtCache map[string]preparedStmt
}

type preparedStmt struct {
	id           uint32
	paramCount   int
	hasResultSet bool
}

// Dial opens dsn (a standard go-sql-driver/mysql DSN) via the driver's
// normal handshake/auth, then hijacks the underlying net.Conn for direct
// binary-protocol use. The returned Conn owns that connection for its
// lifetime; call Close when done.
func Dial(ctx context.Context, dsn string) (*Conn, error) {
	netName := dialNetworkName()
	capture := make(chan net.Conn, 1)
	mysql.RegisterDialContext(netName, func(ctx context.Context, addr string) (net.Conn, error) {
		d := net.Dialer{}
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		capture <- c
		return c, nil
	})

	hijackDSN := strings.Replace(dsn, "tcp(", netName+"(", 1)
	db, err := sql.Open("mysql", hijackDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)

	pc, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	var nc net.Conn
	select {
	case nc = <-capture:
	case <-ctx.Done():
		db.Close()
		return nil, ctx.Err()
	}
	// The driver's handshake sets an absolute SetReadDeadline/
	// SetWriteDeadline (per the DSN's readTimeout/writeTimeout) on this
	// same net.Conn; left in place it expires on its own schedule
	// regardless of when this package actually reads/writes next, causing
	// a spurious i/o timeout. This package manages its own read/write
	// timing from here on, so clear it.
	nc.SetDeadline(time.Time{})

	return &Conn{
		raw:       nc,
		db:        db,
		poolConn:  pc,
		stmtCache: make(map[string]preparedStmt),
	}, nil
}

// Close releases the connection. After Close, the Conn must not be used.
func (c *Conn) Close() error {
	if c.raw != nil {
		c.raw.Close()
	}
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
