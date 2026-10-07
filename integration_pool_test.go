//go:build integration

package binarymultistmt

import (
	"context"
	"testing"
	"time"
)

// connectionID runs SELECT CONNECTION_ID() through c and returns the
// server's answer, so tests can tell whether two Conns are the same
// physical connection or not — proof independent of this package's own
// bookkeeping.
func connectionID(t *testing.T, ctx context.Context, c *Conn) uint64 {
	t.Helper()
	b := NewBatch().Add("SELECT CONNECTION_ID()", nil, true)
	res, err := c.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute(SELECT CONNECTION_ID()): %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("SELECT CONNECTION_ID() failed: %+v", res.Results)
	}
	rows := res.Results[0].Result.Rows
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	id, ok := rows[0][0].(uint64)
	if !ok {
		t.Fatalf("expected uint64 connection id, got %T (%v)", rows[0][0], rows[0][0])
	}
	return id
}

func TestIntegration_DBReusesIdleConn(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	db, err := Open(dsn, 2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	c1, err := db.AcquireConn(ctx)
	if err != nil {
		t.Fatalf("AcquireConn 1: %v", err)
	}
	id1 := connectionID(t, ctx, c1)
	if err := c1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	c2, err := db.AcquireConn(ctx)
	if err != nil {
		t.Fatalf("AcquireConn 2: %v", err)
	}
	defer c2.Close()
	id2 := connectionID(t, ctx, c2)

	if id1 != id2 {
		t.Fatalf("expected the same physical connection to be reused (same CONNECTION_ID), got %d then %d", id1, id2)
	}
}

func TestIntegration_DBDiscardsBrokenConn(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	db, err := Open(dsn, 2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	c1, err := db.AcquireConn(ctx)
	if err != nil {
		t.Fatalf("AcquireConn 1: %v", err)
	}
	id1 := connectionID(t, ctx, c1)

	// An invalid PREPARE marks c1 broken (per Execute's doc comment) without
	// actually desyncing the real wire — a convenient stand-in for a real
	// protocol-level failure, exercising the same discard path.
	bad := NewBatch().Add("THIS IS NOT VALID SQL", nil, false)
	if _, err := c1.Execute(ctx, bad); err == nil {
		t.Fatalf("expected Execute with invalid SQL to fail")
	}
	if !c1.broken {
		t.Fatalf("expected c1.broken to be set after a prepare failure")
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	c2, err := db.AcquireConn(ctx)
	if err != nil {
		t.Fatalf("AcquireConn 2: %v", err)
	}
	defer c2.Close()
	id2 := connectionID(t, ctx, c2)

	if id1 == id2 {
		t.Fatalf("expected a broken connection to be discarded, not reused (got the same CONNECTION_ID %d twice)", id1)
	}
}

func TestIntegration_DBBlocksAtMaxConns(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	db, err := Open(dsn, 1)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	c1, err := db.AcquireConn(ctx)
	if err != nil {
		t.Fatalf("AcquireConn 1: %v", err)
	}

	acquired := make(chan *Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c2, err := db.AcquireConn(ctx)
		if err != nil {
			errCh <- err
			return
		}
		acquired <- c2
	}()

	select {
	case <-acquired:
		t.Fatalf("second AcquireConn returned before the first Conn (at maxConns=1) was released")
	case err := <-errCh:
		t.Fatalf("second AcquireConn errored before the first Conn was released: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := c1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	select {
	case c2 := <-acquired:
		c2.Close()
	case err := <-errCh:
		t.Fatalf("second AcquireConn errored: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("second AcquireConn did not unblock after the first Conn was released")
	}
}
