//go:build integration

package binarymultistmt

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func testDSN(t *testing.T) string {
	dsn := os.Getenv("TIDB_BINARY_MULTISTMT_TEST_DSN")
	if dsn == "" {
		t.Skip("TIDB_BINARY_MULTISTMT_TEST_DSN not set, skipping integration test")
	}
	return dsn
}

// setupTable drops and recreates a throwaway table via a normal driver
// connection (not the hijacked path — this is just test fixture setup).
func setupTable(t *testing.T, dsn, table string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, val VARCHAR(100))", table)); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})
}

func rowCount(t *testing.T, dsn, table string) int {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	var n int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

func TestIntegration_PipelinedInsertsCommit(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_insert_commit")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	b := NewBatch()
	for i := 1; i <= 5; i++ {
		b.Add("INSERT INTO tbms_insert_commit (id, val) VALUES (?, ?)", []any{int64(i), fmt.Sprintf("v%d", i)}, nil)
	}

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected all statements to succeed, got %+v", res.Results)
	}

	if got, want := rowCount(t, dsn, "tbms_insert_commit"), 5; got != want {
		t.Fatalf("expected %d rows committed, got %d", want, got)
	}
}

func TestIntegration_FailureLeavesTransactionOpenForCallerRollback(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_insert_fail")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	b := NewBatch()
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(1), "ok"}, nil)
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(1), "dup"}, nil) // duplicate PK
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(2), "after-failure"}, nil)

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.AllSucceeded {
		t.Fatalf("expected the duplicate-key insert to fail")
	}
	if res.Results[1].Err == nil {
		t.Fatalf("expected statement #1 (the duplicate) to report an error")
	}
	if res.Results[0].Err != nil || res.Results[2].Err != nil {
		t.Fatalf("expected only statement #1 to fail, got %+v", res.Results)
	}

	// Nothing committed yet — Execute must not have auto-rolled-back either,
	// per the package contract: the transaction is still open on conn.
	if err := conn.Rollback(ctx); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if got, want := rowCount(t, dsn, "tbms_insert_fail"), 0; got != want {
		t.Fatalf("expected 0 rows after rollback, got %d", got)
	}
}

func TestIntegration_SelectDoesNotDesyncLaterStatements(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_select_sync")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	seed := NewBatch()
	for i := 1; i <= 3; i++ {
		seed.Add("INSERT INTO tbms_select_sync (id, val) VALUES (?, ?)", []any{int64(i), fmt.Sprintf("v%d", i)}, nil)
	}
	if res, err := conn.Execute(ctx, seed); err != nil || !res.AllSucceeded {
		t.Fatalf("seed insert failed: err=%v res=%+v", err, res)
	}

	b := NewBatch()
	b.Add("SELECT val FROM tbms_select_sync WHERE id = ?", []any{int64(1)}, nil)
	b.Add("INSERT INTO tbms_select_sync (id, val) VALUES (?, ?)", []any{int64(4), "after-select"}, nil)
	b.Add("SELECT val FROM tbms_select_sync WHERE id BETWEEN ? AND ?", []any{int64(1), int64(4)}, nil)

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected all statements to succeed, got %+v", res.Results)
	}

	if got, want := rowCount(t, dsn, "tbms_select_sync"), 4; got != want {
		t.Fatalf("expected 4 rows after the batch, got %d — likely a response desync from the SELECTs", got)
	}
}

// TestIntegration_SyntaxErrorFailsOnlyThatStatement confirms a PREPARE-time
// failure (a malformed statement, as opposed to the runtime duplicate-key
// failure TestIntegration_FailureLeavesTransactionOpenForCallerRollback
// covers) is reported precisely — Execute itself returns a non-nil error
// (prepare happens before anything is pipelined, so this is correctly a
// connection-usable "this one statement's SQL is bad" error, not a
// protocol-level one) and conn is still perfectly usable afterward.
func TestIntegration_SyntaxErrorFailsOnlyThatStatement(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_syntax_error")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	bad := NewBatch()
	bad.Add("INSERT INTO tbms_syntax_error (id, val) VALUES (?, ?)", []any{int64(1), "ok"}, nil)
	bad.Add("THIS IS NOT VALID SQL (?, ?)", []any{int64(2), "bad"}, nil)

	if _, err := conn.Execute(ctx, bad); err == nil {
		t.Fatalf("expected Execute to fail on the malformed statement")
	}

	// conn must still be usable: the failure happened during prepare, before
	// BEGIN was ever sent, so there's no stray open transaction to clean up.
	good := NewBatch()
	good.Add("INSERT INTO tbms_syntax_error (id, val) VALUES (?, ?)", []any{int64(1), "ok"}, nil)
	res, err := conn.Execute(ctx, good)
	if err != nil {
		t.Fatalf("Execute after a prior syntax error: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected success, got %+v", res.Results)
	}
	if got, want := rowCount(t, dsn, "tbms_syntax_error"), 1; got != want {
		t.Fatalf("expected 1 row, got %d", got)
	}
}
