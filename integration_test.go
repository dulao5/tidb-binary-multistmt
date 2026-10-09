//go:build integration

package binarymultistmt

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
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
		t.Fatalf("expected all statements to succeed")
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

	var errs [3]error
	recordErr := func(i int) func(*StatementResult) {
		return func(sr *StatementResult) { errs[i] = sr.Err }
	}

	b := NewBatch()
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(1), "ok"}, recordErr(0))
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(1), "dup"}, recordErr(1)) // duplicate PK
	b.Add("INSERT INTO tbms_insert_fail (id, val) VALUES (?, ?)", []any{int64(2), "after-failure"}, recordErr(2))

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.AllSucceeded {
		t.Fatalf("expected the duplicate-key insert to fail")
	}
	if errs[1] == nil {
		t.Fatalf("expected statement #1 (the duplicate) to report an error")
	}
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("expected only statement #1 to fail, got %+v", errs)
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

	var firstSelectRows, secondSelectRows int
	b := NewBatch()
	b.Add("SELECT val FROM tbms_select_sync WHERE id = ?", []any{int64(1)}, func(sr *StatementResult) {
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			firstSelectRows++
		}
	})
	b.Add("INSERT INTO tbms_select_sync (id, val) VALUES (?, ?)", []any{int64(4), "after-select"}, nil)
	b.Add("SELECT val FROM tbms_select_sync WHERE id BETWEEN ? AND ?", []any{int64(1), int64(4)}, func(sr *StatementResult) {
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			secondSelectRows++
		}
	})

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected all statements to succeed")
	}
	if firstSelectRows != 1 {
		t.Fatalf("expected 1 row from the first SELECT, got %d", firstSelectRows)
	}
	if secondSelectRows != 4 {
		t.Fatalf("expected 4 rows from the second SELECT, got %d — likely a response desync", secondSelectRows)
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
		t.Fatalf("expected success")
	}
	if got, want := rowCount(t, dsn, "tbms_syntax_error"), 1; got != want {
		t.Fatalf("expected 1 row, got %d", got)
	}
}

// TestIntegration_AutoCommitDoesNotRollBackEarlierStatements confirms
// ExecuteAutoCommit's core semantic difference from Execute: with no BEGIN,
// each statement commits independently as it runs, so a later statement's
// failure does not undo an earlier statement's success — there is nothing
// to roll back, on purpose.
func TestIntegration_AutoCommitDoesNotRollBackEarlierStatements(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_autocommit")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	b := NewBatch()
	b.Add("INSERT INTO tbms_autocommit (id, val) VALUES (?, ?)", []any{int64(1), "ok"}, nil)
	b.Add("INSERT INTO tbms_autocommit (id, val) VALUES (?, ?)", []any{int64(1), "dup"}, nil) // duplicate PK

	res, err := conn.ExecuteAutoCommit(ctx, b)
	if err != nil {
		t.Fatalf("ExecuteAutoCommit: %v", err)
	}
	if res.AllSucceeded {
		t.Fatalf("expected the duplicate-key insert to fail")
	}

	// No Rollback call here — ExecuteAutoCommit never opened a transaction.
	// The first INSERT must still be visible.
	if got, want := rowCount(t, dsn, "tbms_autocommit"), 1; got != want {
		t.Fatalf("expected the first statement's insert to have committed on its own despite the second failing, got %d rows", got)
	}
}

// withOptimisticTxnMode appends a tidb_txn_mode=optimistic system-variable
// param to dsn — go-sql-driver/mysql executes unrecognized DSN params as
// SET session variables right after connecting, so this configures the
// hijacked connection's session before Dial's handshake even finishes.
func withOptimisticTxnMode(dsn string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "tidb_txn_mode=optimistic"
}

// TestIntegration_CommitRejectionKeepsConnReusable reproduces a genuine
// write conflict (two optimistic-mode Conns racing an UPDATE on the same
// row) and confirms Execute reports it as a *CommitError — not a
// connection/protocol-level failure — and that the Conn whose COMMIT was
// rejected is immediately reusable for another Execute call, with no Close/
// Dial or Rollback needed. The race is timing-dependent, so it retries a
// few times before giving up; TestExecute_CommitRejectionReturnsCommitErrorAndKeepsConnHealthy
// covers this package's own handling of a rejected COMMIT deterministically.
func TestIntegration_CommitRejectionKeepsConnReusable(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_commit_conflict")

	plain, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer plain.Close()
	if _, err := plain.Exec("INSERT INTO tbms_commit_conflict (id, val) VALUES (1, 'seed')"); err != nil {
		t.Fatalf("seed insert: %v", err)
	}

	ctx := context.Background()
	optimisticDSN := withOptimisticTxnMode(dsn)

	var commitErr *CommitError
	var usableConn *Conn
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts && commitErr == nil; attempt++ {
		connA, err := Dial(ctx, optimisticDSN)
		if err != nil {
			t.Fatalf("Dial A: %v", err)
		}
		connB, err := Dial(ctx, optimisticDSN)
		if err != nil {
			t.Fatalf("Dial B: %v", err)
		}

		var wg sync.WaitGroup
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			bA := NewBatch().Add("UPDATE tbms_commit_conflict SET val = ? WHERE id = 1", []any{"a"}, nil)
			_, errA = connA.Execute(ctx, bA)
		}()
		go func() {
			defer wg.Done()
			bB := NewBatch().Add("UPDATE tbms_commit_conflict SET val = ? WHERE id = 1", []any{"b"}, nil)
			_, errB = connB.Execute(ctx, bB)
		}()
		wg.Wait()

		switch {
		case errors.As(errA, &commitErr):
			usableConn = connA
			connB.Close()
		case errors.As(errB, &commitErr):
			usableConn = connB
			connA.Close()
		default:
			connA.Close()
			connB.Close()
		}
	}

	if commitErr == nil {
		t.Skipf("could not reproduce a real write conflict after %d attempts (timing-dependent)", maxAttempts)
	}
	defer usableConn.Close()

	if usableConn.broken {
		t.Fatalf("expected the Conn whose COMMIT was rejected to remain usable (not broken)")
	}

	// Prove it for real, not just via the broken flag: run another Execute
	// on the same Conn and confirm it still works.
	b2 := NewBatch().Add("INSERT INTO tbms_commit_conflict (id, val) VALUES (?, ?)", []any{int64(2), "after-conflict"}, nil)
	res2, err2 := usableConn.Execute(ctx, b2)
	if err2 != nil {
		t.Fatalf("Execute after *CommitError: %v", err2)
	}
	if !res2.AllSucceeded {
		t.Fatalf("expected the post-conflict Execute to succeed, proving the Conn is genuinely reusable")
	}
}
