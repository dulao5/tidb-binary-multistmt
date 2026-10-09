//go:build integration

package binarymultistmt

import (
	"context"
	"database/sql"
	"testing"
)

// TestIntegration_ExpandInSelectsMultipleRows confirms ExpandIn's expanded
// SQL/args actually work end to end: a variable-length IN-list, PREPAREd
// and pipelined-EXECUTEd against real TiDB, selects exactly the rows it
// should.
func TestIntegration_ExpandInSelectsMultipleRows(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_expandin")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	for i := 1; i <= 5; i++ {
		if _, err := db.Exec("INSERT INTO tbms_expandin (id, val) VALUES (?, ?)", i, "v"); err != nil {
			t.Fatalf("seed insert: %v", err)
		}
	}

	sql_, args, err := ExpandIn("SELECT val FROM tbms_expandin WHERE id IN (?)", []any{[]int{1, 3, 5}})
	if err != nil {
		t.Fatalf("ExpandIn: %v", err)
	}

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	var gotRows int
	b := NewBatch()
	b.Add(sql_, args, func(sr *StatementResult) {
		for row := sr.Rows.Next(); row != nil; row = sr.Rows.Next() {
			gotRows++
		}
	})
	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected the expanded IN-list SELECT to succeed")
	}
	if gotRows != 3 {
		t.Fatalf("expected 3 rows (ids 1,3,5), got %d", gotRows)
	}
}

// TestIntegration_ExpandValuesBulkInsert confirms ExpandValues' expanded
// multi-row INSERT actually inserts every row.
func TestIntegration_ExpandValuesBulkInsert(t *testing.T) {
	dsn := testDSN(t)
	setupTable(t, dsn, "tbms_expandvalues")

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	rows := [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	}
	sql_, args, err := ExpandValues("INSERT INTO tbms_expandvalues (id, val) VALUES (?, ?)", rows)
	if err != nil {
		t.Fatalf("ExpandValues: %v", err)
	}

	b := NewBatch()
	b.Add(sql_, args, nil)
	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected the expanded bulk INSERT to succeed")
	}

	if got, want := rowCount(t, dsn, "tbms_expandvalues"), 3; got != want {
		t.Fatalf("expected %d rows inserted, got %d", want, got)
	}
}
