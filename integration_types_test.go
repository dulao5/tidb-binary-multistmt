//go:build integration

package binarymultistmt

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

// TestIntegration_AllSupportedParamTypesRoundTrip inserts one row binding
// every type buildExecutePayload supports, then reads it back through a
// normal driver connection (this package doesn't decode result sets yet —
// see the result-set-decoding issue) to confirm TiDB actually understood
// the encoded values, not just that this package's own encode/decode is
// internally self-consistent.
func TestIntegration_AllSupportedParamTypesRoundTrip(t *testing.T) {
	dsn := testDSN(t)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const table = "tbms_types_roundtrip"
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE ` + table + ` (
		id INT PRIMARY KEY,
		b BOOL,
		i64 BIGINT,
		u64 BIGINT UNSIGNED,
		f64 DOUBLE,
		s VARCHAR(100),
		blob_val VARBINARY(100),
		n_blob VARBINARY(100),
		t DATETIME(6)
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	wantTime := time.Date(2026, 3, 4, 5, 6, 7, 123000000, time.UTC)

	b := NewBatch()
	b.Add(
		"INSERT INTO "+table+" (id, b, i64, u64, f64, s, blob_val, n_blob, t) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		[]any{
			int64(1),
			true,
			int64(-123456789),
			uint64(18446744073709551615), // max uint64
			3.5,
			"hello, world",
			[]byte{0xDE, 0xAD, 0xBE, 0xEF},
			[]byte(nil),
			wantTime,
		},
		false,
	)

	res, err := conn.Execute(ctx, b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected success, got %+v", res.Results)
	}

	var (
		gotB     bool
		gotI64   int64
		gotU64   uint64
		gotF64   float64
		gotS     string
		gotBlob  []byte
		gotNBlob sql.NullString
		gotT     time.Time
	)
	row := db.QueryRowContext(ctx, "SELECT b, i64, u64, f64, s, blob_val, n_blob, t FROM "+table+" WHERE id = ?", 1)
	if err := row.Scan(&gotB, &gotI64, &gotU64, &gotF64, &gotS, &gotBlob, &gotNBlob, &gotT); err != nil {
		t.Fatalf("scan: %v", err)
	}

	if !gotB {
		t.Errorf("bool: expected true, got false")
	}
	if gotI64 != -123456789 {
		t.Errorf("int64: expected -123456789, got %d", gotI64)
	}
	if gotU64 != 18446744073709551615 {
		t.Errorf("uint64: expected max uint64, got %d", gotU64)
	}
	if gotF64 != 3.5 {
		t.Errorf("float64: expected 3.5, got %v", gotF64)
	}
	if gotS != "hello, world" {
		t.Errorf("string: expected %q, got %q", "hello, world", gotS)
	}
	if string(gotBlob) != string([]byte{0xDE, 0xAD, 0xBE, 0xEF}) {
		t.Errorf("[]byte: expected DE AD BE EF, got % x", gotBlob)
	}
	if gotNBlob.Valid {
		t.Errorf("nil []byte: expected NULL, got %q", gotNBlob.String)
	}
	if !gotT.Equal(wantTime) {
		t.Errorf("time.Time: expected %v, got %v", wantTime, gotT)
	}
}
