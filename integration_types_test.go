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
		nil,
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

// TestIntegration_SelectDecodesAllSupportedTypes inserts the same shape of
// row as the round-trip test above, then reads it back through this
// package's own Conn.Execute (auto-detected as row-returning) and decoder — no driver
// readback this time — confirming decodeColumnDef/decodeBinaryRow correctly
// understand real column metadata and row bytes TiDB actually sends, not
// just hand-crafted fixtures.
func TestIntegration_SelectDecodesAllSupportedTypes(t *testing.T) {
	dsn := testDSN(t)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("DROP TABLE IF EXISTS tbms_select_decode"); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE tbms_select_decode (
		id INT PRIMARY KEY,
		b BOOL,
		i64 BIGINT,
		u64 BIGINT UNSIGNED,
		f64 DOUBLE,
		s VARCHAR(100),
		n_blob VARBINARY(100),
		t DATETIME(6)
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS tbms_select_decode") })

	ctx := context.Background()
	conn, err := Dial(ctx, dsn)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	wantTime := time.Date(2026, 3, 4, 5, 6, 7, 123000000, time.UTC)

	insert := NewBatch()
	insert.Add(
		"INSERT INTO tbms_select_decode (id, b, i64, u64, f64, s, n_blob, t) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		[]any{int64(1), true, int64(-123456789), uint64(18446744073709551615), 3.5, "hello, world", []byte(nil), wantTime},
		nil,
	)
	if res, err := conn.Execute(ctx, insert); err != nil || !res.AllSucceeded {
		t.Fatalf("insert: err=%v res=%+v", err, res)
	}

	sel := NewBatch()
	sel.Add("SELECT b, i64, u64, f64, s, n_blob, t FROM tbms_select_decode WHERE id = ?", []any{int64(1)}, nil)
	res, err := conn.Execute(ctx, sel)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected success, got %+v", res.Results)
	}

	rs := res.Results[0].Result
	if rs == nil {
		t.Fatalf("expected a decoded ResultSet, got nil")
	}
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rs.Rows))
	}
	wantCols := []string{"b", "i64", "u64", "f64", "s", "n_blob", "t"}
	if len(rs.Columns) != len(wantCols) {
		t.Fatalf("expected %d columns, got %d", len(wantCols), len(rs.Columns))
	}
	for i, name := range wantCols {
		if rs.Columns[i].Name != name {
			t.Errorf("column %d: expected name %q, got %q", i, name, rs.Columns[i].Name)
		}
	}
	if !rs.Columns[2].Unsigned {
		t.Errorf("u64 column: expected Unsigned=true")
	}

	row := rs.Rows[0]
	if got, ok := row[0].(int64); !ok || got != 1 { // BOOL/TINYINT comes back as int64
		t.Errorf("b: got %#v, want int64(1)", row[0])
	}
	if got, ok := row[1].(int64); !ok || got != -123456789 {
		t.Errorf("i64: got %#v, want int64(-123456789)", row[1])
	}
	if got, ok := row[2].(uint64); !ok || got != 18446744073709551615 {
		t.Errorf("u64: got %#v, want max uint64", row[2])
	}
	if got, ok := row[3].(float64); !ok || got != 3.5 {
		t.Errorf("f64: got %#v, want float64(3.5)", row[3])
	}
	if got, ok := row[4].([]byte); !ok || string(got) != "hello, world" {
		t.Errorf("s: got %#v, want []byte(\"hello, world\")", row[4])
	}
	if row[5] != nil {
		t.Errorf("n_blob: got %#v, want nil (NULL)", row[5])
	}
	gotTime, ok := row[6].(time.Time)
	if !ok || !gotTime.Equal(wantTime) {
		t.Errorf("t: got %#v, want %v", row[6], wantTime)
	}
}
