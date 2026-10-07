package binarymultistmt

import (
	"errors"
	"reflect"
	"testing"
)

func TestExpandIn_NoSlices_PassesThroughUnchanged(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE a = ? AND b = ?", []any{1, "x"})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE a = ? AND b = ?" {
		t.Fatalf("sql changed unexpectedly: %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1, "x"}) {
		t.Fatalf("args changed unexpectedly: %#v", args)
	}
}

func TestExpandIn_SingleSlice(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{[]int{1, 2, 3}})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE id IN (?,?,?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1, 2, 3}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_MixedScalarAndSlice(t *testing.T) {
	sql, args, err := ExpandIn(
		"SELECT * FROM t WHERE tenant = ? AND id IN (?) AND status = ?",
		[]any{"acme", []string{"a", "b"}, "active"},
	)
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE tenant = ? AND id IN (?,?) AND status = ?" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{"acme", "a", "b", "active"}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_TwoSlices(t *testing.T) {
	sql, args, err := ExpandIn(
		"SELECT * FROM t WHERE id IN (?) AND grp IN (?)",
		[]any{[]int{1, 2}, []int{9}},
	)
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE id IN (?,?) AND grp IN (?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1, 2, 9}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_ByteSliceNotExpanded(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE blob = ?", []any{[]byte("abc")})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE blob = ?" {
		t.Fatalf("sql = %q, []byte must not be treated as a list", sql)
	}
	if len(args) != 1 {
		t.Fatalf("args = %#v, want exactly one []byte arg", args)
	}
}

func TestExpandIn_EmptySlice_Errors(t *testing.T) {
	_, _, err := ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{[]int{}})
	if !errors.Is(err, ErrEmptyInArgs) {
		t.Fatalf("expected ErrEmptyInArgs, got %v", err)
	}
}

func TestExpandIn_QuestionMarkInsideStringLiteral_NotCountedAsPlaceholder(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE name LIKE '%?%' AND id IN (?)", []any{[]int{1, 2}})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE name LIKE '%?%' AND id IN (?,?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1, 2}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_DoubleQuotedLiteral_NotCountedAsPlaceholder(t *testing.T) {
	sql, args, err := ExpandIn(`SELECT * FROM t WHERE name = "?" AND id IN (?)`, []any{[]int{1}})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != `SELECT * FROM t WHERE name = "?" AND id IN (?)` {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_PlaceholderCountMismatch_Errors(t *testing.T) {
	if _, _, err := ExpandIn("SELECT * FROM t WHERE a = ? AND b = ?", []any{1}); err == nil {
		t.Fatalf("expected an error for too few args")
	}
	if _, _, err := ExpandIn("SELECT * FROM t WHERE a = ?", []any{1, 2}); err == nil {
		t.Fatalf("expected an error for too many args")
	}
}

func TestExpandIn_NoArgs(t *testing.T) {
	sql, args, err := ExpandIn("SELECT COUNT(*) FROM t", nil)
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT COUNT(*) FROM t" {
		t.Fatalf("sql = %q", sql)
	}
	if len(args) != 0 {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandIn_ArrayType(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{[3]int{7, 8, 9}})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}
	if sql != "SELECT * FROM t WHERE id IN (?,?,?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{7, 8, 9}) {
		t.Fatalf("args = %#v", args)
	}
}

// Result of ExpandIn feeds straight into Batch.Add unchanged — confirm the
// two compose correctly end to end (no DB needed; Batch.Add/Statements are
// pure, buildExecutePayload's own type-coverage is tested separately).
func TestExpandIn_ComposesWithBatchAdd(t *testing.T) {
	sql, args, err := ExpandIn("SELECT * FROM t WHERE id IN (?)", []any{[]int{1, 2, 3}})
	if err != nil {
		t.Fatalf("ExpandIn failed: %v", err)
	}

	b := NewBatch()
	b.Add(sql, args, true)
	stmts := b.Statements()
	if len(stmts) != 1 {
		t.Fatalf("expected 1 queued statement, got %d", len(stmts))
	}
	if stmts[0].SQL != "SELECT * FROM t WHERE id IN (?,?,?)" {
		t.Fatalf("sql = %q", stmts[0].SQL)
	}
	if !reflect.DeepEqual(stmts[0].Args, []any{1, 2, 3}) {
		t.Fatalf("args = %#v", stmts[0].Args)
	}
}
