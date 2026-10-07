package binarymultistmt

import (
	"reflect"
	"strings"
	"testing"
)

func TestExpandValues_Basic(t *testing.T) {
	sql, args, err := ExpandValues(
		"INSERT INTO accounts (id, balance) VALUES (?, ?)",
		[][]any{{1, 100}, {2, 200}, {3, 300}},
	)
	if err != nil {
		t.Fatalf("ExpandValues failed: %v", err)
	}
	want := "INSERT INTO accounts (id, balance) VALUES (?,?),(?,?),(?,?)"
	if sql != want {
		t.Fatalf("sql = %q, want %q", sql, want)
	}
	if !reflect.DeepEqual(args, []any{1, 100, 2, 200, 3, 300}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandValues_SingleRow(t *testing.T) {
	sql, args, err := ExpandValues("INSERT INTO t (a) VALUES (?)", [][]any{{1}})
	if err != nil {
		t.Fatalf("ExpandValues failed: %v", err)
	}
	if sql != "INSERT INTO t (a) VALUES (?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandValues_SingleColumn(t *testing.T) {
	sql, args, err := ExpandValues("INSERT INTO t (a) VALUES (?)", [][]any{{1}, {2}})
	if err != nil {
		t.Fatalf("ExpandValues failed: %v", err)
	}
	if sql != "INSERT INTO t (a) VALUES (?),(?)" {
		t.Fatalf("sql = %q", sql)
	}
	if !reflect.DeepEqual(args, []any{1, 2}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestExpandValues_EmptyRows_Errors(t *testing.T) {
	if _, _, err := ExpandValues("INSERT INTO t (a) VALUES (?)", nil); err == nil {
		t.Fatalf("expected an error for empty rows")
	}
}

func TestExpandValues_RowArityMismatch_Errors(t *testing.T) {
	_, _, err := ExpandValues("INSERT INTO t (a, b) VALUES (?, ?)", [][]any{{1, 2}, {3}})
	if err == nil {
		t.Fatalf("expected an error for a row with the wrong number of values")
	}
	if !strings.Contains(err.Error(), "row 1") {
		t.Fatalf("expected the error to name the offending row, got %v", err)
	}
}

func TestExpandValues_NoTemplateFound_Errors(t *testing.T) {
	_, _, err := ExpandValues("INSERT INTO t (a, b) SELECT x, y FROM src", [][]any{{1, 2}})
	if err == nil {
		t.Fatalf("expected an error when there's no placeholder-only group")
	}
}

func TestExpandValues_ColumnListNotMistakenForTemplate(t *testing.T) {
	// "(id, balance)" contains identifiers, not just "?", so it must not be
	// picked as the template; only "(?, ?)" qualifies.
	sql, _, err := ExpandValues("INSERT INTO accounts (id, balance) VALUES (?, ?)", [][]any{{1, 2}})
	if err != nil {
		t.Fatalf("ExpandValues failed: %v", err)
	}
	if !strings.Contains(sql, "(id, balance)") {
		t.Fatalf("column list must be left untouched: %q", sql)
	}
}

func TestExpandValues_AmbiguousMultipleTemplates_Errors(t *testing.T) {
	_, _, err := ExpandValues("INSERT INTO t (a) VALUES (?); INSERT INTO t2 (b) VALUES (?)", [][]any{{1}})
	if err == nil {
		t.Fatalf("expected an error for two placeholder-only groups")
	}
}

func TestExpandValues_QuestionMarkInStringLiteral_NotMistakenForTemplate(t *testing.T) {
	// "(?, '(?)')" is not placeholder-only (it contains a quoted literal
	// with parens inside it, correctly ignored as paren nesting because
	// they're inside a string), so there is no qualifying template at all —
	// confirm that's reported as an error, not silently mishandled.
	_, _, err := ExpandValues("INSERT INTO t (a, note) VALUES (?, '(?)')", [][]any{{1}, {2}})
	if err == nil {
		t.Fatalf("expected an error: no placeholder-only group in this sqlText")
	}
}

func TestFindPlaceholderTuple_Simple(t *testing.T) {
	open, close, n, err := findPlaceholderTuple("INSERT INTO t (a, b) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("findPlaceholderTuple failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("n = %d, want 2", n)
	}
	if open != strings.LastIndex("INSERT INTO t (a, b) VALUES (?, ?)", "(") {
		t.Fatalf("open = %d", open)
	}
	if close != len("INSERT INTO t (a, b) VALUES (?, ?)")-1 {
		t.Fatalf("close = %d", close)
	}
}
