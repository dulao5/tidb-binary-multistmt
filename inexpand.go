package binarymultistmt

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// ErrEmptyInArgs is returned by ExpandIn when a slice/array-valued arg has
// zero elements: "IN ()" is not valid SQL, and silently turning it into
// "IN (NULL)" (as some libraries do) would subtly change the statement's
// result (NULL never matches, but is also not what an empty-list caller
// usually means), so ExpandIn refuses instead of guessing.
var ErrEmptyInArgs = errors.New("binarymultistmt: ExpandIn: a slice/array arg has no elements")

// ExpandIn rewrites sqlText's "?" placeholders so that each one whose
// corresponding arg is a slice or array becomes a comma-separated run of "?"
// matching that slice's length, and returns the flattened arg list to
// match — turning a single "IN (?)" placeholder plus a []int{1,2,3} arg into
// "IN (?,?,?)" plus three separate int args. The surrounding "(" ")" around
// the single "?" in "IN (?)" is ordinary SQL text the caller already wrote
// (exactly as with a non-slice "IN (?)"); ExpandIn only ever rewrites the
// "?" itself, so IN is the only clause callers reasonably use this on, but
// nothing here is IN-specific.
//
// This exists because Statement.Args binds positionally, one Go value per
// "?" (see Statement.SQL) — there is no way to express a variable-length
// value list without first expanding the placeholder text itself, and doing
// that expansion is what determines the actual SQL text PREPAREd on the
// server (and the cache key in Conn's internal per-connection PREPARE
// cache: two calls with different-length slices PREPARE as two distinct
// statements, which is correct — a placeholder count has to match the
// IN-list it binds — but means a wildly varying slice length defeats
// prepared-statement reuse for that statement; callers who care should
// round the slice up to a fixed set of bucket sizes themselves before
// calling ExpandIn).
//
// Call ExpandIn before Batch.Add; it does not touch a Batch or the server,
// so its result can be inspected or reused independently of this package's
// execution path (e.g. in a table-driven test, or cached by the caller
// alongside the slice length that produced it).
//
// args is scanned left to right in lockstep with sqlText's "?" occurrences
// (skipping any "?" that appears inside a '...' or "..." string literal, so
// a literal question mark in a LIKE pattern or similar is left alone and
// does not consume an arg). A []byte arg is never expanded — it is treated
// as a single opaque value, matching database/sql's own convention for
// binary data — only other slice/array kinds trigger expansion. It is an
// error if the number of (non-literal) "?" placeholders does not match
// len(args), or if any slice/array arg is empty (ErrEmptyInArgs).
func ExpandIn(sqlText string, args []any) (string, []any, error) {
	placeholders, err := countPlaceholders(sqlText)
	if err != nil {
		return "", nil, err
	}
	if placeholders != len(args) {
		return "", nil, fmt.Errorf("binarymultistmt: ExpandIn: sqlText has %d placeholder(s) but %d arg(s) were given", placeholders, len(args))
	}

	needsExpansion := false
	for _, a := range args {
		if isExpandableSlice(a) {
			needsExpansion = true
			break
		}
	}
	if !needsExpansion {
		return sqlText, args, nil
	}

	var out strings.Builder
	flat := make([]any, 0, len(args))
	argIdx := 0

	inSingle, inDouble := false, false
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch {
		case inSingle:
			out.WriteByte(c)
			if c == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			out.WriteByte(c)
			if c == '"' {
				inDouble = false
			}
			continue
		case c == '\'':
			inSingle = true
			out.WriteByte(c)
			continue
		case c == '"':
			inDouble = true
			out.WriteByte(c)
			continue
		case c != '?':
			out.WriteByte(c)
			continue
		}

		arg := args[argIdx]
		argIdx++

		if !isExpandableSlice(arg) {
			out.WriteByte('?')
			flat = append(flat, arg)
			continue
		}

		v := reflect.ValueOf(arg)
		n := v.Len()
		if n == 0 {
			return "", nil, fmt.Errorf("%w (placeholder #%d)", ErrEmptyInArgs, argIdx-1)
		}
		for k := 0; k < n; k++ {
			if k > 0 {
				out.WriteByte(',')
			}
			out.WriteByte('?')
			flat = append(flat, v.Index(k).Interface())
		}
	}

	return out.String(), flat, nil
}

// countPlaceholders counts sqlText's "?" placeholders, the same way
// ExpandIn's main scan does (skipping those inside '...'/"..." literals), so
// ExpandIn can validate len(args) before doing any rewriting.
func countPlaceholders(sqlText string) (int, error) {
	count := 0
	inSingle, inDouble := false, false
	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			}
		case inDouble:
			if c == '"' {
				inDouble = false
			}
		case c == '\'':
			inSingle = true
		case c == '"':
			inDouble = true
		case c == '?':
			count++
		}
	}
	if inSingle || inDouble {
		return 0, fmt.Errorf("binarymultistmt: ExpandIn: sqlText has an unterminated string literal")
	}
	return count, nil
}

// isExpandableSlice reports whether v is a slice/array ExpandIn should
// expand into multiple placeholders — i.e. any slice/array except []byte,
// which database/sql itself treats as a single binary value, not a list.
func isExpandableSlice(v any) bool {
	if v == nil {
		return false
	}
	if _, ok := v.([]byte); ok {
		return false
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		return true
	default:
		return false
	}
}
