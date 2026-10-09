package binarymultistmt

import (
	"fmt"
	"strings"
)

// ExpandValues rewrites sqlText's single value-tuple template — a
// parenthesized, comma-separated run of "?" and nothing else, e.g. the
// "(?, ?)" in "INSERT INTO accounts (id, balance) VALUES (?, ?)" — into one
// copy of that tuple per row in rows, comma-joined, and returns the
// flattened args to match: turning a two-column template plus
// [][]any{{1, 100}, {2, 200}, {3, 300}} into "VALUES (?, ?), (?, ?), (?, ?)"
// plus six args in row-major order.
//
// This is ExpandIn's counterpart for the "one row per call becomes N rows
// per call" shape instead of the "one value becomes N values" shape;
// together they're the two ways Statement.Args' one-Go-value-per-"?"
// contract needs help expressing a variable-length list. Call it before
// Batch.Add, same as ExpandIn — it doesn't touch a Batch or the server.
//
// sqlText must contain exactly one parenthesized group whose content,
// trimmed of whitespace, is purely "?" placeholders separated by commas
// (nothing else — no column names, no expressions) — that is the row
// template ExpandValues repeats. A column-list parenthesis like the "(id,
// balance)" above does not qualify (it names columns, not placeholders), so
// the usual "INSERT INTO t (col, ...) VALUES (?, ...)" shape has exactly one
// qualifying group, as intended. It is an error if sqlText has zero such
// groups (ExpandValues can't find a template to repeat), more than one
// (ambiguous — ExpandValues does not guess which one you meant), rows is
// empty, or any row's length doesn't match the template's placeholder count.
//
// Every element of every row is bound exactly like a scalar Statement.Args
// element — one value per flattened "?", sent as a genuine COM_STMT_EXECUTE
// binary protocol parameter (see buildExecutePayload), never formatted into
// the SQL text. ExpandValues only rewrites placeholder text and reorders
// the Go values, so there is no string-escaping behavior here to get wrong
// — no injection surface to worry about.
func ExpandValues(sqlText string, rows [][]any) (string, []any, error) {
	if len(rows) == 0 {
		return "", nil, fmt.Errorf("binarymultistmt: ExpandValues: rows is empty")
	}

	open, close, n, err := findPlaceholderTuple(sqlText)
	if err != nil {
		return "", nil, err
	}

	for i, row := range rows {
		if len(row) != n {
			return "", nil, fmt.Errorf("binarymultistmt: ExpandValues: row %d has %d value(s), want %d (the template's placeholder count)", i, len(row), n)
		}
	}

	var tuple strings.Builder
	tuple.WriteByte('(')
	for i := 0; i < n; i++ {
		if i > 0 {
			tuple.WriteByte(',')
		}
		tuple.WriteByte('?')
	}
	tuple.WriteByte(')')

	tuples := make([]string, len(rows))
	for i := range tuples {
		tuples[i] = tuple.String()
	}

	newSQL := sqlText[:open] + strings.Join(tuples, ",") + sqlText[close+1:]

	flat := make([]any, 0, len(rows)*n)
	for _, row := range rows {
		flat = append(flat, row...)
	}

	return newSQL, flat, nil
}

// findPlaceholderTuple scans sqlText for parenthesized groups (skipping
// '...'/"..." string literals, same as countPlaceholders/ExpandIn) and
// returns the byte offsets of the "(" and ")" of the one group whose content
// is purely comma-separated "?" placeholders, plus how many placeholders it
// contains. It is an error if there isn't exactly one such group.
func findPlaceholderTuple(sqlText string) (open, close, count int, err error) {
	type span struct{ open, close int }
	var candidates []span

	var parenStack []int
	inSingle, inDouble := false, false

	for i := 0; i < len(sqlText); i++ {
		c := sqlText[i]
		switch {
		case inSingle:
			if c == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if c == '"' {
				inDouble = false
			}
			continue
		case c == '\'':
			inSingle = true
			continue
		case c == '"':
			inDouble = true
			continue
		case c == '(':
			parenStack = append(parenStack, i)
			continue
		case c == ')':
			if len(parenStack) == 0 {
				return 0, 0, 0, fmt.Errorf("binarymultistmt: ExpandValues: sqlText has an unmatched ')'")
			}
			o := parenStack[len(parenStack)-1]
			parenStack = parenStack[:len(parenStack)-1]
			if _, ok := placeholderOnlyCount(sqlText[o+1 : i]); ok {
				candidates = append(candidates, span{open: o, close: i})
			}
			continue
		}
	}
	if len(parenStack) > 0 {
		return 0, 0, 0, fmt.Errorf("binarymultistmt: ExpandValues: sqlText has an unmatched '('")
	}
	if inSingle || inDouble {
		return 0, 0, 0, fmt.Errorf("binarymultistmt: ExpandValues: sqlText has an unterminated string literal")
	}

	switch len(candidates) {
	case 0:
		return 0, 0, 0, fmt.Errorf("binarymultistmt: ExpandValues: no \"(?, ...)\" placeholder-only group found in sqlText")
	case 1:
		n, _ := placeholderOnlyCount(sqlText[candidates[0].open+1 : candidates[0].close])
		return candidates[0].open, candidates[0].close, n, nil
	default:
		return 0, 0, 0, fmt.Errorf("binarymultistmt: ExpandValues: sqlText has %d placeholder-only groups, want exactly 1 (ambiguous)", len(candidates))
	}
}

// placeholderOnlyCount reports whether s, trimmed of surrounding whitespace,
// is nothing but one or more "?" separated by commas (each side optionally
// padded with whitespace), and if so, how many.
func placeholderOnlyCount(s string) (int, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	parts := strings.Split(s, ",")
	for _, p := range parts {
		if strings.TrimSpace(p) != "?" {
			return 0, false
		}
	}
	return len(parts), true
}
