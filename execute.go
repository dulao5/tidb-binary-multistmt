package binarymultistmt

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

// SQLError distinguishes a server-reported SQL-level failure (an ERR packet
// for one statement — Execute keeps draining the rest of the batch's
// responses) from a connection/protocol-level failure (Execute aborts
// immediately and the Conn must be discarded).
type SQLError struct {
	msg string
}

func (e *SQLError) Error() string { return e.msg }

// CommitError reports that COMMIT itself was rejected by the server after
// every statement in the batch individually reported success — as opposed
// to one of the batch's statements failing (that always surfaces as a
// *SQLError delivered to that statement's own Callback instead). This can
// happen even though this package only supports pessimistic transactions —
// e.g. a prewrite-time write conflict, a concurrent schema change
// invalidating the transaction, or the GC lifetime being exceeded.
//
// TiDB/MySQL already aborts the transaction server-side when this happens:
// verified empirically (two sessions racing an optimistic-mode write
// conflict) that a session whose COMMIT was just rejected behaves exactly
// as if ROLLBACK had already run — a fresh statement on it immediately
// succeeds under autocommit, with no explicit Rollback call needed or
// correct. c itself is unaffected by a *CommitError and stays usable; do
// not Close it, and do not call Rollback, because of one alone.
type CommitError struct {
	// Err is the underlying error COMMIT's response reported — typically a
	// *SQLError.
	Err error
}

func (e *CommitError) Error() string { return fmt.Sprintf("commit failed: %v", e.Err) }
func (e *CommitError) Unwrap() error { return e.Err }

// StatementResult is one Batch statement's outcome, delivered to its
// Callback — see Statement.Callback. There is no other way to read a
// statement's result: Conn.Execute keeps no per-statement record after the
// batch finishes, on purpose, so there's exactly one place to look.
type StatementResult struct {
	Index int
	SQL   string
	// HasResultSet reports whether this statement is row-returning, per the
	// server's own COM_STMT_PREPARE response (see prepare()) — not a
	// caller-supplied flag.
	HasResultSet bool
	// Err is nil on success, or the error for this statement (typically a
	// *SQLError for a server-reported failure).
	Err error
	// Rows is non-nil only while Callback runs for a statement with
	// HasResultSet true and Err nil: a forward-only iterator reading rows
	// directly off the wire, so Execute never buffers a result set in
	// memory. Do not retain or use it after Callback returns. A statement
	// queued with a nil Callback still has its rows drained (to keep the
	// pipelined stream in sync for later statements) — they are simply
	// never decoded or exposed anywhere.
	Rows *RowIterator
}

// RowIterator streams one row-returning statement's rows directly off the
// wire, one at a time, instead of Conn.Execute buffering the whole result
// set up front. Only valid for the duration of the Callback call that
// receives it via StatementResult.Rows.
type RowIterator struct {
	cols  []Column
	raw   io.Reader
	err   error
	done  bool
	count int
}

// Columns returns this result set's column list.
func (it *RowIterator) Columns() []Column { return it.cols }

// Next decodes and returns the next row (one value per Columns(), nil for
// SQL NULL), or nil when there are no more rows — call Err afterward to tell
// a clean end-of-result-set apart from a failure partway through.
func (it *RowIterator) Next() []any {
	if it.done {
		return nil
	}
	row, err := readPacket(it.raw)
	if err != nil {
		it.err = err
		it.done = true
		return nil
	}
	if len(row) == 0 {
		it.err = fmt.Errorf("empty row packet")
		it.done = true
		return nil
	}
	if row[0] == 0xFF {
		it.err = &SQLError{msg: errPacketText(row)}
		it.done = true
		return nil
	}
	if row[0] == 0xFE && len(row) < 9 {
		it.done = true // terminal EOF, not an error
		return nil
	}
	values, err := decodeBinaryRow(row, it.cols)
	if err != nil {
		it.err = fmt.Errorf("row %d: %w", it.count, err)
		it.done = true
		return nil
	}
	it.count++
	return values
}

// Err reports the error that stopped iteration, or nil if Next returned nil
// because the result set was exhausted normally.
func (it *RowIterator) Err() error { return it.err }

// ExecuteResult is what Conn.Execute returns.
type ExecuteResult struct {
	// AllSucceeded reports whether every statement in the batch succeeded.
	// When true, Execute has already sent COMMIT. When false, Execute has
	// NOT sent ROLLBACK — the transaction is left open on this Conn for the
	// caller to explicitly call Rollback (or decide something else). See
	// the package doc comment.
	AllSucceeded bool
}

// prepare sends COM_STMT_PREPARE for sqlText (unless already cached on this
// connection) and drains the param-definition/column-definition/EOF packets
// that follow the OK. Whether sqlText is row-returning is read straight off
// the wire — the OK packet's own column-count field, authoritative per the
// MySQL binary protocol (0 for a non-row-returning statement) — rather than
// taken on faith from the caller.
func (c *Conn) prepare(sqlText string) (preparedStmt, error) {
	if ps, ok := c.stmtCache[sqlText]; ok {
		return ps, nil
	}

	if err := writePacket(c.raw, append([]byte{comStmtPrepare}, sqlText...)); err != nil {
		return preparedStmt{}, err
	}
	resp, err := readPacket(c.raw)
	if err != nil {
		return preparedStmt{}, err
	}
	if len(resp) == 0 {
		return preparedStmt{}, fmt.Errorf("prepare %q: empty response packet", sqlText)
	}
	if resp[0] == 0xFF {
		return preparedStmt{}, fmt.Errorf("prepare %q failed: %s", sqlText, errPacketText(resp))
	}
	if resp[0] != 0x00 {
		return preparedStmt{}, fmt.Errorf("prepare %q: unexpected first byte 0x%02x", sqlText, resp[0])
	}
	if len(resp) < 9 {
		return preparedStmt{}, fmt.Errorf("prepare %q: OK packet too short (%d bytes, want at least 9)", sqlText, len(resp))
	}
	id := binary.LittleEndian.Uint32(resp[1:5])
	columnCount := int(binary.LittleEndian.Uint16(resp[5:7]))
	paramCount := int(binary.LittleEndian.Uint16(resp[7:9]))

	for i := 0; i < paramCount; i++ {
		if _, err := readPacket(c.raw); err != nil {
			return preparedStmt{}, err
		}
	}
	if paramCount > 0 {
		if _, err := readPacket(c.raw); err != nil { // EOF after param defs
			return preparedStmt{}, err
		}
	}
	for i := 0; i < columnCount; i++ {
		if _, err := readPacket(c.raw); err != nil {
			return preparedStmt{}, err
		}
	}
	if columnCount > 0 {
		if _, err := readPacket(c.raw); err != nil { // EOF after column defs
			return preparedStmt{}, err
		}
	}

	ps := preparedStmt{id: id, paramCount: paramCount, hasResultSet: columnCount > 0}
	c.stmtCache[sqlText] = ps
	return ps, nil
}

// binTimeLayout is how time.Time args are sent: as a fieldString-typed
// value, not MySQL's native packed DATE/DATETIME/TIMESTAMP binary struct.
// The server accepts a string in this position for a date/time column (it
// parses it the same way it would a text-protocol literal), which is far
// simpler than implementing the packed struct and costs nothing in
// practice for this package's use case.
const binTimeLayout = "2006-01-02 15:04:05.000000"

// appendIntValue appends v as a signed fieldLongLong value/type pair.
func appendIntValue(types, values []byte, v int64) ([]byte, []byte) {
	types = append(types, fieldLongLong, 0x00)
	values = binary.LittleEndian.AppendUint64(values, uint64(v))
	return types, values
}

// appendUintValue appends v as an unsigned fieldLongLong value/type pair
// (the unsigned flag is OR'd into the type's high byte, per the protocol).
func appendUintValue(types, values []byte, v uint64) ([]byte, []byte) {
	types = append(types, fieldLongLong, fieldUnsignedFlag)
	values = binary.LittleEndian.AppendUint64(values, v)
	return types, values
}

// appendBytesValue appends v as a fieldString-typed, length-encoded value —
// used for both string and []byte args (MySQL's binary protocol doesn't
// distinguish them in parameter position; the column's own declared type on
// the server side governs how the bytes are interpreted).
func appendBytesValue(types, values []byte, v []byte) ([]byte, []byte) {
	types = append(types, fieldString, 0x00)
	values = appendLenEncInt(values, uint64(len(v)))
	values = append(values, v...)
	return types, values
}

// valueOf resolves a driver.Valuer to its underlying value before the type
// switch below sees it, same as database/sql itself does.
func valueOf(a any) (any, error) {
	if dv, ok := a.(driver.Valuer); ok {
		v, err := dv.Value()
		if err != nil {
			return nil, fmt.Errorf("driver.Valuer.Value: %w", err)
		}
		return v, nil
	}
	return a, nil
}

// buildExecutePayload encodes a COM_STMT_EXECUTE for stmtID, binding args in
// order. Supports nil, bool, every sized int/uint (unsigned gets the
// protocol's unsigned type flag), float32/64, string, []byte (both encoded
// as length-prefixed fieldString values — see appendBytesValue), time.Time
// (encoded as a formatted string — see binTimeLayout), and driver.Valuer.
//
// String/[]byte values are length-encoded per the real MySQL protocol
// integer encoding (appendLenEncInt) rather than capped at a fixed size;
// the only hard ceiling is writePacket's maxPacketPayload, since this
// package doesn't implement COM_STMT_SEND_LONG_DATA.
func buildExecutePayload(stmtID uint32, args []any) ([]byte, error) {
	head := make([]byte, 0, 9)
	var b4 [4]byte
	head = append(head, comStmtExecute)
	binary.LittleEndian.PutUint32(b4[:], stmtID)
	head = append(head, b4[:]...)
	head = append(head, 0x00) // flags: CURSOR_TYPE_NO_CURSOR
	binary.LittleEndian.PutUint32(b4[:], 1)
	head = append(head, b4[:]...) // iteration_count = 1

	if len(args) == 0 {
		return head, nil
	}

	nullMask := make([]byte, (len(args)+7)/8)
	types := make([]byte, 0, len(args)*2)
	values := make([]byte, 0, 64)

	for i, a := range args {
		v, err := valueOf(a)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i, err)
		}
		switch x := v.(type) {
		case nil:
			nullMask[i/8] |= 1 << uint(i%8)
			types = append(types, fieldNULL, 0x00)
		case bool:
			types = append(types, fieldTiny, 0x00)
			if x {
				values = append(values, 0x01)
			} else {
				values = append(values, 0x00)
			}
		case int:
			types, values = appendIntValue(types, values, int64(x))
		case int8:
			types, values = appendIntValue(types, values, int64(x))
		case int16:
			types, values = appendIntValue(types, values, int64(x))
		case int32:
			types, values = appendIntValue(types, values, int64(x))
		case int64:
			types, values = appendIntValue(types, values, x)
		case uint:
			types, values = appendUintValue(types, values, uint64(x))
		case uint8:
			types, values = appendUintValue(types, values, uint64(x))
		case uint16:
			types, values = appendUintValue(types, values, uint64(x))
		case uint32:
			types, values = appendUintValue(types, values, uint64(x))
		case uint64:
			types, values = appendUintValue(types, values, x)
		case float32:
			types = append(types, fieldDouble, 0x00)
			values = binary.LittleEndian.AppendUint64(values, math.Float64bits(float64(x)))
		case float64:
			types = append(types, fieldDouble, 0x00)
			values = binary.LittleEndian.AppendUint64(values, math.Float64bits(x))
		case string:
			types, values = appendBytesValue(types, values, []byte(x))
		case []byte:
			if x == nil {
				nullMask[i/8] |= 1 << uint(i%8)
				types = append(types, fieldNULL, 0x00)
				continue
			}
			types, values = appendBytesValue(types, values, x)
		case time.Time:
			types, values = appendBytesValue(types, values, []byte(x.Format(binTimeLayout)))
		default:
			return nil, fmt.Errorf("arg %d: unsupported type %T (not in database/sql/driver.Value's set and doesn't implement driver.Valuer)", i, a)
		}
	}

	payload := make([]byte, 0, len(head)+len(nullMask)+1+len(types)+len(values))
	payload = append(payload, head...)
	payload = append(payload, nullMask...)
	payload = append(payload, 0x01) // new_params_bound_flag
	payload = append(payload, types...)
	payload = append(payload, values...)
	return payload, nil
}

// drainExecuteResponse reads exactly one EXECUTE's response off the wire —
// a single OK/ERR packet for a non-row-returning statement, or (for one
// that returns rows) the column-count/column-defs/EOF header followed by
// its rows.
//
// Rows are never buffered here. For a row-returning statement, cb (if
// non-nil) is invoked exactly once, synchronously, with a RowIterator that
// reads directly off raw as the caller calls Next(); if cb is nil, the rows
// are simply never decoded into anything a caller can see. Either way, any
// rows cb (or the caller) didn't consume are drained automatically before
// this function returns, so raw is correctly positioned at the next
// statement's response regardless of how much of the result set was
// actually read.
func drainExecuteResponse(raw io.Reader, hasResultSet bool, cb func(*RowIterator)) error {
	resp, err := readPacket(raw)
	if err != nil {
		return err
	}
	if len(resp) == 0 {
		return fmt.Errorf("empty response packet")
	}
	if resp[0] == 0xFF {
		return &SQLError{msg: errPacketText(resp)}
	}
	if !hasResultSet {
		if resp[0] != 0x00 {
			return fmt.Errorf("expected OK, got first byte 0x%02x", resp[0])
		}
		return nil
	}

	// columnCount comes straight off the wire — do not use it to pre-size
	// cols (make([]Column, 0, columnCount) panics with "cap out of range"
	// for a maliciously/accidentally huge value; found by FuzzDrainExecuteResponse).
	// It's still safe as the loop bound below: a bogus huge count just
	// means the loop runs until the next readPacket call hits EOF/an error
	// on the exhausted stream, same as any other truncated-input case.
	columnCount := readLenEncInt(resp)
	var cols []Column
	for i := uint64(0); i < columnCount; i++ {
		defPkt, err := readPacket(raw)
		if err != nil {
			return err
		}
		col, err := decodeColumnDef(defPkt)
		if err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
		cols = append(cols, col)
	}
	if columnCount > 0 {
		if _, err := readPacket(raw); err != nil { // EOF after column defs
			return err
		}
	}

	it := &RowIterator{cols: cols, raw: raw}
	if cb != nil {
		cb(it)
	}
	for it.Next() != nil { // drain whatever cb (or no one) left unread
	}
	return it.err
}

func writeComQuery(w io.Writer, text string) error {
	return writePacket(w, append([]byte{comQuery}, text...))
}

// readOKorErr reads one OK/ERR-only response (used for BEGIN/COMMIT/
// ROLLBACK, none of which return rows). An ERR packet is a *SQLError — the
// wire protocol round-tripped fine, the server just rejected the command —
// distinguishable via errors.As from a connection/protocol-level failure
// (a read error, or a malformed/unexpected packet).
func readOKorErr(raw io.Reader) error {
	resp, err := readPacket(raw)
	if err != nil {
		return err
	}
	if len(resp) == 0 {
		return fmt.Errorf("empty response packet")
	}
	if resp[0] == 0xFF {
		return &SQLError{msg: errPacketText(resp)}
	}
	if resp[0] != 0x00 {
		return fmt.Errorf("expected OK, got first byte 0x%02x", resp[0])
	}
	return nil
}

// Execute sends BEGIN, writes every statement in b's EXECUTE packet
// back-to-back (preparing any not-yet-cached SQL text first), then reads
// all responses. If every statement succeeded, Execute sends COMMIT itself
// before returning. If any statement failed, Execute does NOT send
// ROLLBACK — the transaction is left open on c, and the caller must
// explicitly call c.Rollback (or otherwise resolve it) before reusing c.
//
// If every statement succeeded but COMMIT itself is then rejected by the
// server, Execute returns a non-nil *CommitError (check with errors.As)
// alongside an ExecuteResult with AllSucceeded false — see CommitError's
// doc comment for why c stays healthy and reusable in that case, and why
// Rollback neither applies nor is needed.
//
// A connection/protocol-level failure (as opposed to a per-statement
// SQL-level ERR or a *CommitError) returns a non-nil error and leaves c
// unusable — the caller must Close it and Dial a new one.
//
// Only pessimistic transactions make sense here: a mid-pipeline failure
// does not stop already-written EXECUTEs from running (each is an
// independent command to the server — it has no idea they're "one batch"),
// so row locks must already be held as each statement runs, not deferred to
// commit, for "any failure → roll back everything" to stay correct.
// Optimistic transactions defer conflict detection to COMMIT (prewrite)
// time — a conflict there fails the whole transaction at once, with no way
// to attribute it back to the one statement that actually collided, which
// defeats the per-statement Callback this package is built around. This
// package does not support optimistic transactions and has no plans to.
func (c *Conn) Execute(ctx context.Context, b *Batch) (*ExecuteResult, error) {
	return c.execute(ctx, b, true)
}

// ExecuteAutoCommit pipelines every statement in b exactly like Execute —
// preparing any not-yet-cached SQL text, writing every EXECUTE back-to-back,
// then reading all responses — but never sends BEGIN or COMMIT. Each
// statement commits on its own as it executes, the same as issuing them one
// at a time with no explicit transaction (MySQL's session-default
// autocommit behavior). There is no transaction to roll back afterward:
// never call Rollback after ExecuteAutoCommit — whatever already executed
// is already durable, successful or not.
//
// This trades away the one thing Execute's BEGIN/COMMIT round trips buy —
// all-or-nothing atomicity across the batch — for two fewer round trips per
// call. It fits a read-only batch, or one where a partial/failed write
// genuinely doesn't need undoing (e.g. best-effort logging); anything that
// needs "every statement in this batch lands, or none do" must use Execute
// instead.
func (c *Conn) ExecuteAutoCommit(ctx context.Context, b *Batch) (*ExecuteResult, error) {
	return c.execute(ctx, b, false)
}

// execute is Execute and ExecuteAutoCommit's shared implementation;
// explicitTxn selects whether BEGIN/COMMIT wrap the pipelined EXECUTEs.
func (c *Conn) execute(ctx context.Context, b *Batch, explicitTxn bool) (*ExecuteResult, error) {
	stmts := b.Statements()
	if len(stmts) == 0 {
		return &ExecuteResult{AllSucceeded: true}, nil
	}

	ids := make([]uint32, len(stmts))
	hasResultSet := make([]bool, len(stmts))
	for i, s := range stmts {
		ps, err := c.prepare(s.SQL)
		if err != nil {
			c.broken = true
			return nil, fmt.Errorf("prepare statement #%d (%s): %w", i, s.SQL, err)
		}
		ids[i] = ps.id
		hasResultSet[i] = ps.hasResultSet
	}

	if explicitTxn {
		if err := writeComQuery(c.raw, "BEGIN"); err != nil {
			c.broken = true
			return nil, fmt.Errorf("write BEGIN: %w", err)
		}
		if err := readOKorErr(c.raw); err != nil {
			c.broken = true
			return nil, fmt.Errorf("BEGIN failed: %w", err)
		}
	}

	for i, s := range stmts {
		payload, err := buildExecutePayload(ids[i], s.Args)
		if err != nil {
			// Nothing has been written to the wire for this statement yet
			// (buildExecutePayload is pure encoding), so the connection
			// itself is still in sync — unlike the write/read failures
			// below, this doesn't need c.broken.
			return nil, fmt.Errorf("encode exec #%d (%s): %w", i, s.SQL, err)
		}
		if err := writePacket(c.raw, payload); err != nil {
			c.broken = true
			return nil, fmt.Errorf("write exec #%d: %w", i, err)
		}
	}

	allOK := true
	for i, s := range stmts {
		hrs := hasResultSet[i]

		// calledViaStream tracks whether cb below actually ran (it may not:
		// a statement that fails outright — e.g. an ERR packet instead of a
		// result-set header — never reaches the streaming branch inside
		// drainExecuteResponse). Only when it didn't run do we invoke
		// s.Callback ourselves afterward, so it fires exactly once either
		// way.
		calledViaStream := false
		var cb func(*RowIterator)
		if s.Callback != nil && hrs {
			cb = func(it *RowIterator) {
				calledViaStream = true
				sr := StatementResult{Index: i, SQL: s.SQL, HasResultSet: true, Rows: it}
				s.Callback(&sr)
			}
		}

		err := drainExecuteResponse(c.raw, hrs, cb)
		if s.Callback != nil && !calledViaStream {
			sr := StatementResult{Index: i, SQL: s.SQL, HasResultSet: hrs, Err: err}
			s.Callback(&sr)
		}
		if err != nil {
			var sqlErr *SQLError
			if !errors.As(err, &sqlErr) {
				// Connection/protocol-level failure: we can no longer trust
				// response-stream alignment for the remaining statements,
				// and sending COMMIT/ROLLBACK on a connection in this state
				// is unsafe. Abort.
				c.broken = true
				return nil, fmt.Errorf("reading response #%d: %w", i, err)
			}
			allOK = false
		}
	}

	if explicitTxn && allOK {
		if err := writeComQuery(c.raw, "COMMIT"); err != nil {
			c.broken = true
			return nil, fmt.Errorf("write COMMIT: %w", err)
		}
		if err := readOKorErr(c.raw); err != nil {
			var sqlErr *SQLError
			if errors.As(err, &sqlErr) {
				// The wire round-tripped fine; the server rejected COMMIT
				// itself and has already rolled back server-side (see
				// CommitError's doc comment) — c is healthy, only the
				// batch's net effect didn't land.
				return &ExecuteResult{AllSucceeded: false}, &CommitError{Err: sqlErr}
			}
			// A genuine protocol-level failure reading COMMIT's response —
			// the wire could be desynced. Discard c.
			c.broken = true
			return nil, fmt.Errorf("COMMIT failed: %w", err)
		}
	}

	return &ExecuteResult{AllSucceeded: allOK}, nil
}

// Rollback sends ROLLBACK on c. Call this after Execute (not
// ExecuteAutoCommit, which has no transaction to roll back) returns an
// ExecuteResult with AllSucceeded false AND a nil error (i.e. one of the
// batch's statements failed) — before reusing c for another Batch. Do not
// call it after a *CommitError: TiDB already rolled back server-side when
// COMMIT was rejected, so there is nothing left to roll back.
func (c *Conn) Rollback(ctx context.Context) error {
	if err := writeComQuery(c.raw, "ROLLBACK"); err != nil {
		c.broken = true
		return fmt.Errorf("write ROLLBACK: %w", err)
	}
	if err := readOKorErr(c.raw); err != nil {
		c.broken = true
		return err
	}
	return nil
}
