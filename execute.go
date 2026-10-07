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

// StatementResult is one Batch statement's outcome after Conn.Execute.
type StatementResult struct {
	Index int
	SQL   string
	// Err is nil on success, or the error for this statement (typically a
	// *SQLError for a server-reported failure).
	Err error
}

// ExecuteResult is what Conn.Execute returns.
type ExecuteResult struct {
	Results []StatementResult
	// AllSucceeded reports whether every statement in the batch succeeded.
	// When true, Execute has already sent COMMIT. When false, Execute has
	// NOT sent ROLLBACK — the transaction is left open on this Conn for the
	// caller to explicitly call Rollback (or decide something else). See
	// the package doc comment.
	AllSucceeded bool
}

// prepare sends COM_STMT_PREPARE for sqlText (unless already cached on this
// connection) and drains the param-definition/column-definition/EOF packets
// that follow the OK.
func (c *Conn) prepare(sqlText string, hasResultSet bool) (preparedStmt, error) {
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

	ps := preparedStmt{id: id, paramCount: paramCount, hasResultSet: hasResultSet}
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
// a single OK/ERR packet for a non-row-returning statement, or a binary
// result set (column-count/column-defs/EOF, then rows until a terminal EOF)
// for one that returns rows. Row *values* are not decoded yet, only
// skipped — packets are self-delimited by their length header, so
// correctly advancing past them (to stay in sync for the next statement's
// response) never requires understanding their binary-encoded contents.
// See the result-set-decoding issue for turning this into real data.
func drainExecuteResponse(raw io.Reader, hasResultSet bool) error {
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

	columnCount := readLenEncInt(resp)
	for i := uint64(0); i < columnCount; i++ {
		if _, err := readPacket(raw); err != nil {
			return err
		}
	}
	if columnCount > 0 {
		if _, err := readPacket(raw); err != nil { // EOF after column defs
			return err
		}
	}
	for {
		row, err := readPacket(raw)
		if err != nil {
			return err
		}
		if len(row) == 0 {
			return fmt.Errorf("empty row packet")
		}
		if row[0] == 0xFF {
			return &SQLError{msg: errPacketText(row)}
		}
		if row[0] == 0xFE && len(row) < 9 {
			return nil // terminal EOF
		}
		// else: a binary row packet (leading 0x00) — discard, keep reading.
	}
}

func writeComQuery(w io.Writer, text string) error {
	return writePacket(w, append([]byte{comQuery}, text...))
}

func readOKorErr(raw io.Reader) error {
	resp, err := readPacket(raw)
	if err != nil {
		return err
	}
	if len(resp) == 0 {
		return fmt.Errorf("empty response packet")
	}
	if resp[0] == 0xFF {
		return fmt.Errorf("ERR: %s", errPacketText(resp))
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
// A connection/protocol-level failure (as opposed to a per-statement
// SQL-level ERR) returns a non-nil error and leaves c unusable — the caller
// must Close it and Dial a new one.
func (c *Conn) Execute(ctx context.Context, b *Batch) (*ExecuteResult, error) {
	stmts := b.Statements()
	if len(stmts) == 0 {
		return &ExecuteResult{AllSucceeded: true}, nil
	}

	ids := make([]uint32, len(stmts))
	for i, s := range stmts {
		ps, err := c.prepare(s.SQL, s.HasResultSet)
		if err != nil {
			return nil, fmt.Errorf("prepare statement #%d (%s): %w", i, s.SQL, err)
		}
		ids[i] = ps.id
	}

	if err := writeComQuery(c.raw, "BEGIN"); err != nil {
		return nil, fmt.Errorf("write BEGIN: %w", err)
	}
	if err := readOKorErr(c.raw); err != nil {
		return nil, fmt.Errorf("BEGIN failed: %w", err)
	}

	for i, s := range stmts {
		payload, err := buildExecutePayload(ids[i], s.Args)
		if err != nil {
			return nil, fmt.Errorf("encode exec #%d (%s): %w", i, s.SQL, err)
		}
		if err := writePacket(c.raw, payload); err != nil {
			return nil, fmt.Errorf("write exec #%d: %w", i, err)
		}
	}

	results := make([]StatementResult, len(stmts))
	allOK := true
	for i, s := range stmts {
		err := drainExecuteResponse(c.raw, s.HasResultSet)
		results[i] = StatementResult{Index: i, SQL: s.SQL, Err: err}
		if err != nil {
			var sqlErr *SQLError
			if !errors.As(err, &sqlErr) {
				// Connection/protocol-level failure: we can no longer trust
				// response-stream alignment for the remaining statements,
				// and sending COMMIT/ROLLBACK on a connection in this state
				// is unsafe. Abort.
				return nil, fmt.Errorf("reading response #%d: %w", i, err)
			}
			allOK = false
		}
	}

	if allOK {
		if err := writeComQuery(c.raw, "COMMIT"); err != nil {
			return nil, fmt.Errorf("write COMMIT: %w", err)
		}
		if err := readOKorErr(c.raw); err != nil {
			return nil, fmt.Errorf("COMMIT failed: %w", err)
		}
	}

	return &ExecuteResult{Results: results, AllSucceeded: allOK}, nil
}

// Rollback sends ROLLBACK on c. Call this after Execute returns an
// ExecuteResult with AllSucceeded false, before reusing c for another Batch.
func (c *Conn) Rollback(ctx context.Context) error {
	if err := writeComQuery(c.raw, "ROLLBACK"); err != nil {
		return fmt.Errorf("write ROLLBACK: %w", err)
	}
	return readOKorErr(c.raw)
}
