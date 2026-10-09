package binarymultistmt

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
)

// buildPrepareOKPacket hand-builds the first 9 bytes of a
// COM_STMT_PREPARE_OK packet — all prepare() actually reads (status, id,
// column count, param count); it doesn't care about anything after.
func buildPrepareOKPacket(id uint32, columnCount, paramCount uint16) []byte {
	buf := make([]byte, 9)
	buf[0] = 0x00
	binary.LittleEndian.PutUint32(buf[1:5], id)
	binary.LittleEndian.PutUint16(buf[5:7], columnCount)
	binary.LittleEndian.PutUint16(buf[7:9], paramCount)
	return buf
}

// buildErrPacket hand-builds a minimal well-formed ERR packet.
func buildErrPacket(code uint16, msg string) []byte {
	buf := []byte{0xFF}
	buf = binary.LittleEndian.AppendUint16(buf, code)
	buf = append(buf, '#')
	buf = append(buf, "HY000"...)
	buf = append(buf, msg...)
	return buf
}

func encodeInt64Row(v any) []byte {
	x, _ := v.(int64)
	return binary.LittleEndian.AppendUint64(nil, uint64(x))
}

// TestPrepare_DerivesHasResultSetFromColumnCount confirms prepare() no
// longer trusts a caller-supplied flag: hasResultSet comes straight from
// COM_STMT_PREPARE_OK's own column-count field.
func TestPrepare_DerivesHasResultSetFromColumnCount(t *testing.T) {
	t.Run("columnCount zero means no result set", func(t *testing.T) {
		client, server := pipePair(t)
		go func() {
			readPacket(server) // drain COM_STMT_PREPARE
			writeRawPacket(server, 0, buildPrepareOKPacket(1, 0, 0))
		}()
		c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}
		ps, err := c.prepare("INSERT INTO t VALUES (1)")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if ps.hasResultSet {
			t.Fatalf("expected hasResultSet=false for columnCount=0")
		}
	})

	t.Run("columnCount nonzero means a result set", func(t *testing.T) {
		client, server := pipePair(t)
		go func() {
			readPacket(server) // drain COM_STMT_PREPARE
			writeRawPacket(server, 0, buildPrepareOKPacket(1, 1, 0))
			writeRawPacket(server, 0, buildColumnDefPacket("a", colTypeLongLong, false))
			writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00}) // EOF after column defs
		}()
		c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}
		ps, err := c.prepare("SELECT a FROM t")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		if !ps.hasResultSet {
			t.Fatalf("expected hasResultSet=true for columnCount=1")
		}
	})
}

// TestDrainExecuteResponse_CallbackStreamsAndAutoDrains confirms the
// streaming path never buffers: cb reads only the first of two rows itself,
// and drainExecuteResponse is responsible for silently draining the second
// row and the terminal EOF so the wire stays aligned — proven here by a
// sentinel packet right after the result set, which the test reads cleanly
// once drainExecuteResponse returns.
func TestDrainExecuteResponse_CallbackStreamsAndAutoDrains(t *testing.T) {
	client, server := pipePair(t)
	go func() {
		writeRawPacket(server, 0, []byte{0x01}) // column count = 1
		writeRawPacket(server, 0, buildColumnDefPacket("a", colTypeLongLong, false))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00}) // EOF after column defs
		writeRawPacket(server, 0, buildBinaryRowPacket([]any{int64(1)}, encodeInt64Row))
		writeRawPacket(server, 0, buildBinaryRowPacket([]any{int64(2)}, encodeInt64Row))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00}) // terminal EOF
		writeRawPacket(server, 0, []byte("sentinel"))       // next statement's response
	}()

	var got []any
	cbCalled := false
	cb := func(it *RowIterator) {
		cbCalled = true
		got = it.Next() // read only the first row, leave the rest
	}

	err := drainExecuteResponse(client, true, cb)
	if err != nil {
		t.Fatalf("drainExecuteResponse: %v", err)
	}
	if !cbCalled {
		t.Fatalf("expected cb to be invoked")
	}
	if len(got) != 1 || got[0].(int64) != 1 {
		t.Fatalf("expected cb's own Next() to see row {1}, got %v", got)
	}

	sentinel, err := readPacket(client)
	if err != nil {
		t.Fatalf("reading sentinel after auto-drain: %v", err)
	}
	if string(sentinel) != "sentinel" {
		t.Fatalf("stream misaligned: expected sentinel packet, got %q — auto-drain left bytes unread", sentinel)
	}
}

// TestExecute_CallbackFiresExactlyOnce drives a full Conn.Execute over a
// hand-scripted fake server covering three cases in one batch: a
// row-returning statement whose Callback only partially consumes its rows,
// a non-row-returning statement that succeeds, and a row-returning
// statement that fails outright (an ERR packet instead of ever reaching the
// result-set header) — confirming every statement's Callback fires exactly
// once regardless of which path it took, including the one that never
// reaches drainExecuteResponse's streaming branch at all.
func TestExecute_CallbackFiresExactlyOnce(t *testing.T) {
	client, server := pipePair(t)
	go func() {
		// prepare stmt0: SELECT, 1 column
		readPacket(server)
		writeRawPacket(server, 0, buildPrepareOKPacket(1, 1, 0))
		writeRawPacket(server, 0, buildColumnDefPacket("a", colTypeLongLong, false))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00})

		// prepare stmt1: INSERT, no columns
		readPacket(server)
		writeRawPacket(server, 0, buildPrepareOKPacket(2, 0, 0))

		// prepare stmt2: SELECT, 1 column (but EXECUTE will fail outright)
		readPacket(server)
		writeRawPacket(server, 0, buildPrepareOKPacket(3, 1, 0))
		writeRawPacket(server, 0, buildColumnDefPacket("b", colTypeLongLong, false))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00})

		// BEGIN
		readPacket(server)
		writeRawPacket(server, 0, []byte{0x00})

		// all three EXECUTEs are written back-to-back before any response
		readPacket(server)
		readPacket(server)
		readPacket(server)

		// response #0: a 2-row result set
		writeRawPacket(server, 0, []byte{0x01})
		writeRawPacket(server, 0, buildColumnDefPacket("a", colTypeLongLong, false))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00})
		writeRawPacket(server, 0, buildBinaryRowPacket([]any{int64(10)}, encodeInt64Row))
		writeRawPacket(server, 0, buildBinaryRowPacket([]any{int64(20)}, encodeInt64Row))
		writeRawPacket(server, 0, []byte{0xFE, 0x00, 0x00})

		// response #1: OK
		writeRawPacket(server, 0, []byte{0x00})

		// response #2: ERR, straight away — never reaches a result-set header
		writeRawPacket(server, 0, buildErrPacket(1105, "stmt2 failed"))
	}()

	c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}

	var (
		cb0, cb1, cb2 bool
		rowsSeen      [][]any
	)
	b := NewBatch()
	b.Add("SELECT a FROM t", nil, func(sr *StatementResult) {
		cb0 = true
		if !sr.HasResultSet {
			t.Errorf("stmt0: expected HasResultSet=true")
		}
		if sr.Rows == nil {
			t.Fatalf("stmt0: expected a non-nil RowIterator")
		}
		if row := sr.Rows.Next(); row != nil {
			rowsSeen = append(rowsSeen, row)
		}
		// deliberately not consuming the second row — Execute must drain it.
	})
	b.Add("INSERT INTO t VALUES (1)", nil, func(sr *StatementResult) {
		cb1 = true
		if sr.HasResultSet {
			t.Errorf("stmt1: expected HasResultSet=false")
		}
		if sr.Err != nil {
			t.Errorf("stmt1: unexpected error: %v", sr.Err)
		}
	})
	b.Add("SELECT b FROM t", nil, func(sr *StatementResult) {
		cb2 = true
		if !sr.HasResultSet {
			t.Errorf("stmt2: expected HasResultSet=true even though it failed")
		}
		var sqlErr *SQLError
		if !errors.As(sr.Err, &sqlErr) {
			t.Errorf("stmt2: expected a *SQLError, got %v", sr.Err)
		}
		if sr.Rows != nil {
			t.Errorf("stmt2: expected a nil RowIterator for a statement that failed before any result-set header")
		}
	})

	res, err := c.Execute(context.Background(), b)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.AllSucceeded {
		t.Fatalf("expected stmt2's failure to make AllSucceeded false")
	}
	if !cb0 || !cb1 || !cb2 {
		t.Fatalf("expected every statement's callback to fire exactly once, got cb0=%v cb1=%v cb2=%v", cb0, cb1, cb2)
	}
	if len(rowsSeen) != 1 || rowsSeen[0][0].(int64) != 10 {
		t.Fatalf("expected stmt0's callback to have read exactly row {10} itself, got %v", rowsSeen)
	}
}

// TestExecuteAutoCommit_SkipsBeginAndCommit confirms ExecuteAutoCommit never
// sends BEGIN before pipelining EXECUTEs. Absence of a trailing COMMIT is
// confirmed implicitly: if ExecuteAutoCommit mistakenly tried to send one,
// the server goroutine below (which only ever reads PREPARE then EXECUTE)
// would never consume it, and the client's write would block until
// pipePair's deadline and surface as a write-timeout error instead of nil.
func TestExecuteAutoCommit_SkipsBeginAndCommit(t *testing.T) {
	client, server := pipePair(t)
	var gotOpcode byte
	go func() {
		readPacket(server) // drain COM_STMT_PREPARE
		writeRawPacket(server, 0, buildPrepareOKPacket(1, 0, 0))

		pkt, _ := readPacket(server) // should be COM_STMT_EXECUTE, not BEGIN's COM_QUERY
		if len(pkt) > 0 {
			gotOpcode = pkt[0]
		}
		writeRawPacket(server, 0, []byte{0x00}) // OK for the EXECUTE
	}()

	c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}
	b := NewBatch().Add("INSERT INTO t VALUES (1)", nil, nil)
	res, err := c.ExecuteAutoCommit(context.Background(), b)
	if err != nil {
		t.Fatalf("ExecuteAutoCommit: %v", err)
	}
	if !res.AllSucceeded {
		t.Fatalf("expected success")
	}
	if gotOpcode != comStmtExecute {
		t.Fatalf("expected the command right after PREPARE to be COM_STMT_EXECUTE (0x%02x), got 0x%02x — ExecuteAutoCommit must not send BEGIN", comStmtExecute, gotOpcode)
	}
}
