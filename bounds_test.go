package binarymultistmt

import (
	"bytes"
	"net"
	"testing"
	"time"
)

// writeRawPacket writes a raw MySQL packet (header+payload) to w, for
// feeding a hand-crafted (possibly malformed) response to the functions
// under test. Deliberately does not take a *testing.T (go vet flags
// t.Fatalf/Errorf calls from a non-test goroutine, and every caller here
// runs this from inside a `go func(){...}()`) — a write failure surfaces
// anyway as a failed/timed-out read on the test's main goroutine.
func writeRawPacket(w net.Conn, seq byte, payload []byte) {
	hdr := make([]byte, 4+len(payload))
	putUint24(hdr[:3], len(payload))
	hdr[3] = seq
	copy(hdr[4:], payload)
	w.Write(hdr)
}

// pipePair returns a connected net.Conn pair (net.Pipe) with a short
// deadline, so a test that accidentally blocks on a read fails fast instead
// of hanging.
func pipePair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	c, s := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	c.SetDeadline(deadline)
	s.SetDeadline(deadline)
	t.Cleanup(func() {
		c.Close()
		s.Close()
	})
	return c, s
}

func TestReadOKorErr_EmptyPacketIsAnErrorNotAPanic(t *testing.T) {
	client, server := pipePair(t)
	go writeRawPacket(server, 0, nil)

	err := readOKorErr(client)
	if err == nil {
		t.Fatalf("expected an error for an empty response packet, got nil")
	}
}

func TestReadOKorErr_ShortERRPacketReportsShortPacket(t *testing.T) {
	client, server := pipePair(t)
	// A real ERR packet is at least 9 bytes (marker+errno+'#'+sqlstate);
	// feed a truncated one.
	go writeRawPacket(server, 0, []byte{0xFF, 0x01, 0x02})

	err := readOKorErr(client)
	if err == nil {
		t.Fatalf("expected an error for a truncated ERR packet, got nil")
	}
}

func TestDrainExecuteResponse_EmptyPacketIsAnErrorNotAPanic(t *testing.T) {
	client, server := pipePair(t)
	go writeRawPacket(server, 0, nil)

	err := drainExecuteResponse(client, false)
	if err == nil {
		t.Fatalf("expected an error for an empty response packet, got nil")
	}
}

func TestDrainExecuteResponse_EmptyRowPacketIsAnErrorNotAPanic(t *testing.T) {
	client, server := pipePair(t)
	go func() {
		// column count = 1 (triggers the result-set path), then an empty
		// "row" packet instead of a well-formed one.
		writeRawPacket(server, 0, []byte{0x01})
		writeRawPacket(server, 1, []byte{0x00, 'c'}) // column-def (content not validated)
		writeRawPacket(server, 2, []byte{0xFE, 0x00, 0x00})
		writeRawPacket(server, 3, nil)
	}()

	err := drainExecuteResponse(client, true)
	if err == nil {
		t.Fatalf("expected an error for an empty row packet, got nil")
	}
}

func TestPrepare_EmptyResponseIsAnErrorNotAPanic(t *testing.T) {
	client, server := pipePair(t)
	go func() {
		readPacket(server) // drain the COM_STMT_PREPARE request
		writeRawPacket(server, 1, nil)
	}()

	c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}
	if _, err := c.prepare("SELECT 1", false); err == nil {
		t.Fatalf("expected an error for an empty prepare response, got nil")
	}
}

func TestPrepare_ShortOKPacketIsAnErrorNotAPanic(t *testing.T) {
	client, server := pipePair(t)
	go func() {
		readPacket(server)
		// OK marker present but far fewer than the 9 bytes a real
		// COM_STMT_PREPARE_OK packet needs for stmt id/column/param counts.
		writeRawPacket(server, 1, []byte{0x00, 0x01, 0x02})
	}()

	c := &Conn{raw: client, stmtCache: make(map[string]preparedStmt)}
	if _, err := c.prepare("SELECT 1", false); err == nil {
		t.Fatalf("expected an error for a short OK packet, got nil")
	}
}

func TestReadLenEncInt_NeverPanicsOnShortInput(t *testing.T) {
	cases := [][]byte{
		nil,
		{},
		{0xfc},
		{0xfc, 0x01},
		{0xfd},
		{0xfd, 0x01, 0x02},
		{0xfe},
		{0xfe, 0x01, 0x02, 0x03},
	}
	for _, b := range cases {
		readLenEncInt(b) // must not panic regardless of return value
	}
}

func TestReadPacket_RoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := writePacket(&buf, []byte("hello")); err != nil {
		t.Fatalf("writePacket: %v", err)
	}
	got, err := readPacket(&buf)
	if err != nil {
		t.Fatalf("readPacket: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("expected %q, got %q", "hello", got)
	}
}

func TestReadPacket_TruncatedHeaderReturnsError(t *testing.T) {
	_, err := readPacket(bytes.NewReader([]byte{0x01, 0x00})) // only 2 of 4 header bytes
	if err == nil {
		t.Fatalf("expected an error for a truncated packet header, got nil")
	}
}
