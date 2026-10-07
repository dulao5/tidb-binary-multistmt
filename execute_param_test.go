package binarymultistmt

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"
)

// payloadTail returns payload with the fixed 10-byte EXECUTE header (1
// command byte + 4 stmt id + 1 flags + 4 iteration count) stripped, for
// asserting just the null-bitmap/types/values region these tests care
// about.
func payloadTail(t *testing.T, payload []byte) []byte {
	t.Helper()
	if len(payload) < 10 {
		t.Fatalf("payload too short: %d bytes", len(payload))
	}
	return payload[10:]
}

func TestBuildExecutePayload_Nil(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{nil})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	// 1 null-bitmap byte (bit 0 set) + new_params_bound_flag(1) + type(2) + no value bytes.
	want := []byte{0x01, 0x01, fieldNULL, 0x00}
	if !bytes.Equal(tail, want) {
		t.Fatalf("got % x, want % x", tail, want)
	}
}

func TestBuildExecutePayload_Bool(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{true, false})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	want := []byte{0x00, 0x01, fieldTiny, 0x00, fieldTiny, 0x00, 0x01, 0x00}
	if !bytes.Equal(tail, want) {
		t.Fatalf("got % x, want % x", tail, want)
	}
}

func TestBuildExecutePayload_SignedInts(t *testing.T) {
	for _, v := range []any{int(-5), int8(-5), int16(-5), int32(-5), int64(-5)} {
		payload, err := buildExecutePayload(1, []any{v})
		if err != nil {
			t.Fatalf("buildExecutePayload(%T): %v", v, err)
		}
		tail := payloadTail(t, payload)
		if len(tail) != 1+1+2+8 {
			t.Fatalf("%T: unexpected length %d", v, len(tail))
		}
		if tail[2] != fieldLongLong || tail[3] != 0x00 {
			t.Fatalf("%T: expected signed LONGLONG type, got % x", v, tail[2:4])
		}
		got := int64(binary.LittleEndian.Uint64(tail[4:12]))
		if got != -5 {
			t.Fatalf("%T: expected -5, got %d", v, got)
		}
	}
}

func TestBuildExecutePayload_UnsignedInts(t *testing.T) {
	for _, v := range []any{uint(7), uint8(7), uint16(7), uint32(7), uint64(7)} {
		payload, err := buildExecutePayload(1, []any{v})
		if err != nil {
			t.Fatalf("buildExecutePayload(%T): %v", v, err)
		}
		tail := payloadTail(t, payload)
		if tail[2] != fieldLongLong || tail[3] != fieldUnsignedFlag {
			t.Fatalf("%T: expected unsigned-flagged LONGLONG type, got % x", v, tail[2:4])
		}
		got := binary.LittleEndian.Uint64(tail[4:12])
		if got != 7 {
			t.Fatalf("%T: expected 7, got %d", v, got)
		}
	}
}

func TestBuildExecutePayload_Floats(t *testing.T) {
	for _, v := range []any{float32(3.5), float64(3.5)} {
		payload, err := buildExecutePayload(1, []any{v})
		if err != nil {
			t.Fatalf("buildExecutePayload(%T): %v", v, err)
		}
		tail := payloadTail(t, payload)
		if tail[2] != fieldDouble {
			t.Fatalf("%T: expected DOUBLE type, got 0x%02x", v, tail[2])
		}
		got := math.Float64frombits(binary.LittleEndian.Uint64(tail[4:12]))
		if got != 3.5 {
			t.Fatalf("%T: expected 3.5, got %v", v, got)
		}
	}
}

func TestBuildExecutePayload_ShortString(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{"hi"})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	want := []byte{0x00, 0x01, fieldString, 0x00, 0x02, 'h', 'i'}
	if !bytes.Equal(tail, want) {
		t.Fatalf("got % x, want % x", tail, want)
	}
}

// TestBuildExecutePayload_StringBeyondOldOneByteCap exercises a length past
// the old hardcoded 250-byte/1-byte-length-prefix limit, proving the real
// length-encoded-integer form (the 0xfc 2-byte form here) is used instead of
// erroring out.
func TestBuildExecutePayload_StringBeyondOldOneByteCap(t *testing.T) {
	s := strings.Repeat("x", 1000)
	payload, err := buildExecutePayload(1, []any{s})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	valueStart := 1 + 1 + 2 // null-bitmap + bound-flag + type
	if tail[valueStart] != 0xfc {
		t.Fatalf("expected the 0xfc (2-byte) length-encoded-int form for a 1000-byte string, got first value byte 0x%02x", tail[valueStart])
	}
	gotLen := binary.LittleEndian.Uint16(tail[valueStart+1 : valueStart+3])
	if int(gotLen) != len(s) {
		t.Fatalf("expected encoded length %d, got %d", len(s), gotLen)
	}
	gotStr := string(tail[valueStart+3 : valueStart+3+len(s)])
	if gotStr != s {
		t.Fatalf("decoded string does not round-trip")
	}
}

func TestBuildExecutePayload_NilByteSliceIsNull(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{[]byte(nil)})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	want := []byte{0x01, 0x01, fieldNULL, 0x00}
	if !bytes.Equal(tail, want) {
		t.Fatalf("got % x, want % x", tail, want)
	}
}

func TestBuildExecutePayload_NonNilByteSlice(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{[]byte{0xDE, 0xAD}})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	want := []byte{0x00, 0x01, fieldString, 0x00, 0x02, 0xDE, 0xAD}
	if !bytes.Equal(tail, want) {
		t.Fatalf("got % x, want % x", tail, want)
	}
}

func TestBuildExecutePayload_TimeTime(t *testing.T) {
	tm := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	payload, err := buildExecutePayload(1, []any{tm})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	if tail[2] != fieldString {
		t.Fatalf("expected fieldString type for time.Time, got 0x%02x", tail[2])
	}
	n := int(tail[3+1])
	got := string(tail[3+2 : 3+2+n])
	want := "2026-01-02 03:04:05.000000"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

type fakeValuer struct{ v int64 }

func (f fakeValuer) Value() (driver.Value, error) { return f.v, nil }

func TestBuildExecutePayload_DriverValuer(t *testing.T) {
	payload, err := buildExecutePayload(1, []any{fakeValuer{v: 42}})
	if err != nil {
		t.Fatalf("buildExecutePayload: %v", err)
	}
	tail := payloadTail(t, payload)
	got := int64(binary.LittleEndian.Uint64(tail[4:12]))
	if got != 42 {
		t.Fatalf("expected the Valuer's underlying 42, got %d", got)
	}
}

func TestBuildExecutePayload_UnsupportedTypeErrors(t *testing.T) {
	type unsupported struct{}
	if _, err := buildExecutePayload(1, []any{unsupported{}}); err == nil {
		t.Fatalf("expected an error for an unsupported arg type")
	}
}

func TestAppendLenEncInt_RoundTripsWithReadLenEncInt(t *testing.T) {
	cases := []uint64{0, 1, 250, 251, 65535, 65536, 0xffffff, 0xffffff + 1}
	for _, n := range cases {
		buf := appendLenEncInt(nil, n)
		got := readLenEncInt(buf)
		if got != n {
			t.Fatalf("n=%d: round-trip got %d (encoded % x)", n, got, buf)
		}
	}
}

func TestWritePacket_RejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	err := writePacket(&buf, make([]byte, maxPacketPayload+1))
	if err == nil {
		t.Fatalf("expected an error for a payload exceeding maxPacketPayload")
	}
}
