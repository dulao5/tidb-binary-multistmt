package binarymultistmt

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// These fuzz targets cover every entry point that parses bytes coming off
// the wire (as opposed to bytes this package generated itself) — exactly
// the boundary #2's bounds-checking work hardened. The property under test
// throughout is simply "never panic", which go test's fuzzing engine
// checks automatically; a non-nil error for garbage input is the expected,
// correct outcome.

func FuzzReadPacket(f *testing.F) {
	f.Add([]byte{0x02, 0x00, 0x00, 0x00, 0xAA, 0xBB})
	f.Add([]byte{})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		readPacket(bytes.NewReader(data))
	})
}

func FuzzDecodeColumnDef(f *testing.F) {
	f.Add(buildColumnDefPacket("id", colTypeLongLong, false))
	f.Add(buildColumnDefPacket("", colTypeString, true))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		decodeColumnDef(data)
	})
}

func FuzzDecodeBinaryRow(f *testing.F) {
	cols := []Column{
		{Name: "a", Type: colTypeLongLong},
		{Name: "b", Type: colTypeString},
		{Name: "c", Type: colTypeDouble},
	}
	seed := buildBinaryRowPacket([]any{int64(1), "x", 3.5}, func(v any) []byte {
		switch x := v.(type) {
		case int64:
			return binary.LittleEndian.AppendUint64(nil, uint64(x))
		case string:
			b := appendLenEncInt(nil, uint64(len(x)))
			return append(b, x...)
		case float64:
			return binary.LittleEndian.AppendUint64(nil, 0)
		}
		return nil
	})
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Fuzz(func(t *testing.T, data []byte) {
		decodeBinaryRow(data, cols)
	})
}

// FuzzDrainExecuteResponse exercises the full response-reading loop
// (OK/ERR/column-defs/EOF/rows) under adversarial/truncated multi-packet
// input — the first fuzzed byte selects hasResultSet so both code paths get
// explored, the rest feeds a bytes.Reader that readPacket consumes packet
// by packet until EOF.
func FuzzDrainExecuteResponse(f *testing.F) {
	f.Add(byte(0), []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add(byte(1), []byte{0x01, 0x00, 0x00, 0x00, 0x01})
	f.Add(byte(1), []byte{})
	f.Fuzz(func(t *testing.T, selector byte, data []byte) {
		drainExecuteResponse(bytes.NewReader(data), selector%2 == 0)
	})
}

func FuzzReadOKorErr(f *testing.F) {
	f.Add([]byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Add([]byte{0x02, 0x00, 0x00, 0x00, 0xFF, 0x00})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		readOKorErr(bytes.NewReader(data))
	})
}
