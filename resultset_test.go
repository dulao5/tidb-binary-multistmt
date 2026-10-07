package binarymultistmt

import (
	"encoding/binary"
	"reflect"
	"testing"
)

// buildColumnDefPacket hand-builds a minimal valid Protocol::ColumnDefinition41
// payload for name/colType/unsigned, matching the layout decodeColumnDef
// expects.
func buildColumnDefPacket(name string, colType byte, unsigned bool) []byte {
	var buf []byte
	appendLenEncStr := func(s string) {
		buf = appendLenEncInt(buf, uint64(len(s)))
		buf = append(buf, s...)
	}
	appendLenEncStr("def") // catalog
	appendLenEncStr("")    // schema
	appendLenEncStr("")    // table
	appendLenEncStr("")    // org_table
	appendLenEncStr(name)  // name
	appendLenEncStr("")    // org_name

	buf = appendLenEncInt(buf, 12)                    // fixed-fields length
	buf = binary.LittleEndian.AppendUint16(buf, 0x21) // charset (unused by decoder)
	buf = binary.LittleEndian.AppendUint32(buf, 11)   // column length (unused)
	buf = append(buf, colType)                        // type
	var flags uint16
	if unsigned {
		flags = colUnsignedFlag
	}
	buf = binary.LittleEndian.AppendUint16(buf, flags) // flags
	buf = append(buf, 0x00)                            // decimals
	buf = append(buf, 0x00, 0x00)                      // filler

	return buf
}

func TestDecodeColumnDef_Basic(t *testing.T) {
	col, err := decodeColumnDef(buildColumnDefPacket("id", colTypeLongLong, true))
	if err != nil {
		t.Fatalf("decodeColumnDef: %v", err)
	}
	if col.Name != "id" {
		t.Fatalf("Name = %q, want %q", col.Name, "id")
	}
	if col.Type != colTypeLongLong {
		t.Fatalf("Type = 0x%02x, want 0x%02x", col.Type, colTypeLongLong)
	}
	if !col.Unsigned {
		t.Fatalf("expected Unsigned = true")
	}
}

func TestDecodeColumnDef_TruncatedDoesNotPanic(t *testing.T) {
	full := buildColumnDefPacket("id", colTypeLongLong, false)
	for n := 0; n < len(full); n++ {
		if _, err := decodeColumnDef(full[:n]); err == nil {
			t.Fatalf("expected an error for a %d-byte (of %d) truncated column-def packet, got nil", n, len(full))
		}
	}
}

// buildBinaryRowPacket hand-builds a binary-protocol row packet: the 0x00
// header, a null-bitmap (2-bit offset) sized for len(values), then each
// non-nil value appended via the given encoder.
func buildBinaryRowPacket(values []any, encode func(v any) []byte) []byte {
	buf := []byte{0x00}
	bitmapLen := (len(values) + 7 + 2) / 8
	bitmap := make([]byte, bitmapLen)
	for i, v := range values {
		if v == nil {
			bitPos := i + 2
			bitmap[bitPos/8] |= 1 << uint(bitPos%8)
		}
	}
	buf = append(buf, bitmap...)
	for _, v := range values {
		if v != nil {
			buf = append(buf, encode(v)...)
		}
	}
	return buf
}

func TestDecodeBinaryRow_MixedNullAndValues(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: colTypeLongLong},
		{Name: "b", Type: colTypeString},
		{Name: "c", Type: colTypeLongLong},
	}
	values := []any{int64(42), "hi", nil}
	row := buildBinaryRowPacket(values, func(v any) []byte {
		switch x := v.(type) {
		case int64:
			return binary.LittleEndian.AppendUint64(nil, uint64(x))
		case string:
			b := appendLenEncInt(nil, uint64(len(x)))
			return append(b, x...)
		}
		panic("unreachable")
	})

	got, err := decodeBinaryRow(row, cols)
	if err != nil {
		t.Fatalf("decodeBinaryRow: %v", err)
	}
	// decodeColumnValue returns string-family types as []byte (the caller
	// casts to string if they want one) — see decodeColumnValue's doc.
	want := []any{int64(42), []byte("hi"), nil}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestDecodeBinaryRow_TruncatedDoesNotPanic(t *testing.T) {
	cols := []Column{{Name: "a", Type: colTypeLongLong}}
	full := buildBinaryRowPacket([]any{int64(42)}, func(v any) []byte {
		return binary.LittleEndian.AppendUint64(nil, uint64(v.(int64)))
	})
	for n := 0; n < len(full); n++ {
		if _, err := decodeBinaryRow(full[:n], cols); err == nil {
			t.Fatalf("expected an error for a %d-byte (of %d) truncated row packet, got nil", n, len(full))
		}
	}
}

func TestByteCursor_BoundsSafety(t *testing.T) {
	c := &byteCursor{buf: []byte{0x01}}
	if _, ok := c.readBytes(5); ok {
		t.Fatalf("expected readBytes past the end to report ok=false")
	}
	c2 := &byteCursor{buf: []byte{0xfe, 0x01, 0x02}} // claims an 8-byte int but only has 2
	if _, ok := c2.readLenEncInt(); ok {
		t.Fatalf("expected a truncated 0xfe length-encoded int to report ok=false")
	}
	c3 := &byteCursor{buf: nil}
	if _, ok := c3.readByte(); ok {
		t.Fatalf("expected readByte on an empty cursor to report ok=false")
	}
}
