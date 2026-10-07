package binarymultistmt

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Column type bytes this package's row decoder understands — the
// Protocol::ColumnType values relevant to ordinary table data (OUT params,
// GEOMETRY internals, and other exotica are out of scope).
const (
	colTypeDecimal    = 0x00
	colTypeTiny       = 0x01
	colTypeShort      = 0x02
	colTypeLong       = 0x03
	colTypeFloat      = 0x04
	colTypeDouble     = 0x05
	colTypeNull       = 0x06
	colTypeTimestamp  = 0x07
	colTypeLongLong   = 0x08
	colTypeInt24      = 0x09
	colTypeDate       = 0x0A
	colTypeTime       = 0x0B
	colTypeDateTime   = 0x0C
	colTypeYear       = 0x0D
	colTypeVarChar    = 0x0F
	colTypeBit        = 0x10
	colTypeJSON       = 0xF5
	colTypeNewDecimal = 0xF6
	colTypeEnum       = 0xF7
	colTypeSet        = 0xF8
	colTypeTinyBlob   = 0xF9
	colTypeMediumBlob = 0xFA
	colTypeLongBlob   = 0xFB
	colTypeBlob       = 0xFC
	colTypeVarString  = 0xFD
	colTypeString     = 0xFE
	colTypeGeometry   = 0xFF
)

const colUnsignedFlag = 0x20

// Column describes one SELECTed column, decoded from its
// Protocol::ColumnDefinition41 packet.
type Column struct {
	Name     string
	Type     byte
	Unsigned bool
	Decimals byte
}

// ResultSet is a SELECT (or other row-returning statement)'s decoded
// output: Columns in order, and Rows in order, each row one value per
// column (nil for SQL NULL).
type ResultSet struct {
	Columns []Column
	Rows    [][]any
}

// byteCursor is a small forward-only reader over an in-memory packet
// payload, for the length-encoded-string/fixed-width-field parsing
// column-definition and binary-row decoding both need. Every method
// reports ok=false (instead of panicking) if the cursor runs off the end of
// buf — the caller turns that into a descriptive error.
type byteCursor struct {
	buf []byte
	pos int
}

func (c *byteCursor) readByte() (byte, bool) {
	if c.pos >= len(c.buf) {
		return 0, false
	}
	b := c.buf[c.pos]
	c.pos++
	return b, true
}

func (c *byteCursor) readBytes(n int) ([]byte, bool) {
	if n < 0 || c.pos+n > len(c.buf) {
		return nil, false
	}
	b := c.buf[c.pos : c.pos+n]
	c.pos += n
	return b, true
}

func (c *byteCursor) readLenEncInt() (uint64, bool) {
	first, ok := c.readByte()
	if !ok {
		return 0, false
	}
	switch {
	case first < 0xfb:
		return uint64(first), true
	case first == 0xfc:
		b, ok := c.readBytes(2)
		if !ok {
			return 0, false
		}
		return uint64(binary.LittleEndian.Uint16(b)), true
	case first == 0xfd:
		b, ok := c.readBytes(3)
		if !ok {
			return 0, false
		}
		return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16, true
	case first == 0xfe:
		b, ok := c.readBytes(8)
		if !ok {
			return 0, false
		}
		return binary.LittleEndian.Uint64(b), true
	default: // 0xfb is the NULL marker in this encoding family; not valid here.
		return 0, false
	}
}

func (c *byteCursor) readLenEncStr() (string, bool) {
	n, ok := c.readLenEncInt()
	if !ok {
		return "", false
	}
	b, ok := c.readBytes(int(n))
	if !ok {
		return "", false
	}
	return string(b), true
}

// decodeColumnDef parses one Protocol::ColumnDefinition41 packet.
func decodeColumnDef(payload []byte) (Column, error) {
	c := &byteCursor{buf: payload}
	// catalog, schema, table, org_table — present but unused here.
	for i := 0; i < 4; i++ {
		if _, ok := c.readLenEncStr(); !ok {
			return Column{}, fmt.Errorf("column definition: truncated before field %d", i)
		}
	}
	name, ok := c.readLenEncStr()
	if !ok {
		return Column{}, fmt.Errorf("column definition: truncated name")
	}
	if _, ok := c.readLenEncStr(); !ok { // org_name
		return Column{}, fmt.Errorf("column definition: truncated org_name")
	}
	fixedLen, ok := c.readLenEncInt()
	if !ok || fixedLen < 10 {
		return Column{}, fmt.Errorf("column definition: bad fixed-fields length")
	}
	fixed, ok := c.readBytes(int(fixedLen))
	if !ok {
		return Column{}, fmt.Errorf("column definition: truncated fixed fields")
	}
	// fixed layout: charset(2) length(4) type(1) flags(2) decimals(1) [filler(2)]
	colType := fixed[6]
	flags := binary.LittleEndian.Uint16(fixed[7:9])
	decimals := fixed[9]

	return Column{
		Name:     name,
		Type:     colType,
		Unsigned: flags&colUnsignedFlag != 0,
		Decimals: decimals,
	}, nil
}

// decodeBinaryRow parses one binary-protocol row packet (the leading 0x00
// header byte already consumed by the caller) into one value per column —
// nil for a column the null-bitmap marks NULL.
func decodeBinaryRow(payload []byte, cols []Column) ([]any, error) {
	c := &byteCursor{buf: payload}
	if _, ok := c.readByte(); !ok { // the row packet's constant 0x00 header
		return nil, fmt.Errorf("row packet: missing header byte")
	}

	// NULL-bitmap: (num_fields + 7 + 2) / 8 bytes, with a 2-bit offset
	// before the first field's bit (per the binary protocol row format).
	bitmapLen := (len(cols) + 7 + 2) / 8
	bitmap, ok := c.readBytes(bitmapLen)
	if !ok {
		return nil, fmt.Errorf("row packet: truncated null-bitmap")
	}
	isNull := func(i int) bool {
		bitPos := i + 2
		return bitmap[bitPos/8]&(1<<uint(bitPos%8)) != 0
	}

	values := make([]any, len(cols))
	for i, col := range cols {
		if isNull(i) {
			values[i] = nil
			continue
		}
		v, err := decodeColumnValue(c, col)
		if err != nil {
			return nil, fmt.Errorf("column %d (%s): %w", i, col.Name, err)
		}
		values[i] = v
	}
	return values, nil
}

// decodeColumnValue decodes one non-NULL column value per col.Type, using
// int64/uint64 (per col.Unsigned)/float64/time.Time/time.Duration for the
// numeric/temporal families. String-family types (VARCHAR, TEXT/BLOB,
// DECIMAL, JSON, ENUM, SET, BIT, GEOMETRY) are returned as []byte rather
// than string — this package doesn't know a column's charset well enough to
// decide when a []byte→string conversion is safe, so it leaves that
// decision, and the cost of making it, to the caller.
func decodeColumnValue(c *byteCursor, col Column) (any, error) {
	switch col.Type {
	case colTypeTiny:
		b, ok := c.readByte()
		if !ok {
			return nil, fmt.Errorf("truncated TINY")
		}
		if col.Unsigned {
			return uint64(b), nil
		}
		return int64(int8(b)), nil

	case colTypeShort, colTypeYear:
		b, ok := c.readBytes(2)
		if !ok {
			return nil, fmt.Errorf("truncated SHORT/YEAR")
		}
		u := binary.LittleEndian.Uint16(b)
		if col.Unsigned {
			return uint64(u), nil
		}
		return int64(int16(u)), nil

	case colTypeLong, colTypeInt24:
		b, ok := c.readBytes(4)
		if !ok {
			return nil, fmt.Errorf("truncated LONG/INT24")
		}
		u := binary.LittleEndian.Uint32(b)
		if col.Unsigned {
			return uint64(u), nil
		}
		return int64(int32(u)), nil

	case colTypeLongLong:
		b, ok := c.readBytes(8)
		if !ok {
			return nil, fmt.Errorf("truncated LONGLONG")
		}
		u := binary.LittleEndian.Uint64(b)
		if col.Unsigned {
			return u, nil
		}
		return int64(u), nil

	case colTypeFloat:
		b, ok := c.readBytes(4)
		if !ok {
			return nil, fmt.Errorf("truncated FLOAT")
		}
		return float64(math.Float32frombits(binary.LittleEndian.Uint32(b))), nil

	case colTypeDouble:
		b, ok := c.readBytes(8)
		if !ok {
			return nil, fmt.Errorf("truncated DOUBLE")
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil

	case colTypeDate, colTypeDateTime, colTypeTimestamp:
		return decodeDateTime(c)

	case colTypeTime:
		return decodeTime(c)

	case colTypeDecimal, colTypeNewDecimal, colTypeVarChar, colTypeVarString,
		colTypeString, colTypeTinyBlob, colTypeMediumBlob, colTypeLongBlob,
		colTypeBlob, colTypeJSON, colTypeEnum, colTypeSet, colTypeBit,
		colTypeGeometry:
		n, ok := c.readLenEncInt()
		if !ok {
			return nil, fmt.Errorf("truncated length-encoded value")
		}
		b, ok := c.readBytes(int(n))
		if !ok {
			return nil, fmt.Errorf("truncated length-encoded value body")
		}
		return append([]byte(nil), b...), nil

	case colTypeNull:
		return nil, nil

	default:
		return nil, fmt.Errorf("unsupported column type 0x%02x", col.Type)
	}
}

// decodeDateTime parses the packed DATE/DATETIME/TIMESTAMP binary value: a
// 1-byte length (0, 4, 7, or 11) followed by that many fields.
func decodeDateTime(c *byteCursor) (any, error) {
	n, ok := c.readByte()
	if !ok {
		return nil, fmt.Errorf("truncated date/time length")
	}
	if n == 0 {
		return time.Time{}, nil
	}
	b, ok := c.readBytes(int(n))
	if !ok {
		return nil, fmt.Errorf("truncated date/time body")
	}
	if len(b) < 4 {
		return nil, fmt.Errorf("date/time body too short (%d bytes)", len(b))
	}
	year := int(binary.LittleEndian.Uint16(b[0:2]))
	month := time.Month(b[2])
	day := int(b[3])
	hour, minute, second, micro := 0, 0, 0, 0
	if len(b) >= 7 {
		hour, minute, second = int(b[4]), int(b[5]), int(b[6])
	}
	if len(b) >= 11 {
		micro = int(binary.LittleEndian.Uint32(b[7:11]))
	}
	return time.Date(year, month, day, hour, minute, second, micro*1000, time.UTC), nil
}

// decodeTime parses the packed TIME binary value: a 1-byte length (0, 8, or
// 12) followed by is_negative(1) days(4) hours(1) minutes(1) seconds(1)
// [microseconds(4)].
func decodeTime(c *byteCursor) (any, error) {
	n, ok := c.readByte()
	if !ok {
		return nil, fmt.Errorf("truncated TIME length")
	}
	if n == 0 {
		return time.Duration(0), nil
	}
	b, ok := c.readBytes(int(n))
	if !ok {
		return nil, fmt.Errorf("truncated TIME body")
	}
	if len(b) < 8 {
		return nil, fmt.Errorf("TIME body too short (%d bytes)", len(b))
	}
	negative := b[0] != 0
	days := binary.LittleEndian.Uint32(b[1:5])
	hours, minutes, seconds := b[5], b[6], b[7]
	micro := uint32(0)
	if len(b) >= 12 {
		micro = binary.LittleEndian.Uint32(b[8:12])
	}
	d := time.Duration(days)*24*time.Hour +
		time.Duration(hours)*time.Hour +
		time.Duration(minutes)*time.Minute +
		time.Duration(seconds)*time.Second +
		time.Duration(micro)*time.Microsecond
	if negative {
		d = -d
	}
	return d, nil
}
