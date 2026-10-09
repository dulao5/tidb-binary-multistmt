package binarymultistmt

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	comQuery       = 0x03
	comStmtPrepare = 0x16
	comStmtExecute = 0x17
	comStmtClose   = 0x19

	fieldTiny     = 0x01
	fieldLongLong = 0x08
	fieldDouble   = 0x05
	fieldNULL     = 0x06
	fieldString   = 0xFE

	fieldUnsignedFlag = 0x80

	// maxPacketPayload is the largest payload a single MySQL packet's
	// 3-byte length field can carry (2^24 - 1). This package doesn't
	// implement splitting one logical command across multiple packets (the
	// "packet length 0 terminates a multi-packet command" case in the real
	// protocol) or COM_STMT_SEND_LONG_DATA, so any single EXECUTE/PREPARE
	// payload must fit under this ceiling in one packet.
	maxPacketPayload = 1<<24 - 1
)

func putUint24(b []byte, n int) {
	b[0] = byte(n)
	b[1] = byte(n >> 8)
	b[2] = byte(n >> 16)
}

// writePacket writes one MySQL packet with sequence number 0 — every call
// site in this package starts a brand new command (never continues a
// >16MB multi-packet command), and each new command resets the sequence
// number to 0.
func writePacket(w io.Writer, payload []byte) error {
	if len(payload) > maxPacketPayload {
		// Without this check, putUint24 would silently truncate the length
		// header and corrupt the stream instead of erroring — see
		// maxPacketPayload's doc comment for why this package doesn't
		// (yet) support splitting across multiple packets.
		return fmt.Errorf("payload is %d bytes, exceeds this package's single-packet limit of %d (no multi-packet/SEND_LONG_DATA support yet)", len(payload), maxPacketPayload)
	}
	hdr := make([]byte, 4+len(payload))
	putUint24(hdr[:3], len(payload))
	hdr[3] = 0
	copy(hdr[4:], payload)
	_, err := w.Write(hdr)
	return err
}

func readPacket(r io.Reader) ([]byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func errPacketText(p []byte) string {
	if len(p) < 9 {
		return fmt.Sprintf("short ERR packet: % x", p)
	}
	code := binary.LittleEndian.Uint16(p[1:3])
	msg := string(p[9:])
	return fmt.Sprintf("code=%d msg=%s", code, msg)
}

// readLenEncInt decodes a MySQL length-encoded integer from the start of b.
func readLenEncInt(b []byte) uint64 {
	if len(b) == 0 {
		return 0
	}
	switch {
	case b[0] < 0xfb:
		return uint64(b[0])
	case b[0] == 0xfc && len(b) >= 3:
		return uint64(b[1]) | uint64(b[2])<<8
	case b[0] == 0xfd && len(b) >= 4:
		return uint64(b[1]) | uint64(b[2])<<8 | uint64(b[3])<<16
	case b[0] == 0xfe && len(b) >= 9:
		return binary.LittleEndian.Uint64(b[1:9])
	}
	return 0
}

// appendLenEncInt appends n to buf as a MySQL length-encoded integer (the
// write-side counterpart to readLenEncInt), choosing the shortest of the
// protocol's four forms.
func appendLenEncInt(buf []byte, n uint64) []byte {
	switch {
	case n < 0xfb:
		return append(buf, byte(n))
	case n <= 0xffff:
		buf = append(buf, 0xfc)
		return binary.LittleEndian.AppendUint16(buf, uint16(n))
	case n <= 0xffffff:
		var b3 [3]byte
		b3[0] = byte(n)
		b3[1] = byte(n >> 8)
		b3[2] = byte(n >> 16)
		return append(append(buf, 0xfd), b3[:]...)
	default:
		buf = append(buf, 0xfe)
		return binary.LittleEndian.AppendUint64(buf, n)
	}
}
