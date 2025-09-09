package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestEntryEncode(t *testing.T) {
	key := []byte("mykey")
	value := []byte("myvalue")

	e := &Entry{
		Timestamp: 1694280000, // fixed timestamp for deterministic test
		Key:       key,
		Value:     value,
	}

	data, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Encoded data is empty")
	}

	// Check CRC
	crcFromData := binary.LittleEndian.Uint32(data[:4])
	content := data[4:]
	expectedCRC := crc32.ChecksumIEEE(content)
	if crcFromData != expectedCRC {
		t.Fatalf("CRC mismatch: got %x, expected %x", crcFromData, expectedCRC)
	}

	// Check key and value are inside
	if !bytes.Contains(content, key) {
		t.Fatal("Encoded content missing key")
	}
	if !bytes.Contains(content, value) {
		t.Fatal("Encoded content missing value")
	}
}
