package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestEntryEncode(t *testing.T) {
	key := "mykey"
	value := "myvalue"

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
	if !bytes.Contains(content, []byte(key)) {
		t.Fatal("Encoded content missing key")
	}
	if !bytes.Contains(content, []byte(value)) {
		t.Fatal("Encoded content missing value")
	}
}

func TestEntryHeaderLength(t *testing.T) {
	key := "mykey"
	e := &Entry{
		Key: key,
	}

	expected := 4 + 8 + 4 + 4 + len(key) // CRC + Timestamp + KeySize + ValueSize + Key bytes
	got := e.HeaderLength()

	if got != expected {
		t.Fatalf("HeaderLength() = %d; want %d", got, expected)
	}
}
