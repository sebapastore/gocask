package bitcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

type Entry struct {
	Timestamp int64
	Key       string
	KeySize   int32
	Value     string
	ValueSize int32
}

// NewEntry creates a new entry with the current timestamp.
func NewEntry(key string, value string) *Entry {
	return &Entry{
		Timestamp: time.Now().Unix(),
		Key:       key,
		Value:     value,
		KeySize:   int32(len(key)),
		ValueSize: int32(len(value)),
	}
}

// Encode serializes the entry into bytes (CRC + payload).
func (e *Entry) Encode() ([]byte, error) {
	payload, err := e.Payload()
	if err != nil {
		return nil, err
	}

	crc := crc32.ChecksumIEEE(payload)
	finalBuf := new(bytes.Buffer)

	if err := binary.Write(finalBuf, binary.LittleEndian, crc); err != nil {
		return nil, fmt.Errorf("failed to write CRC: %w", err)
	}

	if _, err := finalBuf.Write(payload); err != nil {
		return nil, fmt.Errorf("failed to write entry payload: %w", err)
	}

	return finalBuf.Bytes(), nil
}

// Payload: timestamp + keySize + valueSize + key + value
func (e *Entry) Payload() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.LittleEndian, e.Timestamp); err != nil {
		return nil, fmt.Errorf("failed to write timestamp: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, e.KeySize); err != nil {
		return nil, fmt.Errorf("failed to write key size: %w", err)
	}

	if err := binary.Write(buf, binary.LittleEndian, e.ValueSize); err != nil {
		return nil, fmt.Errorf("failed to write value size: %w", err)
	}

	if _, err := buf.Write([]byte(e.Key)); err != nil {
		return nil, fmt.Errorf("failed to write key bytes: %w", err)
	}

	if _, err := buf.Write([]byte(e.Value)); err != nil {
		return nil, fmt.Errorf("failed to write value bytes: %w", err)
	}

	return buf.Bytes(), nil
}

// HeaderLength returns the length in bytes of the header before the value
func (e *Entry) HeaderLength() int {
	// CRC (4 bytes) + Timestamp (8 bytes) + KeySize (4 bytes) + ValueSize (4 bytes) + Key bytes
	return 4 + 8 + 4 + 4 + len(e.Key)
}
