package bitcask

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"time"
)

type Entry struct {
	Timestamp int64
	Key       []byte
	Value     []byte
}

// NewEntry creates a new entry with the current timestamp.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Timestamp: time.Now().Unix(),
		Key:       key,
		Value:     value,
	}
}

// Encode serializes the entry into bytes (CRC + fields).
func (e *Entry) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Timestamp
	if err := binary.Write(buf, binary.LittleEndian, e.Timestamp); err != nil {
		return nil, err
	}

	// Key size
	keySize := uint32(len(e.Key))
	if err := binary.Write(buf, binary.LittleEndian, keySize); err != nil {
		return nil, err
	}

	// Value size
	valueSize := uint32(len(e.Value))
	if err := binary.Write(buf, binary.LittleEndian, valueSize); err != nil {
		return nil, err
	}

	// Key
	if _, err := buf.Write(e.Key); err != nil {
		return nil, err
	}

	// Value
	if _, err := buf.Write(e.Value); err != nil {
		return nil, err
	}

	// Compute CRC
	crc := crc32.ChecksumIEEE(buf.Bytes())

	// Final buffer = CRC + data
	final := new(bytes.Buffer)
	if err := binary.Write(final, binary.LittleEndian, crc); err != nil {
		return nil, err
	}
	if _, err := final.Write(buf.Bytes()); err != nil {
		return nil, err
	}

	return final.Bytes(), nil
}
