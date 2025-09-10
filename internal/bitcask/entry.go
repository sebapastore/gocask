package bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

// timestamp (64 bits) + keySize (32 bits) + valueSize (32 bits)
const headerSize = 16

type Entry struct {
	Timestamp int64
	Key       string
	Value     string
}

// NewEntry creates a new entry with the current timestamp.
func NewEntry(key string, value string) *Entry {
	return &Entry{
		Timestamp: time.Now().Unix(),
		Key:       key,
		Value:     value,
	}
}

// Encode serializes the entry into bytes (CRC + payload).
func (e *Entry) Encode() ([]byte, error) {
	// CRC + header + key + value
	totalSize := 4 + headerSize + e.KeySize() + e.ValueSize()
	buf := make([]byte, totalSize)

	// fill payload directly into buf[4:]
	e.fillPayload(buf[4:])

	// calculate CRC over payload
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf, nil
}

// fillPayload writes the payload (header + key + value) into buf.
// buf must be pre-allocated with the correct size.
func (e *Entry) fillPayload(buf []byte) {
	// header
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Timestamp))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(e.KeySize()))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(e.ValueSize()))

	// key
	copy(buf[headerSize:], e.Key)

	// value
	copy(buf[headerSize+e.KeySize():], e.Value)
}

// encodePayload builds and returns only the payload (header + key + value).
func (e *Entry) EncodePayload() []byte {
	payload := make([]byte, headerSize+e.KeySize()+e.ValueSize())
	e.fillPayload(payload)
	return payload
}

func (e *Entry) KeySize() int32 {
	return int32(len(e.Key))
}

func (e *Entry) ValueSize() int32 {
	return int32(len(e.Value))
}

// ValueOffset returns the length in bytes of the header before the value
func (e *Entry) ValueOffset() int64 {
	// CRC (4 bytes) + Timestamp (8 bytes) + KeySize (4 bytes) + ValueSize (4 bytes) + Key bytes
	return 4 + 8 + 4 + 4 + int64(e.KeySize())
}
