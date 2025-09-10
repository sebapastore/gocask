package bitcask

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"
)

const (
	crcSize       = 4 // 4 bits for CRC
	timestampSize = 8 // 64 bits for timestamp
	keySizeSize   = 4 // 32 bits for key size
	valueSizeSize = 4 // 32 bits for value size
)

const metadataSize = timestampSize + keySizeSize + valueSizeSize
const headerSize = crcSize + metadataSize

const crcOffset = 0
const crcEnd = crcOffset + crcSize
const timestampOffset = crcSize
const timestampEnd = timestampOffset + timestampSize
const keySizeOffset = crcSize + timestampSize
const keySizeEnd = keySizeOffset + keySizeSize
const valueSizeOffset = crcSize + timestampSize + keySizeSize
const valueSizeEnd = valueSizeOffset + valueSizeSize
const keyOffset = crcSize + timestampSize + keySizeSize + valueSizeSize

type Entry struct {
	Timestamp uint64
	Key       string
	Value     string
}

type DecodedEntry struct {
	Key         string
	Timestamp   uint64
	KeySize     uint32
	ValueSize   uint32
	EntrySize   uint32
	ValueOffset uint32
}

// NewEntry creates a new entry with the current timestamp.
func NewEntry(key string, value string) *Entry {
	return &Entry{
		Timestamp: uint64(time.Now().Unix()),
		Key:       key,
		Value:     value,
	}
}

// Encode serializes the entry into bytes (CRC + payload).
func (e *Entry) Encode() ([]byte, error) {
	totalSize := headerSize + e.KeySize() + e.ValueSize()
	buf := make([]byte, totalSize)

	// metadata
	binary.LittleEndian.PutUint64(buf[timestampOffset:timestampEnd], uint64(e.Timestamp))
	binary.LittleEndian.PutUint32(buf[keySizeOffset:keySizeEnd], uint32(e.KeySize()))
	binary.LittleEndian.PutUint32(buf[valueSizeOffset:valueSizeEnd], uint32(e.ValueSize()))

	// key and value
	copy(buf[keyOffset:], e.Key)
	copy(buf[e.ValueOffset():], []byte(e.Value))

	// calculate CRC over payload
	crc := crc32.ChecksumIEEE(buf[crcEnd:])
	binary.LittleEndian.PutUint32(buf[0:crcEnd], crc)

	return buf, nil
}

func Decode(headerBuf, kvBuf []byte, keySize, valueSize uint32) (*DecodedEntry, error) {
	crc := binary.LittleEndian.Uint32(headerBuf[crcOffset:])
	key := string(kvBuf[0:keySize])

	payload := make([]byte, metadataSize+len(kvBuf))
	copy(payload, headerBuf[crcEnd:])
	copy(payload[metadataSize:], kvBuf)

	if crc32.ChecksumIEEE(payload) != crc {
		return nil, fmt.Errorf("CRC mismatch for key %s", key)
	}

	timestamp := binary.LittleEndian.Uint64(headerBuf[timestampOffset:timestampEnd])
	valueOffset := headerSize + keySize

	decodedEntry := DecodedEntry{
		key,
		timestamp,
		keySize,
		valueSize,
		valueOffset + valueSize,
		valueOffset,
	}

	return &decodedEntry, nil
}

func (e *Entry) KeySize() int {
	return len(e.Key)
}

func (e *Entry) ValueSize() int {
	return len(e.Value)
}

func (e *Entry) ValueOffset() int64 {
	return headerSize + int64(e.KeySize())
}
