package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type KeydirEntry struct {
	FileID    int
	ValuePos  int64
	ValueSize int32
	Timestamp int64
}

type Database struct {
	keydir map[string]KeydirEntry
	path   string
}

func NewDatabase(path string) (*Database, error) {
	// Ensure file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create database file: %w", err)
		}
		defer closeFile(file)
		fmt.Println("Database created at", path)
	}

	db := &Database{
		keydir: make(map[string]KeydirEntry),
		path:   path,
	}

	// Open file for reading existing entries
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer closeFile(file)

	var offset int64 = 0
	reader := bufio.NewReader(file)

	for {
		entry, n, err := DecodeNextEntry(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to decode entry at offset %d: %v\n", offset, err)
			// skip the corrupted entry by advancing the offset
			offset += int64(n)
			continue
		}

		// Update keydir with file position info
		db.keydir[entry.Key] = KeydirEntry{
			FileID:    1, // currently only one file
			ValuePos:  offset + int64(entry.HeaderLength()),
			ValueSize: entry.ValueSize,
			Timestamp: entry.Timestamp,
		}

		offset += int64(n)
	}

	return db, nil
}

func DecodeNextEntry(r io.Reader) (*Entry, int, error) {
	var crc uint32
	var timestamp int64
	var keySize, valueSize int32

	// Keep track of total bytes read
	totalBytes := 0

	// Read CRC (4 bytes)
	if err := binary.Read(r, binary.LittleEndian, &crc); err != nil {
		return nil, totalBytes, err
	}
	totalBytes += 4

	// Read Timestamp (8 bytes)
	if err := binary.Read(r, binary.LittleEndian, &timestamp); err != nil {
		return nil, totalBytes, err
	}
	totalBytes += 8

	// Read KeySize (4 bytes)
	if err := binary.Read(r, binary.LittleEndian, &keySize); err != nil {
		return nil, totalBytes, err
	}
	totalBytes += 4

	// Read ValueSize (4 bytes)
	if err := binary.Read(r, binary.LittleEndian, &valueSize); err != nil {
		return nil, totalBytes, err
	}
	totalBytes += 4

	// Read Key
	keyBytes := make([]byte, keySize)
	n, err := io.ReadFull(r, keyBytes)
	if err != nil {
		return nil, totalBytes, err
	}
	totalBytes += n

	// Read Value
	valueBytes := make([]byte, valueSize)
	n, err = io.ReadFull(r, valueBytes)
	if err != nil {
		return nil, totalBytes, err
	}
	totalBytes += n

	entry := &Entry{
		Timestamp: timestamp,
		Key:       string(keyBytes),
		Value:     string(valueBytes),
		KeySize:   keySize,
		ValueSize: valueSize,
	}

	// Verify CRC
	payload, err := entry.Payload()

	if err != nil {
		return nil, totalBytes, err
	}

	if crc32.ChecksumIEEE(payload) != crc {
		return nil, totalBytes, fmt.Errorf("CRC mismatch for key %s", entry.Key)
	}

	return entry, totalBytes, nil
}

func (db *Database) Get(key string) (string, bool, error) {
	meta, exists := db.keydir[key]
	if !exists {
		return "", false, nil
	}

	// Open file to read value
	file, err := os.Open(db.path)
	if err != nil {
		return "", false, fmt.Errorf("failed to open database file: %w", err)
	}
	defer closeFile(file)

	// Seek to value position
	if _, err := file.Seek(meta.ValuePos, io.SeekStart); err != nil {
		return "", false, fmt.Errorf("failed to seek to value: %w", err)
	}

	// Read value
	value := make([]byte, meta.ValueSize)
	if _, err := file.Read(value); err != nil {
		return "", false, fmt.Errorf("failed to read value: %w", err)
	}

	return string(value), true, nil
}

func (db *Database) Set(key string, value string) error {
	entry := NewEntry(key, value)

	// Open file in append mode
	file, err := os.OpenFile(db.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open database file: %w", err)
	}
	defer closeFile(file)

	// Calculate value position
	fileOffset, err := file.Seek(0, io.SeekEnd) // current end of file
	if err != nil {
		return fmt.Errorf("failed to seek database file: %w", err)
	}
	valuePos := fileOffset + int64(entry.HeaderLength())

	// Encode and write
	data, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Update keydir
	db.keydir[key] = KeydirEntry{
		FileID:    1, //TODO: Handle multiple files
		ValuePos:  valuePos,
		ValueSize: entry.ValueSize,
		Timestamp: entry.Timestamp,
	}

	return nil
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to close file %s: %v\n", file.Name(), err)
	}
}
