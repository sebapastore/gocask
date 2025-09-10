package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type KeydirEntry struct {
	FileID    uint32
	ValuePos  uint64
	ValueSize uint32
	Timestamp uint64
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

	var offset uint64 = 0
	reader := bufio.NewReader(file)

	for {
		decodedEntry, err := DecodeNextEntry(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to decode entry at offset %d: %v\n", offset, err)
			// skip the corrupted entry by advancing the offset
			offset += uint64(decodedEntry.EntrySize)
			continue
		}
		fmt.Printf("decoded: %d\n", decodedEntry.ValueOffset)

		fileID := uint32(1) // TODO: implement multiple files

		fmt.Printf("key dir")
		db.keydir[decodedEntry.Key] = buildKeydirEntry(fileID, offset, decodedEntry)

		offset += uint64(decodedEntry.EntrySize)
	}

	return db, nil
}

func DecodeNextEntry(r io.Reader) (*DecodedEntry, error) {
	// 1. Read the fixed-size header
	headerBuf := make([]byte, headerSize)
	if _, err := io.ReadFull(r, headerBuf); err != nil {
		return nil, err // Can be io.EOF
	}

	// 2. Extract sizes from the header to know how much more to read
	keySize := binary.LittleEndian.Uint32(headerBuf[keySizeOffset:])
	valueSize := binary.LittleEndian.Uint32(headerBuf[valueSizeOffset:])

	// 3. Read the variable-sized key and value
	kvBuf := make([]byte, keySize+valueSize)
	if _, err := io.ReadFull(r, kvBuf); err != nil {
		return nil, err
	}

	decodedEntry, err := Decode(headerBuf, kvBuf, keySize, valueSize)

	if err != nil {
		return nil, err
	}

	return decodedEntry, nil
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
	if _, err := file.Seek(int64(meta.ValuePos), io.SeekStart); err != nil {
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
	valuePos := fileOffset + entry.ValueOffset()

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
		ValuePos:  uint64(valuePos),
		ValueSize: uint32(len(entry.Value)),
		Timestamp: entry.Timestamp,
	}

	return nil
}

func buildKeydirEntry(fileID uint32, entryOffset uint64, decodedEntry *DecodedEntry) KeydirEntry {
	return KeydirEntry{
		FileID:    fileID,
		ValuePos:  entryOffset + headerSize + uint64(decodedEntry.KeySize),
		ValueSize: decodedEntry.ValueSize,
		Timestamp: decodedEntry.Timestamp,
	}
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to close file %s: %v\n", file.Name(), err)
	}
}
