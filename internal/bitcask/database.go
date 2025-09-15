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
	keydir     map[string]KeydirEntry
	dbPath     string
	activeFile *os.File
}

func NewDatabase(dbPath string) *Database {
	return &Database{
		keydir: make(map[string]KeydirEntry),
		dbPath: dbPath,
	}
}

func (db *Database) Open() error {
	fileName := "data.1.cask" // TODO: implement multiple files
	filePath := db.dbPath + "/" + fileName

	// Open or create db file
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create database file: %w", err)
	}

	db.activeFile = f

	// TODO: implement multiple files
	if err := db.loadKeydir(); err != nil {
		return fmt.Errorf("failed to load keydir: %w", err)
	}

	return nil
}

func (db *Database) Close() error {
	if db.activeFile == nil {
		return nil
	}
	return db.activeFile.Close()
}

func (db *Database) Get(key string) (string, bool, error) {
	meta, exists := db.keydir[key]
	if !exists {
		return "", false, nil
	}

	// Seek to value position
	if _, err := db.activeFile.Seek(int64(meta.ValuePos), io.SeekStart); err != nil {
		return "", false, fmt.Errorf("failed to seek to value: %w", err)
	}

	// Read value
	value := make([]byte, meta.ValueSize)
	if _, err := db.activeFile.Read(value); err != nil {
		return "", false, fmt.Errorf("failed to read value: %w", err)
	}

	return string(value), true, nil
}

func (db *Database) Set(key string, value string) error {
	entry := NewEntry(key, value)

	// Calculate value position
	fileOffset, err := db.activeFile.Seek(0, io.SeekEnd) // current end of file
	if err != nil {
		return fmt.Errorf("failed to seek database file: %w", err)
	}
	valuePos := fileOffset + entry.ValueOffset()

	// Encode and write
	data, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}
	if _, err := db.activeFile.Write(data); err != nil {
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

func (db *Database) loadKeydir() error {
	var offset uint64 = 0

	if _, err := db.activeFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(db.activeFile)

	for {
		decodedEntry, err := decodeNextEntry(reader)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to decode entry at offset %d: %v\n", offset, err)
			// skip the corrupted entry by advancing the offset
			offset += uint64(decodedEntry.EntrySize)
			continue
		}

		fileID := uint32(1) // TODO: implement multiple files
		db.keydir[decodedEntry.Key] = buildKeydirEntry(fileID, offset, decodedEntry)
		offset += uint64(decodedEntry.EntrySize)
	}

	return nil
}

func decodeNextEntry(r io.Reader) (*DecodedEntry, error) {
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

func buildKeydirEntry(fileID uint32, entryOffset uint64, decodedEntry *DecodedEntry) KeydirEntry {
	return KeydirEntry{
		FileID:    fileID,
		ValuePos:  entryOffset + headerSize + uint64(decodedEntry.KeySize),
		ValueSize: decodedEntry.ValueSize,
		Timestamp: decodedEntry.Timestamp,
	}
}
