package bitcask

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const defaultMaxFileSize = 100 * 1024 * 1024 // 100 MB

var TombstoneValue = []byte{0xDE, 0xAD, 0xBE, 0xEF} // tombstone: non-ASCII/UTF-8 bytes, used to mark deletions

type KeydirEntry struct {
	FileID    uint64
	ValuePos  uint64
	ValueSize uint32
	Timestamp uint64
}

type Database struct {
	maxFileSize  uint64
	keydir       map[string]KeydirEntry
	dbPath       string
	activeFile   *os.File
	activeFileID uint64
	files        map[uint64]*os.File
}

func NewDatabase(dbPath string, maxFileSize uint64) *Database {
	if maxFileSize == 0 {
		maxFileSize = defaultMaxFileSize
	}

	return &Database{
		keydir:      make(map[string]KeydirEntry),
		dbPath:      dbPath,
		maxFileSize: maxFileSize,
		files:       make(map[uint64]*os.File),
	}
}

func (db *Database) Open() error {
	files, err := filepath.Glob(filepath.Join(db.dbPath, "data.*.cask"))
	if err != nil {
		return fmt.Errorf("failed to list segment files: %w", err)
	}

	fileIDs := parseSegmentFileIDs(files)

	// Create first DB file if none exists and return
	if len(fileIDs) == 0 {
		const activeFileID = 1
		f, err := db.createNewDBFile(activeFileID)
		if err != nil {
			return err
		}
		db.activeFile = f
		db.activeFileID = activeFileID
		db.files[activeFileID] = f
		return nil
	}

	// Load keydir from all files in order
	for _, id := range fileIDs {
		if err := db.loadKeydirFromFileID(id); err != nil {
			return fmt.Errorf("failed to load keydir from file id %d: %w", id, err)
		}
	}

	// Set activeFile
	if len(fileIDs) > 0 {
		activeFileID := fileIDs[len(fileIDs)-1]
		f, err := db.getDBFileByID(activeFileID)
		if err != nil {
			return fmt.Errorf("failed to open active db file: %w", err)
		}
		db.activeFile = f
		db.activeFileID = activeFileID
	}

	return nil
}

func (db *Database) Close() error {
	for _, f := range db.files {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) Get(key string) (string, bool, error) {
	if len(db.files) == 0 {
		return "", false, fmt.Errorf("the database is not fully initialized: there are not db files")
	}

	meta, exists := db.keydir[key]
	if !exists {
		return "", false, nil
	}

	// Seek to value position from specific file
	if _, err := db.files[meta.FileID].Seek(int64(meta.ValuePos), io.SeekStart); err != nil {
		return "", false, fmt.Errorf("failed to seek to value: %w", err)
	}

	// Read value
	value := make([]byte, meta.ValueSize)
	if _, err := db.files[meta.FileID].Read(value); err != nil {
		return "", false, fmt.Errorf("failed to read value: %w", err)
	}

	if bytes.Equal(value, TombstoneValue) {
		return "", false, nil
	}

	return string(value), true, nil
}

func (db *Database) Set(key string, value string) error {
	if db.activeFile == nil {
		return fmt.Errorf("the database is not fully initialized: there is not an active file")
	}

	entry := NewEntry(key, value)

	// Calculate value position
	fileOffset, err := db.activeFile.Seek(0, io.SeekEnd) // current end of file
	if err != nil {
		return fmt.Errorf("failed to seek database file: %w", err)
	}
	valuePos := fileOffset + entry.ValueOffset()

	// Encode
	data, err := entry.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode entry: %w", err)
	}

	// Check if adding this entry would exceed maxFileSize
	if uint64(fileOffset)+uint64(len(data)) > db.maxFileSize {
		// rotate to a new file (or handle however you want)
		if err := db.rotateActiveFile(); err != nil {
			return fmt.Errorf("failed to rotate file: %w", err)
		}
		valuePos = entry.ValueOffset()
	}

	if _, err := db.activeFile.Write(data); err != nil {
		return fmt.Errorf("failed to write entry: %w", err)
	}

	// Update keydir
	db.keydir[key] = KeydirEntry{
		FileID:    db.activeFileID,
		ValuePos:  uint64(valuePos),
		ValueSize: uint32(len(entry.Value)),
		Timestamp: entry.Timestamp,
	}

	return nil
}

func (db *Database) Delete(key string) error {
	return db.Set(key, string(TombstoneValue))
}

func (db *Database) createNewDBFile(fileID uint64) (*os.File, error) {
	filePath := filepath.Join(db.dbPath, fmt.Sprintf("data.%d.cask", fileID))
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create file with ID %d: %w", fileID, err)
	}
	return f, nil
}

func (db *Database) rotateActiveFile() error {
	activeFileBaseName := filepath.Base(db.activeFile.Name())
	activeFileID, err := extractDBFileID(activeFileBaseName)

	if err != nil {
		return err
	}

	newActiveFileID := uint64(activeFileID + 1)

	f, err := db.createNewDBFile(newActiveFileID)

	if err != nil {
		return fmt.Errorf("failed to rotate db file: %w", err)
	}

	db.activeFile = f
	db.activeFileID = newActiveFileID
	db.files[newActiveFileID] = f

	return nil
}

func (db *Database) loadKeydirFromFileID(fileID uint64) error {
	filePath := db.getDBFilePathByID(fileID)
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	db.files[fileID] = f

	var offset uint64 = 0

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader := bufio.NewReader(f)

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

		db.keydir[decodedEntry.Key] = db.buildKeydirEntry(offset, decodedEntry, fileID)
		offset += uint64(decodedEntry.EntrySize)
	}

	return nil
}

func (db *Database) getDBFileByID(id uint64) (*os.File, error) {
	filePath := db.getDBFilePathByID(id)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	return f, nil
}

func (db *Database) getDBFilePathByID(id uint64) string {
	return filepath.Join(db.dbPath, fmt.Sprintf("data.%d.cask", id))
}

func (db *Database) buildKeydirEntry(entryOffset uint64, decodedEntry *DecodedEntry, fileID uint64) KeydirEntry {
	return KeydirEntry{
		FileID:    fileID,
		ValuePos:  entryOffset + headerSize + uint64(decodedEntry.KeySize),
		ValueSize: decodedEntry.ValueSize,
		Timestamp: decodedEntry.Timestamp,
	}
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

func parseSegmentFileIDs(files []string) []uint64 {
	var ids []uint64
	for _, f := range files {
		base := filepath.Base(f)
		id, err := extractDBFileID(base)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}

	// ensure ascending order
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	return ids
}

func extractDBFileID(fileName string) (uint64, error) {
	// Strict format: data.<number>.cask
	const prefix = "data."
	const suffix = ".cask"

	if !strings.HasPrefix(fileName, prefix) || !strings.HasSuffix(fileName, suffix) {
		return 0, fmt.Errorf("invalid file name %q: must match data.<number>.cask", fileName)
	}

	// cut off prefix and suffix
	inner := strings.TrimPrefix(fileName, prefix)
	inner = strings.TrimSuffix(inner, suffix)

	if inner == "" {
		return 0, fmt.Errorf("invalid file name %q: missing number between data. and .cask", fileName)
	}

	id, err := strconv.ParseUint(inner, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse ID from file %q: %w", fileName, err)
	}

	return id, nil
}
