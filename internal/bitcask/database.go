package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const defaultMaxFileSize = 100 * 1024 * 1024 // 100 MB

type KeydirEntry struct {
	FileID    uint32
	ValuePos  uint64
	ValueSize uint32
	Timestamp uint64
}

type Database struct {
	maxFileSize uint64
	keydir      map[string]KeydirEntry
	dbPath      string
	activeFile  *os.File
}

func NewDatabase(dbPath string, maxFileSize uint64) *Database {
	if maxFileSize == 0 {
		maxFileSize = defaultMaxFileSize
	}

	return &Database{
		keydir:      make(map[string]KeydirEntry),
		dbPath:      dbPath,
		maxFileSize: maxFileSize,
	}
}

func (db *Database) Open() error {
	files, err := filepath.Glob(filepath.Join(db.dbPath, "data.*.cask"))
	if err != nil {
		return fmt.Errorf("failed to list segment files: %w", err)
	}

	fileIDs := parseSegmentFileIDs(files)

	// Create first DB file if none exists yet
	if len(fileIDs) == 0 {
		f, err := db.createNewDBFile(1)
		if err != nil {
			return err
		}
		db.activeFile = f
		return nil
	}

	// Load keydir from all files in order
	for _, id := range fileIDs {
		filePath := db.getDBFilePathByID(id)
		if err := db.loadKeydirFromFile(filePath); err != nil {
			return fmt.Errorf("failed to load keydir from %s: %w", filePath, err)
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
	if db.activeFile == nil {
		return "", false, fmt.Errorf("the database is not fully initialized: there is not active file")
	}

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
	if db.activeFile == nil {
		return fmt.Errorf("the database is not fully initialized: there is not active file")
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

func (db *Database) createNewDBFile(fileID uint64) (*os.File, error) {
	filePath := filepath.Join(db.dbPath, fmt.Sprintf("data.%d.cask", fileID))
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create file with ID %d: %w", fileID, err)
	}
	return f, nil
}

func (db *Database) rotateActiveFile() error {
	currentFileID, err := extractDBFileID(db.activeFile.Name())

	if err != nil {
		return err
	}

	newxFileID := uint64(currentFileID + 1)

	f, err := db.createNewDBFile(newxFileID)

	if err != nil {
		return fmt.Errorf("failed to rotate db file: %w", err)
	}

	db.activeFile = f

	return nil
}

func (db *Database) loadKeydirFromFile(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("failed to close file: %v", err)
		}
	}()

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

		fileID := uint32(1) // TODO: implement multiple files
		db.keydir[decodedEntry.Key] = buildKeydirEntry(fileID, offset, decodedEntry)
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
	parts := strings.Split(fileName, ".")
	if len(parts) < 3 {
		return 0, fmt.Errorf("failed to extract ID from file %s: the name has less than 3 parts", fileName)
	}
	id, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("failed to extract ID from file %s: %w", fileName, err)
	}
	return uint64(id), nil

}
