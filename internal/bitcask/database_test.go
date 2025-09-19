package bitcask

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseSetAndGet(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	// set
	require.NoError(t, db.Set("foo", "bar"))

	// get
	val, ok, err := db.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "bar", val)
}

func TestDatabaseDelete(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	// set
	require.NoError(t, db.Set("foo", "bar"))

	// delete
	err := db.Delete("foo")
	require.NoError(t, err)

	// get
	val, ok, err := db.Get("foo")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", val)
}

func TestDatabaseDeleteAndReWrite(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	// set
	require.NoError(t, db.Set("foo", "bar"))

	// delete
	err := db.Delete("foo")
	require.NoError(t, err)

	// set
	require.NoError(t, db.Set("foo", "buz"))

	// get
	val, ok, err := db.Get("foo")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "buz", val)
}

func TestDatabaseDeletePersistance(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())

	// set
	require.NoError(t, db.Set("foo", "bar"))

	// delete
	err := db.Delete("foo")
	require.NoError(t, err)

	// Close an reopen the database
	_ = db.Close()
	_ = db.Open()

	// get
	val, ok, err := db.Get("foo")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", val)
}

func TestDatabasePersistence(t *testing.T) {
	dir := t.TempDir()

	// First open and write
	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	require.NoError(t, db.Set("key1", "val1"))
	require.NoError(t, db.Set("key2", "val2"))
	require.NoError(t, db.Close())

	// Reopen and check values are still there
	db = NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val2", val)
}

func TestDatabaseHandlesMalformedEntries(t *testing.T) {
	dir := t.TempDir()

	db := NewDatabase(dir, 0)
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	// Create valid entries
	entry1 := NewEntry("key1", "val1")
	entry2 := NewEntry("key2", "val2")
	entry3 := NewEntry("key3", "val3")

	entry1Encoded, _ := entry1.Encode()
	_, _ = db.activeFile.Write(entry1Encoded)

	entry2Encoded, _ := entry1.Encode()
	_, _ = db.activeFile.Write(entry2Encoded)
	// Flip a byte in the value portion
	valueStart := entry2.ValueOffset()
	entry2Encoded[valueStart] ^= 0xAA

	entry3Encoded, _ := entry3.Encode()
	_, _ = db.activeFile.Write(entry3Encoded)

	require.NoError(t, db.Close())
	require.NoError(t, db.Open())

	// Check that valid entries are loaded
	if val, ok, _ := db.Get("key1"); !ok || val != "val1" {
		t.Errorf("expected 'goodvalue', got '%v'", val)
	}

	if val, ok, _ := db.Get("key3"); !ok || val != "val3" {
		t.Errorf("expected 'ok', got '%v'", val)
	}

	// Check that malformed entry did not break loading
	if _, ok, _ := db.Get("key2"); ok {
		t.Errorf("malformed entry should not exist")
	}
}

func TestFileRotationOnMaxSize(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 70) // 70 bytes
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	_ = db.Set("key1", "value1") // 30 bytes
	_ = db.Set("key2", "value2") // 30 bytes

	require.Equal(t, uint64(1), db.activeFileID)
	require.Equal(t, "data.1.cask", filepath.Base(db.activeFile.Name()))

	_ = db.Set("key3", "value3") // 30 bytes

	require.Equal(t, uint64(2), db.activeFileID)
	require.Equal(t, "data.2.cask", filepath.Base(db.activeFile.Name()))

	// Check sizes
	file1Info, _ := db.files[1].Stat()
	require.Equal(t, int64(60), file1Info.Size())

	file2Info, _ := db.files[2].Stat()
	require.Equal(t, int64(30), file2Info.Size())
}

func TestGetValuesAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 70) // 70 bytes
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	_ = db.Set("key1", "value1") // 30 bytes
	_ = db.Set("key2", "value2") // 30 bytes
	_ = db.Set("key3", "value3") // 30 bytes

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value2", val)

	val, ok, err = db.Get("key3")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value3", val)
}

func TestPersistenceAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 70) // 70 bytes
	require.NoError(t, db.Open())

	_ = db.Set("key1", "value1") // 30 bytes
	_ = db.Set("key2", "value2") // 30 bytes
	_ = db.Set("key3", "value3") // 30 bytes
	_ = db.Close()

	require.NoError(t, db.Open())

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value2", val)

	val, ok, err = db.Get("key3")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value3", val)

	_ = db.Close()
}

func TestDeletionAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 70) // 70 bytes
	require.NoError(t, db.Open())
	defer func() { _ = db.Close() }()

	_ = db.Set("key1", "value1") // 30 bytes
	_ = db.Set("key2", "value2") // 30 bytes
	_ = db.Set("key3", "value3") // 30 bytes
	_ = db.Delete("key2")        // 28 bytes (tombstone has 4 bytes)

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", val)

	val, ok, err = db.Get("key3")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value3", val)
}

func TestDeletetionPersistenceAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir, 70)
	require.NoError(t, db.Open())

	_ = db.Set("key1", "value1") // 30 bytes
	_ = db.Set("key2", "value2") // 30 bytes
	_ = db.Set("key3", "value3") // 30 bytes
	_ = db.Delete("key2")        // 28 bytes (tombstone has 4 bytes)
	_ = db.Close()

	require.NoError(t, db.Open())

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", val)

	val, ok, err = db.Get("key3")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "value3", val)

	_ = db.Close()
}
