package bitcask

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseSetAndGet(t *testing.T) {
	dir := t.TempDir()
	db := NewDatabase(dir)
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

func TestDatabasePersistence(t *testing.T) {
	dir := t.TempDir()

	// First open and write
	db := NewDatabase(dir)
	require.NoError(t, db.Open())
	require.NoError(t, db.Set("key1", "val1"))
	require.NoError(t, db.Set("key2", "val2"))
	require.NoError(t, db.Close())

	// Reopen and check values are still there
	db = NewDatabase(dir)
	require.NoError(t, db.Open())

	val, ok, err := db.Get("key1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val1", val)

	val, ok, err = db.Get("key2")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val2", val)

	_ = db.Close()
}

func TestDatabaseHandlesMalformedEntries(t *testing.T) {
	dir := t.TempDir()

	// Create a file with valid data
	db := NewDatabase(dir)
	require.NoError(t, db.Open())
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
