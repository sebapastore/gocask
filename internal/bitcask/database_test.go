package bitcask

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseSetGet(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")

	// create and save
	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	// test Set/Get
	_ = db.Set("foo", "bar")
	val, ok, _ := db.Get("foo")
	if !ok || val != "bar" {
		t.Errorf("expected 'bar', got '%s'", val)
	}

	// test missing key
	_, ok, _ = db.Get("missing")
	if ok {
		t.Errorf("expected missing key to be absent")
	}
}

func TestDatabasePersistence(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")

	// create and save
	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Set("key1", "value1")
	_ = db.Set("key2", "value2")

	// load again
	db2, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	// verify values persisted
	val, ok, _ := db2.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("expected 'value1', got '%s'", val)
	}

	val, ok, _ = db2.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("expected 'value2', got '%s'", val)
	}
}

func TestDatabaseHandlesMalformedEntries(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")

	// Create valid entries
	entry1 := NewEntry("key1", "val1")
	entry2 := NewEntry("key2", "val2")
	entry3 := NewEntry("key3", "val3")

	// Write valid entries to file
	file, err := os.Create(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = file.Close() }()

	entry1Encoded, _ := entry1.Encode()
	_, _ = file.Write(entry1Encoded)

	entry2Encoded, _ := entry1.Encode()
	_, _ = file.Write(entry2Encoded)
	// Flip a byte in the value portion
	valueStart := entry2.HeaderLength()
	entry2Encoded[valueStart] ^= 0xAA

	entry3Encoded, _ := entry3.Encode()
	_, _ = file.Write(entry3Encoded)

	// Load database
	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

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
