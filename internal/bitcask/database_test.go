package bitcask

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDatabaseSetGet(t *testing.T) {
	db := &Database{
		data: make(map[string]string),
		path: "",
	}

	// test Set/Get
	db.Set("foo", "bar")
	val, ok := db.Get("foo")
	if !ok || val != "bar" {
		t.Errorf("expected 'bar', got '%s'", val)
	}

	// test missing key
	_, ok = db.Get("missing")
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
	db.Set("key1", "value1")
	db.Set("key2", "value2")

	if err := db.Save(); err != nil {
		t.Fatal(err)
	}

	// load again
	db2, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	// verify values persisted
	val, ok := db2.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("expected 'value1', got '%s'", val)
	}

	val, ok = db2.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("expected 'value2', got '%s'", val)
	}
}

func TestDatabaseHandlesMalformedLines(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")

	// write malformed content manually
	content := "goodkey:goodvalue\nbadlinewithoutseparator\nanother:ok\n"
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	if val, ok := db.Get("goodkey"); !ok || val != "goodvalue" {
		t.Errorf("expected 'goodvalue', got '%s'", val)
	}

	if val, ok := db.Get("another"); !ok || val != "ok" {
		t.Errorf("expected 'ok', got '%s'", val)
	}
}
