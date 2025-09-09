package bitcask

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunSetCommand(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")
	var buf bytes.Buffer

	args := []string{"--db", tmpFile, "set", "foo", "bar"}
	err := Run(args, &buf)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "SET key=foo value=bar") {
		t.Errorf("unexpected output: %s", output)
	}

	// Reload DB and check value persisted
	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	val, ok, _ := db.Get("foo")
	if !ok || val != "bar" {
		t.Errorf("expected foo=bar, got foo=%s", val)
	}
}

func TestRunGetCommand(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")
	db, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	_ = db.Set("hello", "world")

	var buf bytes.Buffer
	args := []string{"--db", tmpFile, "get", "hello"}
	err = Run(args, &buf)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "The value for key hello is world") {
		t.Errorf("unexpected output: %s", output)
	}
}

func TestRunGetMissingKey(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "testdb")
	_, err := NewDatabase(tmpFile)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	args := []string{"--db", tmpFile, "get", "missing"}
	err = Run(args, &buf)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "There is no value for key missing") {
		t.Errorf("unexpected output: %s", output)
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var buf bytes.Buffer
	args := []string{"foobar"}
	err := Run(args, &buf)
	if err != nil {
		t.Fatal(err)
	}

	output := buf.String()
	if !strings.Contains(output, "Unknown command: foobar") {
		t.Errorf("unexpected output: %s", output)
	}
}
