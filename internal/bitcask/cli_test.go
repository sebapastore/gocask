package bitcask

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunSingleCommandSetAndGet(t *testing.T) {
	dir := t.TempDir()

	// 1. Run "set"
	out := &bytes.Buffer{}
	err := Run([]string{"--db", dir, "set", "foo", "bar"}, strings.NewReader(""), out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "SET key=foo value=bar")

	// 2. Run "get"
	out.Reset()
	err = Run([]string{"--db", dir, "get", "foo"}, strings.NewReader(""), out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "Value for key \"foo\" is \"bar\"")
}

func TestRunSingleCommandGetMissingKey(t *testing.T) {
	dir := t.TempDir()

	out := &bytes.Buffer{}
	err := Run([]string{"--db", dir, "get", "nope"}, strings.NewReader(""), out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "No value for key \"nope\"")
}

func TestRunUnknownCommand(t *testing.T) {
	dir := t.TempDir()

	out := &bytes.Buffer{}
	err := Run([]string{"--db", dir, "foobar"}, strings.NewReader(""), out)
	require.NoError(t, err)
	require.Contains(t, out.String(), "Unknown command: foobar")
}

func TestRunREPLMode(t *testing.T) {
	dir := t.TempDir()

	// simulate user typing two commands then EOF
	input := strings.NewReader("set k v\nget k\n")

	out := &bytes.Buffer{}
	err := Run([]string{"--db", dir}, input, out)
	require.NoError(t, err)

	s := out.String()
	require.Contains(t, s, "gocask service ready")
	require.Contains(t, s, "SET key=k value=v")
	require.Contains(t, s, "Value for key \"k\" is \"v\"")
}

func TestPrintUsage(t *testing.T) {
	out := &bytes.Buffer{}
	printUsage(out)
	require.Contains(t, out.String(), "Usage: gocask")
	require.Contains(t, out.String(), "set <key> <value>")
	require.Contains(t, out.String(), "get <key>")
}
