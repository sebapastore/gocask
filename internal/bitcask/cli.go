package bitcask

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

func Run(args []string, input io.Reader, output io.Writer) error {
	var dbPath string
	flags := flag.NewFlagSet("gocask", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&dbPath, "db", "./database", "Database path")

	// Parse flags first
	if err := flags.Parse(args); err != nil {
		return err
	}

	flags.Usage = func() { printUsage(output) }

	// Open DB
	db := NewDatabase(dbPath)
	if err := db.Open(); err != nil {
		return err
	}

	// Remaining args after flags
	remaining := flags.Args()

	if len(remaining) > 0 {
		// Run a single command and exit
		return runCommand(db, remaining, output)
	}

	// No command: start interactive REPL
	scanner := bufio.NewScanner(input)
	_, _ = fmt.Fprintln(output, "gocask service ready. Enter commands:")

	for {
		_, _ = fmt.Fprint(output, "> ")
		if !scanner.Scan() {
			break // EOF or error
		}

		line := scanner.Text()
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		if err := runCommand(db, parts, output); err != nil {
			_, _ = fmt.Fprintf(output, "Error: %v\n", err)
		}
	}

	return scanner.Err()
}

func runCommand(db *Database, args []string, output io.Writer) error {
	command := args[0]

	switch command {
	case "set":
		if len(args) < 3 {
			_, _ = fmt.Fprintln(output, "Usage: set <key> <value>")
			return nil
		}
		key, value := args[1], args[2]
		if err := db.Set(key, value); err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}
		_, _ = fmt.Fprintf(output, "SET key=%s value=%s\n", key, value)

	case "get":
		if len(args) < 2 {
			_, _ = fmt.Fprintln(output, "Usage: get <key>")
			return nil
		}
		key := args[1]
		value, exists, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}
		if !exists {
			_, _ = fmt.Fprintf(output, "No value for key %s\n", key)
			return nil
		}
		_, _ = fmt.Fprintf(output, "Value for key %s is %s\n", key, value)

	case "exit", "quit":
		os.Exit(0) // optional: allow exiting the REPL

	default:
		_, _ = fmt.Fprintf(output, "Unknown command: %s\n", command)
	}

	return nil
}

func printUsage(output io.Writer) {
	_, _ = fmt.Fprintln(output, `
Usage: gocask [options] <command> [args]

Options:
  --db <path>     Path to the database (default "./database")
  -h, --help      Show this help message

Commands (single-command mode):
  set <key> <value>     Store a value
  get <key>             Retrieve a value

Interactive mode:
  Simply run 'gocask' without commands to enter interactive REPL.
  Type commands like 'set <key> <value>' or 'get <key>'.
  Use 'exit' or Ctrl+D to quit.

Examples:
  gocask set name Sirius --db ./mydb
  gocask get name --db ./mydb
  gocask # enter interactive mode
    `)
}
