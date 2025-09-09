package bitcask

import (
	"flag"
	"fmt"
	"io"
)

func Run(args []string, output io.Writer) error {
	var dbPath string
	flags := flag.NewFlagSet("gocask", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&dbPath, "db", "./database", "Database path")

	if err := flags.Parse(args); err != nil {
		return err
	}

	remaining := flags.Args()
	if len(remaining) < 1 {
		printUsage(output)
		return nil
	}

	command := remaining[0]

	db, err := NewDatabase(dbPath)
	if err != nil {
		return err
	}

	switch command {
	case "set":
		if len(remaining) < 3 {
			_, _ = fmt.Fprintln(output, "Usage: gocask set <key> <value> [--db <database-path>]")
			return nil
		}
		key := remaining[1]
		value := remaining[2]
		if err := db.Set(key, value); err != nil {
			return fmt.Errorf("failed to set value: %w", err)
		}
		_, _ = fmt.Fprintf(output, "SET key=%s value=%s\n", key, value)

	case "get":
		if len(remaining) < 2 {
			_, _ = fmt.Fprintln(output, "Usage: gocask get <key> [--db <database-path>]")
			return nil
		}
		key := remaining[1]
		value, exists, err := db.Get(key)
		if err != nil {
			return fmt.Errorf("failed to get value: %w", err)
		}

		if !exists {
			_, _ = fmt.Fprintf(output, "There is no value for key %s\n", key)
			return nil
		}
		_, _ = fmt.Fprintf(output, "The value for key %s is %s\n", key, value)

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

  Commands:
  set <key> <value>     Store a value
  get <key>             Retrieve a value

  Examples:
  gocask set name Sebastian --db ./mydb
  gocask get name --db ./mydb
	`)
}
