package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"
)

type Database struct {
	data map[string]string
	path string
}

func main() {
	var dbPath string
	flag.StringVar(&dbPath, "db", "./database", "Database path")

	flag.Usage = printUsage

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		return
	}

	command := args[0]

	if err := initDatabaseFile(dbPath); err != nil {
		fmt.Println("Error initializing database file:", err)
		os.Exit(1)
	}

	db, err := loadDatabaseFromFile(dbPath)
	if err != nil {
		fmt.Println("Error loading database:", err)
		os.Exit(1)
	}

	switch command {
	case "set":
		if len(args) < 3 {
			fmt.Println("Usage: gocask set <key> <value> [--db <database-path>]")
			return
		}
		key := args[1]
		value := args[2]
		db.data[key] = value
		if err := db.Save(); err != nil {
			fmt.Printf("Error while saving the database: %s", err)
			os.Exit(1)
		}
		fmt.Printf("SET key=%s value=%s\n", key, value)

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: gocask get <key> [--db <database-path>]")
			return
		}
		key := args[1]
		value := db.data[key]
		if value == "" {
			fmt.Printf("There is no value for key %s\n", key)
			os.Exit(1)
		}
		fmt.Printf("The value for key %s is %s\n", key, value)

	default:
		fmt.Println("Unknown command:", command)
	}
}

func printUsage() {
	fmt.Println(`
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

func initDatabaseFile(dbPath string) error {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		file, err := os.Create(dbPath)
		if err != nil {
			return fmt.Errorf("failed to create database file: %w", err)
		}
		defer file.Close()
		fmt.Println("Database created at", dbPath)
	}
	return nil
}

func loadDatabaseFromFile(path string) (*Database, error) {
	db := &Database{
		data: make(map[string]string),
		path: path,
	}

	// Open the file
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer file.Close()

	// Read line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		// simple format: key=value
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := parts[0]
		value := parts[1]
		db.data[key] = value
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read database: %w", err)
	}

	return db, nil
}

func (db *Database) Save() error {
	file, err := os.Create(db.path)
	if err != nil {
		return fmt.Errorf("failed to save database: %w", err)
	}
	defer file.Close()

	for k, v := range db.data {
		_, err := fmt.Fprintf(file, "%s:%s\n", k, v)
		if err != nil {
			return fmt.Errorf("failed to write database: %w", err)
		}
	}

	return nil
}
