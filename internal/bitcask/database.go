package bitcask

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Database struct {
	data map[string]string
	path string
}

func NewDatabase(path string) (*Database, error) {
	// Ensure file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create database file: %w", err)
		}
		defer file.Close()
		fmt.Println("Database created at", path)
	}

	db := &Database{
		data: make(map[string]string),
		path: path,
	}

	// Load contents
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		db.data[parts[0]] = parts[1]
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read database: %w", err)
	}

	return db, nil
}

// Save overwrites the database file
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

func (db *Database) Get(key string) (string, bool) {
	val, exists := db.data[key]
	return val, exists
}

func (db *Database) Set(key, value string) {
	db.data[key] = value
}
