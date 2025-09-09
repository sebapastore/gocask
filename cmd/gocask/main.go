package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/sebapastore/gocask/internal/bitcask"
)

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

	db, err := bitcask.NewDatabase(dbPath)
	if err != nil {
		fmt.Println(err)
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
		db.Set(key, value)
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
		value, exists := db.Get(key)
		if !exists {
			fmt.Printf("There is no value for key %s\n", key)
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
