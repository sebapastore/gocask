package main

import (
	"flag"
	"fmt"
)

func main() {
	var db string
	flag.StringVar(&db, "db", "./database", "Database path")

	flag.Usage = printUsage

	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		return
	}

	command := args[0]

	switch command {
	case "set":
		if len(args) < 3 {
			fmt.Println("Usage: gocask set <key> <value> [--db <database-path>]")
			return
		}
		key := args[1]
		value := args[2]
		fmt.Printf("SET key=%s value=%s\n", key, value)

	case "get":
		if len(args) < 2 {
			fmt.Println("Usage: gocask get <key> [--db <database-path>]")
			return
		}
		key := args[1]
		fmt.Printf("GET key=%s\n", key)

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
