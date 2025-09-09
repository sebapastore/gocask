# Bitcask implementation in Go

This project is an implementation of the **Bitcask key-value store** in Go, inspired by [Coding Challenge #97](https://codingchallenges.substack.com/p/coding-challenge-97-bitcask) by John Crickett from [Subsatck Coding Challenges](https://codingchallenges.substack.com).

## What is Bitcask?

Bitcask is a high-performance, append-only key-value store originally developed by Justin Sheehy and David Smith (with inspiration from Eric Brewer). It uses log-structured data files and an in-memory key directory for fast reads and writes, making it simple yet powerful.

## Motivation

This is a learning project to deepen my understanding of storage engines, file I/O, and database internals while practicing Go.

## Setup

### Git Hooks

To ensure code quality, there are Git hooks for linting and testing before pushing.

Run this command once to enable the custom hooks:

```
git config core.hooksPath .githooks
```

## Development

### Running the CLI

You can interact with the Bitcask database using `go run`:

```bash
Usage: go run cmd/gocask/main.go [options] <command> [args]

Options:
  --db <path>     Path to the database (default "./database")

Commands:
  set <key> <value>     Store a value
  get <key>             Retrieve a value

Examples:
  go run cmd/gocask/main.go set name Seba --db ./mydb
  go run cmd/gocask/main.go get name --db ./mydb
```

### Run the test suite:

```bash
go test ./... -v
```

### Run the linter:

```bash
golangci-lint run
```

This project also has a GitHub Actions CI workflow that runs tests and lint checks on every push and pull request to `main`.

## Todo / Next Steps

- Review and refactor the code if needed before continuing.
- Support multiple database files with a maximum file size (configurable?).
- Implement entry deletions.
- Implement the merge functionality of Bitcask.
- Prevent data loss during merge.
- Consider building a network server and support RESP protocol. 

## References

* [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
* [Coding Challenges](https://codingchallenges.substack.com/)
