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
Usage: gocask [options] <command> [args]

Options:
  --db <path>     Path to the database (default "./database")
  -h, --help      Show this help message

Commands (single-command mode):
  set <key> <value>     Store a value
  get <key>             Retrieve a value
  del <key>             Delete a value

Interactive mode:
  Simply run 'gocask' without commands to enter interactive REPL.
  Type commands like 'set <key> <value>', 'get <key>' or 'del <key>'.
  Use 'exit' or Ctrl+D to quit.

Examples:
  gocask set name Sirius --db ./mydb
  gocask get name --db ./mydb
  gocask # enter interactive mode
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

- [x] Basic in-memory `Set` and `Get`
- [x] `Set` and `Get` with file persistence (append-only log)
- [x] REPL
- [x] Support for multiple data files
- [x] `Delete` functionality using tombstones
- [ ] Merge/compaction functionality to clean up deleted and overwritten keys
- [ ] Optional / future enhancements:
  - [ ] Hint file for faster keydir loading.
  - [ ] Configurable maximum file size.
  - [ ] Automatic file rotation.
  - [ ] Concurrency safety.

## References

* [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)
* [Coding Challenges](https://codingchallenges.substack.com/)
