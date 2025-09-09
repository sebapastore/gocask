# Bitcask implementation in Go

This project is an implementation of the **Bitcask key-value store** in Go, inspired by [Coding Challenge #97](https://codingchallenges.substack.com/p/coding-challenge-97-bitcask) by John Crickett from [Subsatck Coding Challenges](https://codingchallenges.substack.com).

## What is Bitcask?

Bitcask is a high-performance, append-only key-value store originally developed by Justin Sheehy and David Smith (with inspiration from Eric Brewer). It uses log-structured data files and an in-memory key directory for fast reads and writes, making it simple yet powerful.

## Motivation

This is a learning project to deepen my understanding of storage engines, file I/O, and database internals while practicing Go.

## References

* [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf) (original paper)
* [Coding Challenges](https://codingchallenges.substack.com/) by John Crickett
