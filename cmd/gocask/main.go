package main

import (
	"fmt"
	"os"

	"github.com/sebapastore/gocask/internal/bitcask"
)

func main() {
	if err := bitcask.Run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}
