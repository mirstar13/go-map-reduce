package main

import (
	"fmt"
	"os"

	"github.com/mirstar13/go-map-reduce/cmd/cli/command"
)

func main() {
	if err := command.RootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
