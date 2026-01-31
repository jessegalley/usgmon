package main

import (
	"os"

	"github.com/jgalley/usgmon/internal/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		os.Exit(1)
	}
}
