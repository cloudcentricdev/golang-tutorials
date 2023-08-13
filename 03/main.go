package main

import (
	"bufio"
	"os"

	"github.com/cloudcentricdev/golang-tutorials/03/cli"
	"github.com/cloudcentricdev/golang-tutorials/03/skiplist"
)

func main() {
	sl := skiplist.NewSkipList()
	scanner := bufio.NewScanner(os.Stdin)
	demo := cli.NewCLI(scanner, sl)
	demo.Start()
}
