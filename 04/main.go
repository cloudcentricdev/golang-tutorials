package main

import (
	"bufio"
	"os"

	"github.com/cloudcentricdev/golang-tutorials/04/btree"
	"github.com/cloudcentricdev/golang-tutorials/04/cli"
)

func main() {
	tree := btree.NewBTree()
	scanner := bufio.NewScanner(os.Stdin)
	demo := cli.NewCLI(scanner, tree)
	demo.Start()
}
