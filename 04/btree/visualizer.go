package btree

import (
	"strings"

	"github.com/fatih/color"
)

const (
	LeftBranch   = "┌"
	RightBranch  = "└"
	Branch       = "├"
	Limb         = "──"
	Trunk        = "│    "
	LastPosition = -1
)

var (
	ColorEven = color.New(color.FgHiYellow)
	ColorOdd  = color.New(color.FgHiCyan)
)

type visualizer struct {
	t *BTree
}

func (v *visualizer) visualize() string {
	t := v.t
	return v.recurse(t.root, 0, LastPosition)
}

func (v *visualizer) recurse(n *node, level int, parentPos int) string {
	if n == nil {
		return ""
	}
	b := &strings.Builder{}
	c := ColorOdd

	if level%2 == 0 {
		c = ColorEven
	}

	var i int
	for i = 0; i < n.numItems; i++ {
		b.WriteString(v.recurse(n.children[i], level+1, i))
		for l, c := 0, ColorOdd; l < level; l++ {
			if l%2 == 0 {
				c = ColorEven
			}
			b.WriteString(c.Sprint(Trunk))
		}
		key := n.items[i].key
		branch := Branch
		if i == 0 && parentPos == 0 {
			branch = LeftBranch
		} else if i == n.numItems-1 && parentPos == LastPosition {
			branch = RightBranch
		}
		b.WriteString(c.Sprint(branch + Limb + " "))
		b.WriteString(c.Sprint(string(key)))
		b.WriteString(c.Sprint("\n"))
	}

	b.WriteString(v.recurse(n.children[i], level+1, LastPosition))

	return b.String()
}
