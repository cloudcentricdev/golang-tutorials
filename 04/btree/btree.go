package btree

import (
	"errors"
)

const (
	degree      = 5
	maxChildren = 2 * degree
	maxItems    = maxChildren - 1
	minItems    = degree - 1
)

type BTree struct {
	root *node
}

func NewBTree() *BTree {
	return &BTree{}
}

func (t *BTree) Find(key []byte) ([]byte, error) {
	for next := t.root; next != nil; {
		pos, found := next.search(key)

		if found {
			return next.items[pos].val, nil
		}

		next = next.children[pos]
	}

	return nil, errors.New("key not found")
}

func (t *BTree) Insert(key, val []byte) {
	i := &item{key, val}

	// The tree is empty, so initialize a new node.
	if t.root == nil {
		t.root = &node{}
	}

	// The tree root is full, so perform a split on the root.
	if t.root.numItems >= maxItems {
		t.splitRoot()
	}

	// Begin insertion.
	t.root.insert(i)
}

func (t *BTree) splitRoot() {
	newRoot := &node{}
	midItem, newNode := t.root.split()
	newRoot.insertItemAt(0, midItem)
	newRoot.insertChildAt(0, t.root)
	newRoot.insertChildAt(1, newNode)
	t.root = newRoot
}

func (t *BTree) Delete(key []byte) bool {
	if t.root == nil {
		return false
	}
	deletedItem := t.root.delete(key, false)

	if t.root.numItems == 0 {
		if t.root.isLeaf() {
			t.root = nil
		} else {
			t.root = t.root.children[0]
		}
	}

	if deletedItem != nil {
		return true
	}
	return false
}

func (t *BTree) String() string {
	v := &visualizer{t}
	return v.visualize()
}
