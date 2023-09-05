package btree

import "bytes"

type item struct {
	key []byte
	val []byte
}

type node struct {
	items       [maxItems]*item
	children    [maxChildren]*node
	numItems    int
	numChildren int
}

func (n *node) isLeaf() bool {
	return n.numChildren == 0
}

func (n *node) search(key []byte) (int, bool) {
	low, high := 0, n.numItems
	var mid int
	for low < high {
		mid = (low + high) / 2
		cmp := bytes.Compare(key, n.items[mid].key)
		switch {
		case cmp > 0:
			low = mid + 1
		case cmp < 0:
			high = mid
		case cmp == 0:
			return mid, true
		}
	}
	return low, false
}

func (n *node) insertItemAt(pos int, i *item) {
	if pos < n.numItems {
		// Make space for insertion if we are not appending to the very end of the items array.
		copy(n.items[pos+1:n.numItems+1], n.items[pos:n.numItems])
	}
	n.items[pos] = i
	n.numItems++
}

func (n *node) insertChildAt(pos int, c *node) {
	if pos < n.numChildren {
		// Make space for insertion if we are not appending to the very end of the children array.
		copy(n.children[pos+1:n.numChildren+1], n.children[pos:n.numChildren])
	}
	n.children[pos] = c
	n.numChildren++
}

func (n *node) split() (*item, *node) {
	// Retrieve the middle item.
	mid := minItems
	midItem := n.items[mid]

	// Create a new node and copy half of the items from the current node to the new node.
	newNode := &node{}
	copy(newNode.items[:], n.items[mid+1:])
	newNode.numItems = minItems

	// If necessary, copy half of the child pointers from the current node to the new node.
	if !n.isLeaf() {
		copy(newNode.children[:], n.children[mid+1:])
		newNode.numChildren = minItems + 1
	}

	// Remove data items and child pointers from the current node that were moved to the new node.
	for i, l := mid, n.numItems; i < l; i++ {
		n.items[i] = nil
		n.numItems--

		if !n.isLeaf() {
			n.children[i+1] = nil
			n.numChildren--
		}
	}

	// Return the middle item and the newly created node, so we can link them to the parent.
	return midItem, newNode
}

func (n *node) insert(item *item) bool {
	pos, found := n.search(item.key)

	// The data item already exists, so just update its value.
	if found {
		n.items[pos] = item
		return false
	}

	// We have reached a leaf node with sufficient capacity to accommodate insertion, so insert the new data item.
	if n.isLeaf() {
		n.insertItemAt(pos, item)
		return true
	}

	// If the next node along the path of traversal is already full, split it.
	if n.children[pos].numItems >= maxItems {
		midItem, newNode := n.children[pos].split()
		n.insertItemAt(pos, midItem)
		n.insertChildAt(pos+1, newNode)
		// We may need to change our direction after promoting the middle item to the parent, depending on its key.
		switch cmp := bytes.Compare(item.key, n.items[pos].key); {
		case cmp < 0:
			// The key we are looking for is still smaller than the key of the middle item that we took from the child,
			// so we can continue following the same direction.
		case cmp > 0:
			// The middle item that we took from the child has a key that is smaller than the one we are looking for,
			// so we need to change our direction.
			pos++
		default:
			// The middle item that we took from the child is the item we are searching for, so just update its value.
			n.items[pos] = item
			return true
		}

	}

	return n.children[pos].insert(item)
}

func (n *node) removeItemAt(pos int) *item {
	removedItem := n.items[pos]
	n.items[pos] = nil
	// Fill the gap, if the position we are removing from is not the very last occupied position in the "items" array.
	if lastPos := n.numItems - 1; pos < lastPos {
		copy(n.items[pos:lastPos], n.items[pos+1:lastPos+1])
		n.items[lastPos] = nil
	}
	n.numItems--

	return removedItem
}

func (n *node) removeChildAt(pos int) *node {
	removedChild := n.children[pos]
	n.children[pos] = nil
	// Fill the gap, if the position we are removing from is not the very last occupied position in the "children" array.
	if lastPos := n.numChildren - 1; pos < lastPos {
		copy(n.children[pos:lastPos], n.children[pos+1:lastPos+1])
		n.children[lastPos] = nil
	}
	n.numChildren--

	return removedChild
}

func (n *node) fillChildAt(pos int) {
	switch {
	// Borrow the right-most item from the left sibling if the left
	// sibling exists and has more than the minimum number of items.
	case pos > 0 && n.children[pos-1].numItems > minItems:
		// Establish our left and right nodes.
		left, right := n.children[pos-1], n.children[pos]
		// Take the item from the parent and place it at the left-most position of the right node.
		copy(right.items[1:right.numItems+1], right.items[:right.numItems])
		right.items[0] = n.items[pos-1]
		right.numItems++
		// For non-leaf nodes, make the right-most child of the left node the new left-most child of the right node.
		if !right.isLeaf() {
			right.insertChildAt(0, left.removeChildAt(left.numChildren-1))
		}
		// Borrow the right-most item from the left node to replace the parent item.
		n.items[pos-1] = left.removeItemAt(left.numItems - 1)
	// Borrow the left-most item from the right sibling if the right
	// sibling exists and has more than the minimum number of items.
	case pos < n.numChildren-1 && n.children[pos+1].numItems > minItems:
		// Establish our left and right nodes.
		left, right := n.children[pos], n.children[pos+1]
		// Take the item from the parent and place it at the right-most position of the left node.
		left.items[left.numItems] = n.items[pos]
		left.numItems++
		// For non-leaf nodes, make the left-most child of the right node the new right-most child of the left node.
		if !left.isLeaf() {
			left.insertChildAt(left.numChildren, right.removeChildAt(0))
		}
		// Borrow the left-most item from the right node to replace the parent item.
		n.items[pos] = right.removeItemAt(0)
	// There are no suitable nodes to borrow items from, so perform a merge.
	default:
		// If we are at the right-most child pointer, merge the node with its left sibling.
		// In all other cases, we prefer to merge the node with its right sibling for simplicity.
		if pos >= n.numItems {
			pos = n.numItems - 1
		}
		// Establish our left and right nodes.
		left, right := n.children[pos], n.children[pos+1]
		// Borrow an item from the parent node and place it at the right-most available position of the left node.
		left.items[left.numItems] = n.removeItemAt(pos)
		left.numItems++
		// Migrate all items from the right node to the left node.
		copy(left.items[left.numItems:], right.items[:right.numItems])
		left.numItems += right.numItems
		// For non-leaf nodes, migrate all applicable children from the right node to the left node.
		if !left.isLeaf() {
			copy(left.children[left.numChildren:], right.children[:right.numChildren])
			left.numChildren += right.numChildren
		}
		// Remove the child pointer from the parent to the right node and discard the right node.
		n.removeChildAt(pos + 1)
		right = nil
	}
}

func (n *node) delete(key []byte, isSeekingSuccessor bool) *item {
	pos, found := n.search(key)

	var next *node

	// We have found a node holding an item matching the supplied key.
	if found {
		// This is a leaf node, so we can simply remove the item.
		if n.isLeaf() {
			return n.removeItemAt(pos)
		}
		// This is not a leaf node, so we have to find the inorder successor.
		next, isSeekingSuccessor = n.children[pos+1], true
	} else {
		next = n.children[pos]
	}

	// We have reached the leaf node containing the inorder successor, so remove the successor from the leaf.
	if n.isLeaf() && isSeekingSuccessor {
		return n.removeItemAt(0)
	}

	// We were unable to find an item matching the given key. Don't do anything.
	if next == nil {
		return nil
	}

	// Continue traversing the tree to find an item matching the supplied key.
	deletedItem := next.delete(key, isSeekingSuccessor)

	// We found the inorder successor, and we are now back at the internal node containing the item
	// matching the supplied key. Therefore, we replace the item with its inorder successor, effectively
	// deleting the item from the tree.
	if found && isSeekingSuccessor {
		n.items[pos] = deletedItem
	}

	// Check if an underflow occurred after we deleted an item down the tree.
	if next.numItems < minItems {
		// Repair the underflow.
		if found && isSeekingSuccessor {
			n.fillChildAt(pos + 1)
		} else {
			n.fillChildAt(pos)
		}
	}

	// Propagate the deleted item back to the previous stack frame.
	return deletedItem
}
