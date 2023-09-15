package skiplist

import (
	"bytes"
	"errors"
	"math"

	"github.com/cloudcentricdev/golang-tutorials/03/fastrand"
)

const (
	MaxHeight = 16
	PValue    = 0.5 // p = 1/2
)

var ErrKeyNotFound = errors.New("key not found")

var probabilities [MaxHeight]uint32

type node struct {
	key   []byte
	val   []byte
	tower [MaxHeight]*node
}

type SkipList struct {
	head   *node
	height int
}

func NewSkipList() *SkipList {
	sl := &SkipList{}
	sl.head = &node{}
	sl.height = 1
	return sl
}

func init() {
	probability := 1.0

	for level := 0; level < MaxHeight; level++ {
		probabilities[level] = uint32(probability * float64(math.MaxUint32))
		probability *= PValue
	}
}

func randomHeight() int {
	seed := fastrand.Uint32()

	height := 1
	for height < MaxHeight && seed <= probabilities[height] {
		height++
	}

	return height
}

func (sl *SkipList) search(key []byte) (*node, [MaxHeight]*node) {
	var next *node
	var journey [MaxHeight]*node

	prev := sl.head
	for level := sl.height - 1; level >= 0; level-- {
		for next = prev.tower[level]; next != nil; next = prev.tower[level] {
			if bytes.Compare(key, next.key) <= 0 {
				break
			}
			prev = next
		}
		journey[level] = prev
	}

	if next != nil && bytes.Equal(key, next.key) {
		return next, journey
	}
	return nil, journey
}

func (sl *SkipList) Find(key []byte) ([]byte, error) {
	found, _ := sl.search(key)

	if found == nil {
		return nil, ErrKeyNotFound
	}

	return found.val, nil
}

func (sl *SkipList) Insert(key []byte, val []byte) {
	found, journey := sl.search(key)

	if found != nil {
		// update value of existing key
		found.val = val
		return
	}
	height := randomHeight()
	nd := &node{key: key, val: val}

	for level := 0; level < height; level++ {
		prev := journey[level]

		if prev == nil {
			// prev is nil if we are extending the height of the tree,
			// because that level did not exist while the journey was being recorded
			prev = sl.head
		}
		nd.tower[level] = prev.tower[level]
		prev.tower[level] = nd
	}

	if height > sl.height {
		sl.height = height
	}
}

func (sl *SkipList) Delete(key []byte) bool {
	found, journey := sl.search(key)

	if found == nil {
		return false
	}

	for level := 0; level < sl.height; level++ {
		if journey[level].tower[level] != found {
			break
		}
		journey[level].tower[level] = found.tower[level]
		found.tower[level] = nil
	}
	found = nil
	sl.shrink()

	return true
}

func (sl *SkipList) shrink() {
	for level := sl.height - 1; level >= 0; level-- {
		if sl.head.tower[level] == nil {
			sl.height--
		}
	}
}
