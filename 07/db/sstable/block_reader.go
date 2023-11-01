package sstable

import (
	"bytes"
	"encoding/binary"
)

type searchCondition int

const (
	moveUpWhenKeyGTE searchCondition = iota
	moveUpWhenKeyGT
)

type blockReader struct {
	buf        []byte
	offsets    []byte
	numOffsets int
}

func (b *blockReader) readOffsetAt(pos int) int {
	offset, _, _ := b.fetchDataFor(pos)
	return offset
}

func (b *blockReader) readKeyAt(pos int) []byte {
	_, key, _ := b.fetchDataFor(pos)
	return key
}

func (b *blockReader) readValAt(pos int) []byte {
	_, _, val := b.fetchDataFor(pos)
	return val
}

func (b *blockReader) fetchDataFor(pos int) (kvOffset int, key, val []byte) {
	var keyLen, valLen uint64
	var n int
	kvOffset = int(binary.LittleEndian.Uint32(b.offsets[pos*offsetSizeInBytes : pos*offsetSizeInBytes+offsetSizeInBytes]))
	offset := kvOffset
	_, n = binary.Uvarint(b.buf[offset:]) // sharedLen = 0
	offset += n
	keyLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	valLen, n = binary.Uvarint(b.buf[offset:])
	offset += n
	key = b.buf[offset : offset+int(keyLen)]
	offset += int(keyLen)
	val = b.buf[offset : offset+int(valLen)]
	return
}

func (b *blockReader) search(searchKey []byte, condition searchCondition) int {
	low, high := 0, b.numOffsets
	var mid int
	for low < high {
		mid = (low + high) / 2
		key := b.readKeyAt(mid)
		cmp := bytes.Compare(searchKey, key)
		if cmp >= int(condition) {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return low
}
