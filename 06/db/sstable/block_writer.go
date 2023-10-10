package sstable

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	maxBlockSize      = 4096
	offsetSizeInBytes = 4
)

var blockFlushThreshold = int(math.Floor(maxBlockSize * 0.9))

type blockWriter struct {
	buf *bytes.Buffer

	offsets    []uint32
	currOffset uint32 // starting offset of the current data chunk
	nextOffset uint32

	chunkSize  int    // desired numEntries in each data chunk
	numEntries int    // numEntries in the current data chunk
	prefixKey  []byte // prefixKey of the current data chunk
}

func newBlockWriter(chunkSize int) *blockWriter {
	bw := &blockWriter{}
	bw.buf = bytes.NewBuffer(make([]byte, 0, maxBlockSize))
	bw.chunkSize = chunkSize
	return bw
}

func (b *blockWriter) scratchBuf(needed int) []byte {
	available := b.buf.Available()
	if needed > available {
		b.buf.Grow(needed)
	}
	buf := b.buf.AvailableBuffer()
	return buf[:needed]
}

func (b *blockWriter) add(key, val []byte) (int, error) {
	sharedLen := b.calculateSharedLength(key)
	keyLen, valLen := len(key), len(val)
	needed := 3*binary.MaxVarintLen64 + (keyLen - sharedLen) + valLen
	buf := b.scratchBuf(needed)
	n := binary.PutUvarint(buf, uint64(sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(keyLen-sharedLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key[sharedLen:])
	copy(buf[n+keyLen-sharedLen:], val)
	used := n + (keyLen - sharedLen) + valLen
	n, err := b.buf.Write(buf[:used])
	if err != nil {
		return n, err
	}
	b.numEntries++
	b.trackOffset(uint32(n))
	return n, nil
}

func (b *blockWriter) calculateSharedLength(key []byte) int {
	sharedLen := 0
	if b.prefixKey == nil {
		b.prefixKey = key
		return sharedLen
	}

	for i := 0; i < min(len(key), len(b.prefixKey)); i++ {
		if key[i] != b.prefixKey[i] {
			break
		}
		sharedLen++
	}

	return sharedLen
}

func (b *blockWriter) trackOffset(n uint32) {
	b.nextOffset += n
	if b.numEntries == b.chunkSize {
		b.offsets = append(b.offsets, b.currOffset)
		b.currOffset = b.nextOffset
		b.numEntries = 0
		b.prefixKey = nil
	}
}

func (b *blockWriter) reset() {
	b.nextOffset = 0
	b.currOffset = 0
	b.offsets = b.offsets[:0]
	b.numEntries = 0
	b.prefixKey = nil
}

func (b *blockWriter) finish() error {
	if b.prefixKey != nil {
		// Force flush of last prefix key offset.
		b.offsets = append(b.offsets, b.currOffset)
	}
	numOffsets := len(b.offsets)
	needed := (numOffsets + 2) * offsetSizeInBytes
	buf := b.scratchBuf(needed)
	for i, offset := range b.offsets {
		binary.LittleEndian.PutUint32(buf[i*offsetSizeInBytes:i*offsetSizeInBytes+offsetSizeInBytes], offset)
	}
	binary.LittleEndian.PutUint32(buf[needed-footerSizeInBytes:needed-offsetSizeInBytes], uint32(b.buf.Len()+needed))
	binary.LittleEndian.PutUint32(buf[needed-offsetSizeInBytes:needed], uint32(numOffsets))
	_, err := b.buf.Write(buf)
	if err != nil {
		return err
	}
	b.reset()
	return nil
}
