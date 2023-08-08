package main

import (
	"errors"
	"github.com/go-faker/faker/v4"
)

const (
	// alignment = 4 /* bytes */; bitmask = alignment - 1
	bitmask = 3
)

type Arena struct {
	buffer []byte
	offset int // next offset open for insertion
}

func (a *Arena) Alloc(dataSize int) (int, error) {
	currOffset := a.offset
	nextOffset := (currOffset + dataSize + bitmask) &^ bitmask

	if nextOffset >= len(a.buffer) {
		return currOffset, errors.New("not enough memory")
	}
	a.offset = nextOffset

	return currOffset, nil
}

func main() {
	bufferSize := 1 << 10 // 1 KiB
	buffer := make([]byte, bufferSize)

	arena := &Arena{buffer, 0}

	for {
		// insert data until we run out of memory
		randomData := []byte(faker.Word())
		dataSize := len(randomData)
		offset, err := arena.Alloc(dataSize)

		if err != nil {
			return // out of memory, stop inserting data
		}
		copy(buffer[offset:offset+dataSize:offset+dataSize], randomData)
	}
}
