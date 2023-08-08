package main

import (
	"unsafe"
)

func main() {
	alignmentSize := 512 // bytes
	bitmask := alignmentSize - 1

	blockSize := 16 << 10 // 16 KiB
	block := make([]byte, blockSize+alignmentSize)

	alignment := int(uintptr(unsafe.Pointer(&block[0])) & uintptr(bitmask))
	offset := 0

	if alignment != 0 {
		offset = alignmentSize - alignment
	}

	block = block[offset : offset+blockSize : offset+blockSize]
}
