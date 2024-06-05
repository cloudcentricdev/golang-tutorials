package wal

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/cloudcentricdev/golang-tutorials/07/db/encoder"
)

type Reader struct {
	file     io.Reader
	blockNum int
	block    *block
	encoder  *encoder.Encoder
}

func NewReader(logFile io.ReadCloser) *Reader {
	return &Reader{
		file:     logFile,
		blockNum: -1,
		block:    &block{},
		encoder:  encoder.NewEncoder(),
	}
}

func (r *Reader) Next() (key []byte, val *encoder.EncodedValue, err error) {
	b := r.block
	// load first WAL block into memory
	if r.blockNum == -1 {
		if err = r.loadNextBlock(); err != nil {
			return
		}
	}
	// check if EOF reached (when last block in WAL is not properly sealed)
	if b.offset >= b.len {
		err = io.EOF
		return
	}
	start := b.offset
	keyLen, n := binary.Uvarint(b.buf[start:])
	// check if last record reached (when last block in WAL is properly sealed)
	if keyLen == 0 {
		if err = r.loadNextBlock(); err != nil {
			return
		}
		start = b.offset
		keyLen, n = binary.Uvarint(b.buf[start:])
	}
	// read next record in WAL block
	valLen, m := binary.Uvarint(b.buf[start+n:])
	dataLen := int(keyLen) + int(valLen) + n + m
	buf := b.buf[start : start+dataLen]
	b.offset += dataLen
	key = make([]byte, keyLen)
	copy(key, buf[n+m:n+m+int(keyLen)])
	val = r.encoder.Parse(buf[n+m+int(keyLen):])
	return
}

func (r *Reader) loadNextBlock() (err error) {
	b := r.block
	b.len, err = io.ReadFull(r.file, b.buf[:])
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	b.offset = 0
	r.blockNum++

	return nil
}
