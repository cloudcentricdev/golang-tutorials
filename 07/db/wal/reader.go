package wal

import (
	"bytes"
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
	buf      *bytes.Buffer
}

func NewReader(logFile io.ReadCloser) *Reader {
	return &Reader{
		file:     logFile,
		blockNum: -1,
		block:    &block{},
		encoder:  encoder.NewEncoder(),
		buf:      &bytes.Buffer{},
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
	if b.len-b.offset <= headerSize {
		if err = r.loadNextBlock(); err != nil {
			return
		}
	}
	r.buf.Reset()

	for {
		start := b.offset
		dataLen := int(binary.LittleEndian.Uint16(b.buf[start : start+2]))
		chunkType := b.buf[start+2]
		r.buf.Write(b.buf[start+headerSize : start+headerSize+dataLen])
		b.offset += headerSize + dataLen

		if chunkType == chunkTypeFull || chunkType == chunkTypeLast {
			break
		}
		if err = r.loadNextBlock(); err != nil {
			return
		}

	}
	scratch := r.buf.Bytes()
	keyLen, n := binary.Uvarint(scratch[:])
	_, m := binary.Uvarint(scratch[n:])
	key = make([]byte, keyLen)
	copy(key, scratch[n+m:n+m+int(keyLen)])
	val = r.encoder.Parse(scratch[n+m+int(keyLen):])
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
