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
	// load the very first WAL block into memory if necessary
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
	// check if last record in block reached (when last block in WAL is properly sealed)
	if b.len-b.offset <= headerSize {
		if err = r.loadNextBlock(); err != nil {
			return
		}
	}
	// start with a clean scratch buffer
	r.buf.Reset()
	// recover all chunks to form the full payload
	for {
		start := b.offset
		// extract data from chunk header (payload length and chunk type)
		dataLen := int(binary.LittleEndian.Uint16(b.buf[start : start+2]))
		chunkType := b.buf[start+2]
		// copy recovered payload to scratch buffer
		r.buf.Write(b.buf[start+headerSize : start+headerSize+dataLen])
		// advance the data block offset
		b.offset += headerSize + dataLen
		// check if there are no chunks left to process for this record
		if chunkType == chunkTypeFull || chunkType == chunkTypeLast {
			break
		}
		// load next block to retrieve the subsequent chunk
		if err = r.loadNextBlock(); err != nil {
			return
		}
	}
	// retrieve scratch buffer contents (i.e., the payload)
	scratch := r.buf.Bytes()
	// parse the WAL record
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
