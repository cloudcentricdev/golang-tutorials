package wal

import (
	"encoding/binary"
	"io"

	"github.com/cloudcentricdev/golang-tutorials/07/db/encoder"
)

const blockSize = 4 << 10

type syncWriteCloser interface {
	io.WriteCloser
	Sync() error
}

type block struct {
	buf    [blockSize]byte
	offset int
	len    int
}

type Writer struct {
	block   *block
	file    syncWriteCloser
	encoder *encoder.Encoder
}

func NewWriter(logFile syncWriteCloser) *Writer {
	w := &Writer{
		block:   &block{},
		file:    logFile,
		encoder: encoder.NewEncoder(),
	}
	return w
}

func (w *Writer) RecordInsertion(key, val []byte) error {
	val = w.encoder.Encode(encoder.OpKindSet, val)
	return w.record(key, val)
}

func (w *Writer) RecordDeletion(key []byte) error {
	val := w.encoder.Encode(encoder.OpKindDelete, nil)
	return w.record(key, val)
}

func (w *Writer) record(key, val []byte) error {
	// determine the maximum data length
	keyLen, valLen := len(key), len(val)
	maxLen := 2 + binary.MaxVarintLen64 + keyLen + valLen
	// determine where data should be positioned within the current block
	b := w.block
	start := b.offset
	end := start + maxLen
	// seal the block if it doesn't have enough space to accommodate the data and start writing to a new one instead
	if end > blockSize {
		if err := w.sealBlock(); err != nil {
			return err
		}
		start = b.offset
		end = start + maxLen
	}
	// append data to the current block and flush it to disk
	buf := b.buf[start:end]
	n := binary.PutUvarint(buf[2:], uint64(keyLen))
	copy(buf[2+n:], key)
	copy(buf[2+n+keyLen:], val)
	dataLen := n + keyLen + valLen
	binary.LittleEndian.PutUint16(buf[:2], uint16(dataLen))
	b.offset += dataLen + 2
	if err := w.writeAndSync(buf[:dataLen+2]); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() (err error) {
	if err = w.sealBlock(); err != nil {
		return err
	}
	err = w.file.Close()
	w.file = nil
	if err != nil {
		return err
	}
	return nil
}

// sealBlock applies zero padding to the current block and calls writeAndSync to persists it to stable storage
func (w *Writer) sealBlock() error {
	b := w.block
	clear(b.buf[b.offset:])
	if err := w.writeAndSync(b.buf[b.offset:]); err != nil {
		return err
	}
	b.offset = 0
	return nil
}

// writeAndSync writes to the underlying WAL file and forces a sync of its contents to stable storage
func (w *Writer) writeAndSync(p []byte) (err error) {
	if _, err = w.file.Write(p); err != nil {
		return err
	}
	if err = w.file.Sync(); err != nil {
		return err
	}
	return nil
}