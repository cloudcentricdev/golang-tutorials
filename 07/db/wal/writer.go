package wal

import (
	"encoding/binary"
	"io"

	"github.com/cloudcentricdev/golang-tutorials/07/db/encoder"
)

const blockSize = 4 << 10 // 4 KiB

type block struct {
	buf    [blockSize]byte
	offset int
	len    int
}

type syncWriteCloser interface {
	io.WriteCloser
	Sync() error
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
	// determine the maximum length the WAL record could occupy
	keyLen, valLen := len(key), len(val)
	maxLen := 2*binary.MaxVarintLen64 + keyLen + valLen
	// determine where the WAL record should be positioned within the current block
	b := w.block
	start := b.offset
	end := start + maxLen
	// seal the block if it doesn't have enough space to accommodate the WAL record and start writing to a new block instead
	if end > blockSize {
		if err := w.sealBlock(); err != nil {
			return err
		}
		start = b.offset
		end = start + maxLen
	}
	// append WAL record to the current block and flush it to disk
	buf := b.buf[start:end]
	n := binary.PutUvarint(buf[:], uint64(keyLen))
	n += binary.PutUvarint(buf[n:], uint64(valLen))
	copy(buf[n:], key)
	copy(buf[n+keyLen:], val)
	dataLen := n + keyLen + valLen
	b.offset += dataLen
	if err := w.writeAndSync(buf[:dataLen]); err != nil {
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
