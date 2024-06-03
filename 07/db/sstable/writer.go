package sstable

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/cloudcentricdev/golang-tutorials/06/db/encoder"
	"github.com/cloudcentricdev/golang-tutorials/06/db/memtable"
	"github.com/golang/snappy"
)

const (
	dataBlockChunkSize  = 16
	indexBlockChunkSize = 1
)

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file syncCloser
	bw   *bufio.Writer
	buf  []byte

	dataBlock  *blockWriter
	indexBlock *blockWriter
	encoder    *encoder.Encoder

	offset       int    // offset of current data block.
	bytesWritten int    // bytesWritten to current data block.
	lastKey      []byte // lastKey in current data block

	compressionBuf []byte
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = make([]byte, 0, 1024)
	w.dataBlock, w.indexBlock = newBlockWriter(dataBlockChunkSize), newBlockWriter(indexBlockChunkSize)

	return w
}

func (w *Writer) Process(m *memtable.Memtable) error {
	i := m.Iterator()
	for i.HasNext() {
		key, val := i.Next()
		n, err := w.dataBlock.add(key, val)
		if err != nil {
			return err
		}
		w.bytesWritten += n
		w.lastKey = key

		if w.bytesWritten > blockFlushThreshold {
			err = w.flushDataBlock()
			if err != nil {
				return err
			}
		}
	}
	err := w.flushDataBlock()
	if err != nil {
		return err
	}
	err = w.indexBlock.finish()
	if err != nil {
		return err
	}
	_, err = w.bw.ReadFrom(w.indexBlock.buf)
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) flushDataBlock() error {
	if w.bytesWritten <= 0 {
		return nil // nothing to flush
	}
	err := w.dataBlock.finish()
	if err != nil {
		return err
	}
	w.compressionBuf = snappy.Encode(w.compressionBuf, w.dataBlock.buf.Bytes())
	w.dataBlock.buf.Reset()
	_, err = w.bw.Write(w.compressionBuf)
	if err != nil {
		return err
	}
	err = w.addIndexEntry()
	if err != nil {
		return err
	}
	w.offset += len(w.compressionBuf)
	w.bytesWritten = 0
	return nil
}

func (w *Writer) addIndexEntry() error {
	buf := w.buf[:8]
	binary.LittleEndian.PutUint32(buf[:4], uint32(w.offset))              // data block offset
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(w.compressionBuf))) // data block length
	_, err := w.indexBlock.add(w.lastKey, w.encoder.Encode(encoder.OpKindSet, buf))
	if err != nil {
		return err
	}
	return nil
}

func (w *Writer) Close() error {
	// Flush any remaining data from the buffer.
	err := w.bw.Flush()
	if err != nil {
		return err
	}

	// Force OS to flush its I/O buffers and write data to disk.
	err = w.file.Sync()
	if err != nil {
		return err
	}

	// Close the file.
	err = w.file.Close()
	if err != nil {
		return err
	}

	w.bw = nil
	w.file = nil
	return err
}
