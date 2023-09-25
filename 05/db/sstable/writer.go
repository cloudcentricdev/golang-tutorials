package sstable

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	"github.com/cloudcentricdev/golang-tutorials/05/db/memtable"
)

type syncCloser interface {
	io.Closer
	Sync() error
}

type Writer struct {
	file syncCloser
	bw   *bufio.Writer
	buf  []byte
}

func NewWriter(file io.Writer) *Writer {
	w := &Writer{}
	bw := bufio.NewWriter(file)
	w.file, w.bw = file.(syncCloser), bw
	w.buf = make([]byte, 0, 1024)

	return w
}

func (w *Writer) Process(m *memtable.Memtable) error {
	i := m.Iterator()
	for i.HasNext() {
		key, val := i.Next()
		err := w.writeDataBlock(key, val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) writeDataBlock(key, val []byte) error {
	keyLen, valLen := len(key), len(val)
	needed := 4 + keyLen + valLen
	buf := w.buf[:needed]
	binary.LittleEndian.PutUint16(buf[:], uint16(keyLen))
	binary.LittleEndian.PutUint16(buf[2:], uint16(valLen))
	copy(buf[4:], key)
	copy(buf[4+keyLen:], val)
	bytesWritten, err := w.bw.Write(buf)
	if err != nil {
		log.Fatal(err, bytesWritten)
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
