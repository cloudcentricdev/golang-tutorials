package memtable

import (
	"github.com/cloudcentricdev/golang-tutorials/06/db/encoder"
	"github.com/cloudcentricdev/golang-tutorials/06/db/skiplist"
)

type Memtable struct {
	sl        *skiplist.SkipList
	sizeUsed  int // The approximate amount of space used by the Memtable so far (in bytes).
	sizeLimit int // The maximum allowed size of the Memtable (in bytes).
	encoder   *encoder.Encoder
}

func NewMemtable(sizeLimit int) *Memtable {
	m := &Memtable{
		sl:        skiplist.NewSkipList(),
		sizeLimit: sizeLimit,
		encoder:   encoder.NewEncoder(),
	}
	return m
}

func (m *Memtable) HasRoomForWrite(key, val []byte) bool {
	sizeNeeded := len(key) + len(val) + 1
	sizeAvailable := m.sizeLimit - m.sizeUsed

	if sizeNeeded > sizeAvailable {
		return false
	}
	return true
}

func (m *Memtable) Insert(key, val []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpKindSet, val))
	m.sizeUsed += len(key) + len(val) + 1
}

func (m *Memtable) InsertTombstone(key []byte) {
	m.sl.Insert(key, m.encoder.Encode(encoder.OpKindDelete, nil))
	m.sizeUsed += 1
}

func (m *Memtable) Get(key []byte) (*encoder.EncodedValue, error) {
	val, err := m.sl.Find(key)
	if err != nil {
		return nil, err
	}
	return m.encoder.Parse(val), nil
}

func (m *Memtable) Iterator() *skiplist.Iterator {
	return m.sl.Iterator()
}

func (m *Memtable) Size() int {
	return m.sizeUsed
}
