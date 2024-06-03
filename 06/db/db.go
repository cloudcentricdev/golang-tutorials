package db

import (
	"errors"
	"log"

	"github.com/cloudcentricdev/golang-tutorials/06/db/encoder"
	"github.com/cloudcentricdev/golang-tutorials/06/db/memtable"
	"github.com/cloudcentricdev/golang-tutorials/06/db/sstable"
	"github.com/cloudcentricdev/golang-tutorials/06/db/storage"
)

const (
	memtableSizeLimit      = 4 << 10 // 4 KiB
	memtableFlushThreshold = 8 << 10 // 8 KiB
)

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *memtable.Memtable
		queue   []*memtable.Memtable
	}
	sstables []*storage.FileMetadata
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}
	db := &DB{dataStorage: dataStorage}

	err = db.loadSSTables()
	if err != nil {
		return nil, err
	}
	db.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	db.memtables.queue = append(db.memtables.queue, db.memtables.mutable)
	return db, nil
}

func (d *DB) loadSSTables() error {
	meta, err := d.dataStorage.ListFiles()
	if err != nil {
		return err
	}
	for _, f := range meta {
		if !f.IsSSTable() {
			continue
		}
		d.sstables = append(d.sstables, f)
	}
	return nil
}

func (d *DB) Set(key, val []byte) {
	m := d.prepMemtableForKV(key, val)
	m.Insert(key, val)
	d.maybeScheduleFlush()
}

func (d *DB) Delete(key []byte) {
	m := d.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
	d.maybeScheduleFlush()
}

// Ensures that the mutable memtable has sufficient space to accommodate the insertion of "key" and "val".
func (d *DB) prepMemtableForKV(key, val []byte) *memtable.Memtable {
	m := d.memtables.mutable

	if !m.HasRoomForWrite(key, val) {
		m = d.rotateMemtables()
	}
	return m
}

func (d *DB) rotateMemtables() *memtable.Memtable {
	d.memtables.mutable = memtable.NewMemtable(memtableSizeLimit)
	d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	return d.memtables.mutable
}

func (d *DB) maybeScheduleFlush() {
	var totalSize int

	for i := 0; i < len(d.memtables.queue); i++ {
		totalSize += d.memtables.queue[i].Size()
	}

	if totalSize <= memtableFlushThreshold {
		return
	}

	err := d.flushMemtables()
	if err != nil {
		log.Fatal(err)
	}
}

func (d *DB) flushMemtables() error {
	n := len(d.memtables.queue) - 1
	flushable := d.memtables.queue[:n]
	d.memtables.queue = d.memtables.queue[n:]

	for i := 0; i < len(flushable); i++ {
		meta := d.dataStorage.PrepareNewFile()
		f, err := d.dataStorage.OpenFileForWriting(meta)
		if err != nil {
			return err
		}
		w := sstable.NewWriter(f)
		err = w.Process(flushable[i])
		if err != nil {
			return err
		}
		err = w.Close()
		if err != nil {
			return err
		}
		d.sstables = append(d.sstables, meta)
	}
	return nil
}

func (d *DB) Get(key []byte) ([]byte, error) {
	// Scan memtables from newest to oldest.
	for i := len(d.memtables.queue) - 1; i >= 0; i-- {
		m := d.memtables.queue[i]
		encodedValue, err := m.Get(key)
		if err != nil {
			continue // The only possible error is "key not found".
		}
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in memtable "%d".`, key, i)
			return nil, errors.New("key not found")
		}
		log.Printf(`Found key "%s" in memtable "%d" with value "%s"`, key, i, encodedValue.Value())
		return encodedValue.Value(), nil
	}
	// Scan sstables from newest to oldest.
	for j := len(d.sstables) - 1; j >= 0; j-- {
		meta := d.sstables[j]
		f, err := d.dataStorage.OpenFileForReading(meta)
		if err != nil {
			return nil, err
		}
		r, err := sstable.NewReader(f)
		if err != nil {
			return nil, err
		}
		defer r.Close()

		var encodedValue *encoder.EncodedValue
		encodedValue, err = r.Get(key)
		if err != nil {
			if errors.Is(err, sstable.ErrKeyNotFound) {
				continue
			}
			log.Fatal(err)
		}
		if encodedValue.IsTombstone() {
			log.Printf(`Found key "%s" marked as deleted in sstable "%d".`, key, meta.FileNum())
			return nil, errors.New("key not found")
		}
		log.Printf(`Found key "%s" in sstable "%d" with value "%s"`, key, meta.FileNum(), encodedValue.Value())
		return encodedValue.Value(), nil
	}

	return nil, errors.New("key not found")
}
