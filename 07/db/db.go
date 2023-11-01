package db

import (
	"errors"
	"io"
	"log"

	"github.com/cloudcentricdev/golang-tutorials/07/db/encoder"
	"github.com/cloudcentricdev/golang-tutorials/07/db/memtable"
	"github.com/cloudcentricdev/golang-tutorials/07/db/sstable"
	"github.com/cloudcentricdev/golang-tutorials/07/db/storage"
	"github.com/cloudcentricdev/golang-tutorials/07/db/wal"
)

const (
	memtableSizeLimit      = 5 * (3 << 10) // 3 KiB
	memtableFlushThreshold = 1
)

type DB struct {
	dataStorage *storage.Provider
	memtables   struct {
		mutable *memtable.Memtable
		queue   []*memtable.Memtable
	}
	wal struct {
		w  *wal.Writer
		fm *storage.FileMetadata
	}
	sstables []*storage.FileMetadata
	logs     []*storage.FileMetadata
}

func Open(dirname string) (*DB, error) {
	dataStorage, err := storage.NewProvider(dirname)
	if err != nil {
		return nil, err
	}
	db := &DB{dataStorage: dataStorage}
	if err = db.loadFiles(); err != nil {
		return nil, err
	}
	if err = db.replayWALs(); err != nil {
		return nil, err
	}
	if err = db.createNewWAL(); err != nil {
		return nil, err
	}
	db.rotateMemtables()
	return db, nil
}

func (d *DB) loadFiles() error {
	meta, err := d.dataStorage.ListFiles()
	if err != nil {
		return err
	}
	for _, f := range meta {
		switch {
		case f.IsSSTable():
			d.sstables = append(d.sstables, f)
		case f.IsWAL():
			d.logs = append(d.logs, f)
		default:
			continue
		}
	}
	return nil
}

func (d *DB) replayWALs() error {
	for _, fm := range d.logs {
		if err := d.replayWAL(fm); err != nil {
			return err
		}
	}
	d.logs = nil
	return nil
}

func (d *DB) replayWAL(fm *storage.FileMetadata) error {
	f, err := d.dataStorage.OpenFileForReading(fm)
	if err != nil {
		return err
	}
	r := wal.NewReader(f)
	d.wal.fm = fm
	m := d.rotateMemtables()
	for {
		key, val, err := r.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if !m.HasRoomForWrite(key, val.Value()) {
			d.rotateMemtables()
		}
		if val.IsTombstone() {
			m.InsertTombstone(key)
		} else {
			m.Insert(key, val.Value())
		}
	}
	d.rotateMemtables()
	if err = d.flushMemtables(); err != nil {
		return err
	}
	d.memtables.queue, d.memtables.mutable = nil, nil
	if err = f.Close(); err != nil {
		return err
	}
	return nil
}

func (d *DB) Set(key, val []byte) {
	_ = d.wal.w.RecordInsertion(key, val)
	m, _ := d.prepMemtableForKV(key, val)
	m.Insert(key, val)
	d.maybeScheduleFlush()
}

func (d *DB) Delete(key []byte) {
	_ = d.wal.w.RecordDeletion(key)
	m, _ := d.prepMemtableForKV(key, nil)
	m.InsertTombstone(key)
	d.maybeScheduleFlush()
}

// ensures that the mutable memtable has sufficient space to accommodate the insertion of "key" and "val".
func (d *DB) prepMemtableForKV(key, val []byte) (*memtable.Memtable, error) {
	m := d.memtables.mutable

	if !m.HasRoomForWrite(key, val) {
		if err := d.rotateWAL(); err != nil {
			return nil, err
		}
		m = d.rotateMemtables()
	}
	return m, nil
}

func (d *DB) rotateMemtables() *memtable.Memtable {
	d.memtables.mutable = memtable.NewMemtable(memtableSizeLimit, d.wal.fm)
	d.memtables.queue = append(d.memtables.queue, d.memtables.mutable)
	return d.memtables.mutable
}

func (d *DB) rotateWAL() (err error) {
	if err = d.wal.w.Close(); err != nil {
		return err
	}
	if err = d.createNewWAL(); err != nil {
		return err
	}
	return nil
}

func (d *DB) createNewWAL() error {
	ds := d.dataStorage
	fm := ds.PrepareNewWALFile()
	logFile, err := ds.OpenFileForWriting(fm)
	if err != nil {
		return err
	}
	d.wal.w = wal.NewWriter(logFile)
	d.wal.fm = fm
	return nil
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
		meta := d.dataStorage.PrepareNewSSTFile()
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
		err = d.dataStorage.DeleteFile(flushable[i].LogFile())
		if err != nil {
			return err
		}
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
