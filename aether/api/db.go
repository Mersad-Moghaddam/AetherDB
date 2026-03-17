package api

import (
	"errors"
	"sort"

	"aetherdb/aether/index"
	"aetherdb/aether/storage"
)

type DB struct {
	store *storage.DBFile
	idx   *index.LockFreeIndex
}

func Open(path string, size uint64) (*DB, error) {
	st, err := storage.Open(path, size)
	if err != nil {
		return nil, err
	}
	db := &DB{store: st, idx: index.New(1 << 16)}
	entries, err := st.LoadIndex()
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	for _, e := range entries {
		if e.Tombstone {
			db.idx.Delete(e.Key)
			continue
		}
		db.idx.Put(e.Key, index.Meta{Offset: e.Offset, Length: e.Length})
	}
	return db, nil
}

func (d *DB) Get(key string) ([]byte, bool, error) {
	m, ok := d.idx.Get(key)
	if !ok {
		return nil, false, nil
	}
	buf, err := d.store.ReadAt(m.Offset, m.Length)
	if err != nil {
		return nil, false, err
	}
	return buf, true, nil
}

func (d *DB) GetMeta(key string) (index.Meta, bool) {
	return d.idx.Get(key)
}

func (d *DB) Put(key string, value []byte) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}
	off, ln, err := d.store.AppendValue(value)
	if err != nil {
		return err
	}
	d.idx.Put(key, index.Meta{Offset: off, Length: ln})
	return nil
}

func (d *DB) Delete(key string) bool {
	return d.idx.Delete(key)
}

func (d *DB) SyncIndex() error {
	snap := d.idx.Snapshot()
	entries := make([]storage.IndexEntry, 0, len(snap))
	keys := make([]string, 0, len(snap))
	for k := range snap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m := snap[k]
		entries = append(entries, storage.IndexEntry{Key: k, Offset: m.Offset, Length: m.Length, Tombstone: m.Tombstone})
	}
	return d.store.PersistIndex(entries)
}

func (d *DB) SyncAll() error {
	if err := d.SyncIndex(); err != nil {
		return err
	}
	return d.store.Sync()
}

func (d *DB) Close() error {
	_ = d.SyncAll()
	return d.store.Close()
}

func (d *DB) FileDescriptor() int {
	return d.store.FD()
}
