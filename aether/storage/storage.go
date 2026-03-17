package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

const (
	Magic             uint32 = 0x41455448 // "AETH"
	Version           uint32 = 1
	HeaderSize               = 64
	DefaultFileSize          = 64 << 20 // 64 MiB
	DefaultDataOffset        = 4096
)

const (
	headerMagicOffset       = 0
	headerVersionOffset     = 4
	headerIndexOffsetOffset = 8
	headerIndexLengthOffset = 16
	headerDataOffsetOffset  = 24
	headerDataWriteOffset   = 32
)

// IndexEntry is the persisted representation of index metadata.
type IndexEntry struct {
	Key       string
	Offset    uint64
	Length    uint32
	Tombstone bool
}

// DBFile owns the mmapped file and on-disk append-only value log.
type DBFile struct {
	file      *os.File
	fd        int
	data      []byte
	fileSize  uint64
	dataStart uint64
	writeOff  atomic.Uint64
	mapMu     sync.RWMutex
}

func Open(path string, minSize uint64) (*DBFile, error) {
	if minSize < DefaultDataOffset {
		minSize = DefaultFileSize
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	sz := uint64(st.Size())
	if sz == 0 {
		sz = minSize
		if err := f.Truncate(int64(sz)); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	mapped, err := syscall.Mmap(int(f.Fd()), 0, int(sz), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	db := &DBFile{file: f, fd: int(f.Fd()), data: mapped, fileSize: sz}
	if err := db.initOrLoadHeader(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (d *DBFile) initOrLoadHeader() error {
	magic := binary.LittleEndian.Uint32(d.data[headerMagicOffset : headerMagicOffset+4])
	if magic == 0 {
		binary.LittleEndian.PutUint32(d.data[headerMagicOffset:headerMagicOffset+4], Magic)
		binary.LittleEndian.PutUint32(d.data[headerVersionOffset:headerVersionOffset+4], Version)
		binary.LittleEndian.PutUint64(d.data[headerDataOffsetOffset:headerDataOffsetOffset+8], DefaultDataOffset)
		binary.LittleEndian.PutUint64(d.data[headerDataWriteOffset:headerDataWriteOffset+8], DefaultDataOffset)
		d.dataStart = DefaultDataOffset
		d.writeOff.Store(DefaultDataOffset)
		return d.Sync()
	}
	if magic != Magic {
		return fmt.Errorf("invalid magic: %x", magic)
	}
	if binary.LittleEndian.Uint32(d.data[headerVersionOffset:headerVersionOffset+4]) != Version {
		return errors.New("unsupported version")
	}
	d.dataStart = binary.LittleEndian.Uint64(d.data[headerDataOffsetOffset : headerDataOffsetOffset+8])
	if d.dataStart < HeaderSize {
		return errors.New("corrupt data offset")
	}
	wo := binary.LittleEndian.Uint64(d.data[headerDataWriteOffset : headerDataWriteOffset+8])
	if wo < d.dataStart || wo > d.fileSize {
		return errors.New("corrupt write offset")
	}
	d.writeOff.Store(wo)
	return nil
}

func (d *DBFile) GetIndexRegion() (offset uint64, length uint64) {
	offset = binary.LittleEndian.Uint64(d.data[headerIndexOffsetOffset : headerIndexOffsetOffset+8])
	length = binary.LittleEndian.Uint64(d.data[headerIndexLengthOffset : headerIndexLengthOffset+8])
	return
}

func (d *DBFile) LoadIndex() ([]IndexEntry, error) {
	off, length := d.GetIndexRegion()
	if off == 0 || length == 0 {
		return nil, nil
	}
	end := off + length
	if end > d.fileSize || off < HeaderSize {
		return nil, errors.New("invalid index region")
	}

	var out []IndexEntry
	p := off
	for p < end {
		if p+17 > end {
			return nil, errors.New("truncated index entry")
		}
		keyLen := binary.LittleEndian.Uint32(d.data[p : p+4])
		p += 4
		dataOff := binary.LittleEndian.Uint64(d.data[p : p+8])
		p += 8
		valLen := binary.LittleEndian.Uint32(d.data[p : p+4])
		p += 4
		tomb := d.data[p] == 1
		p++
		if p+uint64(keyLen) > end {
			return nil, errors.New("truncated key bytes")
		}
		key := string(d.data[p : p+uint64(keyLen)])
		p += uint64(keyLen)
		out = append(out, IndexEntry{Key: key, Offset: dataOff, Length: valLen, Tombstone: tomb})
	}
	return out, nil
}

// PersistIndex writes a full index snapshot in mapped memory and updates header metadata.
func (d *DBFile) PersistIndex(entries []IndexEntry) error {
	var total uint64
	for _, e := range entries {
		total += 17 + uint64(len(e.Key))
	}
	start := d.writeOff.Load()
	if start+total > d.fileSize {
		if err := d.growToFit(start + total); err != nil {
			return err
		}
	}
	p := start
	for _, e := range entries {
		binary.LittleEndian.PutUint32(d.data[p:p+4], uint32(len(e.Key)))
		p += 4
		binary.LittleEndian.PutUint64(d.data[p:p+8], e.Offset)
		p += 8
		binary.LittleEndian.PutUint32(d.data[p:p+4], e.Length)
		p += 4
		if e.Tombstone {
			d.data[p] = 1
		} else {
			d.data[p] = 0
		}
		p++
		copy(d.data[p:p+uint64(len(e.Key))], e.Key)
		p += uint64(len(e.Key))
	}
	binary.LittleEndian.PutUint64(d.data[headerIndexOffsetOffset:headerIndexOffsetOffset+8], start)
	binary.LittleEndian.PutUint64(d.data[headerIndexLengthOffset:headerIndexLengthOffset+8], total)
	d.writeOff.Store(p)
	binary.LittleEndian.PutUint64(d.data[headerDataWriteOffset:headerDataWriteOffset+8], p)
	return d.Sync()
}

// AppendValue appends value bytes into the mmapped append-only data region.
func (d *DBFile) AppendValue(value []byte) (offset uint64, length uint32, err error) {
	if len(value) == 0 {
		return 0, 0, nil
	}
	need := uint64(len(value))

	for {
		d.mapMu.RLock()
		old := d.writeOff.Load()
		newOff := old + need
		if newOff > d.fileSize {
			d.mapMu.RUnlock()
			if err := d.growToFit(newOff); err != nil {
				return 0, 0, err
			}
			continue
		}
		if d.writeOff.CompareAndSwap(old, newOff) {
			copy(d.data[old:newOff], value)
			binary.LittleEndian.PutUint64(d.data[headerDataWriteOffset:headerDataWriteOffset+8], newOff)
			d.mapMu.RUnlock()
			return old, uint32(len(value)), nil
		}
		d.mapMu.RUnlock()
	}
}

func (d *DBFile) ReadAt(offset uint64, length uint32) ([]byte, error) {
	d.mapMu.RLock()
	defer d.mapMu.RUnlock()
	end := offset + uint64(length)
	if end > uint64(len(d.data)) {
		return nil, errors.New("out of bounds read")
	}
	return d.data[offset:end], nil
}

func (d *DBFile) FD() int { return d.fd }

// Sync flushes dirty pages so mmap writes are durable on disk.
func (d *DBFile) Sync() error {
	if len(d.data) == 0 {
		return nil
	}
	_, _, errno := syscall.Syscall(syscall.SYS_MSYNC,
		uintptr(unsafe.Pointer(&d.data[0])),
		uintptr(len(d.data)),
		uintptr(syscall.MS_SYNC),
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func (d *DBFile) growToFit(required uint64) error {
	d.mapMu.Lock()
	defer d.mapMu.Unlock()
	target := d.fileSize
	if target == 0 {
		target = DefaultFileSize
	}
	for target < required {
		target *= 2
	}
	if target == d.fileSize {
		return nil
	}

	if err := syscall.Munmap(d.data); err != nil {
		return err
	}
	if err := d.file.Truncate(int64(target)); err != nil {
		return err
	}
	mapped, err := syscall.Mmap(d.fd, 0, int(target), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	d.data = mapped
	d.fileSize = target
	return nil
}

func (d *DBFile) Close() error {
	_ = d.Sync()
	if d.data != nil {
		_ = syscall.Munmap(d.data)
	}
	if d.file != nil {
		return d.file.Close()
	}
	return nil
}
