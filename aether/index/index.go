package index

import (
	"hash/fnv"
	"sync/atomic"
)

// Meta points into the mmap value log.
type Meta struct {
	Offset    uint64
	Length    uint32
	Tombstone bool
}

type node struct {
	key  string
	meta atomic.Pointer[Meta]
	next *node
}

// LockFreeIndex is a fixed-size lock-free hash table with CAS-based updates.
type LockFreeIndex struct {
	buckets []atomic.Pointer[node]
}

func New(size int) *LockFreeIndex {
	if size < 64 {
		size = 64
	}
	return &LockFreeIndex{buckets: make([]atomic.Pointer[node], size)}
}

func hashKey(k string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(k))
	return h.Sum64()
}

func (l *LockFreeIndex) bucket(k string) *atomic.Pointer[node] {
	idx := hashKey(k) % uint64(len(l.buckets))
	return &l.buckets[idx]
}

func (l *LockFreeIndex) Get(k string) (Meta, bool) {
	head := l.bucket(k).Load() // Atomic load gives acquire semantics for concurrent readers.
	for n := head; n != nil; n = n.next {
		if n.key == k {
			m := n.meta.Load() // Atomic metadata pointer read; no locks or copies of value payload.
			if m == nil || m.Tombstone {
				return Meta{}, false
			}
			return *m, true
		}
	}
	return Meta{}, false
}

// Put inserts or atomically replaces metadata for a key.
func (l *LockFreeIndex) Put(k string, m Meta) {
	b := l.bucket(k)
	for {
		head := b.Load() // Load current head for lock-free traversal and potential CAS insert.
		for n := head; n != nil; n = n.next {
			if n.key == k {
				for {
					oldPtr := n.meta.Load()
					newMeta := &Meta{Offset: m.Offset, Length: m.Length, Tombstone: false}
					if n.meta.CompareAndSwap(oldPtr, newMeta) { // CAS guarantees single-writer success without mutexes.
						return
					}
				}
			}
		}
		meta := &Meta{Offset: m.Offset, Length: m.Length, Tombstone: false}
		n := &node{key: k, next: head}
		n.meta.Store(meta)
		if b.CompareAndSwap(head, n) { // Bucket-head CAS is the lock-free insertion primitive.
			return
		}
	}
}

func (l *LockFreeIndex) Delete(k string) bool {
	b := l.bucket(k)
	head := b.Load()
	for n := head; n != nil; n = n.next {
		if n.key == k {
			for {
				oldPtr := n.meta.Load()
				if oldPtr == nil || oldPtr.Tombstone {
					return false
				}
				newMeta := &Meta{Offset: oldPtr.Offset, Length: oldPtr.Length, Tombstone: true}
				if n.meta.CompareAndSwap(oldPtr, newMeta) { // Lock-free logical delete via tombstone pointer swap.
					return true
				}
			}
		}
	}
	return false
}

func (l *LockFreeIndex) Snapshot() map[string]Meta {
	out := make(map[string]Meta)
	for i := range l.buckets {
		head := l.buckets[i].Load()
		for n := head; n != nil; n = n.next {
			m := n.meta.Load()
			if m != nil {
				out[n.key] = *m
			}
		}
	}
	return out
}
