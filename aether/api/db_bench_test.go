package api

import (
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func benchmarkPutGet(b *testing.B, valueSize int) {
	b.Helper()
	dir := b.TempDir()
	db, err := Open(filepath.Join(dir, "bench.db"), 64<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	payload := make([]byte, valueSize)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	var ctr atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := ctr.Add(1)
			k := fmt.Sprintf("bench-key-%d", i)
			if err := db.Put(k, payload); err != nil {
				b.Fatal(err)
			}
			if _, ok, err := db.Get(k); err != nil || !ok {
				b.Fatalf("get failed: ok=%v err=%v", ok, err)
			}
		}
	})
}

func BenchmarkDBPutGet64B(b *testing.B)  { benchmarkPutGet(b, 64) }
func BenchmarkDBPutGet1KB(b *testing.B)  { benchmarkPutGet(b, 1024) }
func BenchmarkDBPutGet16KB(b *testing.B) { benchmarkPutGet(b, 16*1024) }
