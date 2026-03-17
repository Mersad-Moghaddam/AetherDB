# AetherDB Design Documentation

## 1) Durable Storage with `mmap`

### How `mmap` works
`mmap` maps a file (or part of a file) into a process virtual address range. The CPU loads/stores into virtual addresses; the MMU and kernel page tables translate these to physical pages. With a file-backed mapping:

1. A virtual page fault occurs on first access.
2. Kernel resolves fault by loading the corresponding file page into page cache.
3. The virtual page is linked to that cached physical page.
4. Reads/writes become normal memory operations from user code.
5. Dirty pages are written back to storage (background flush or explicit `msync`).

So disk pages become memory-like from application code while still maintaining persistence semantics.

### Why this helps
- **Lower syscall overhead**: append/read are mostly memory operations after mapping.
- **Kernel page cache reuse**: storage and network stack can share cached pages.
- **High locality**: append-only data layout makes sequential writes efficient.

### Risks / trade-offs
- **File growth complexity**: mapping has fixed length; database must unmap, `truncate`, remap.
- **Page-fault latency spikes**: first-touch faults can add tail latency.
- **Crash semantics**: writes hit memory first, not durable until flush.
- **Corruption handling**: header/index validation required on startup.

### On-disk format

```text
+------------------------------+
| Header (64 bytes)            |
| magic, version               |
| index_offset, index_length   |
| data_offset, data_write_off  |
+------------------------------+
| Reserved / alignment padding |
| (up to data_offset=4096)     |
+------------------------------+
| Data section                 |
| append-only value bytes      |
+------------------------------+
| Index snapshot section       |
| repeated:                    |
| key_len (u32)                |
| data_off (u64)               |
| value_len (u32)              |
| tombstone (u8)               |
| key bytes                    |
+------------------------------+
```

### Growth + durability strategy
- **Pre-allocation**: file starts at 64 MiB (or configured size), minimizing remap frequency.
- **Geometric growth**: when full, size doubles (`truncate` + remap).
- **Durability**: `msync(MS_SYNC)` is called after metadata/index updates to force writeback.
- Optional future variant: `O_DIRECT` could bypass page cache for some write paths, but current architecture intentionally leverages shared page cache for zero-copy reads.

## 2) Zero-Copy Network Transport

### Classic inefficient path
Without zero-copy, a GET usually does:

1. Disk/page cache -> kernel read buffer.
2. Kernel -> user-space app buffer (`read`).
3. User-space -> kernel socket buffer (`write`).
4. Kernel NIC DMA transfer.

That includes extra copies and context switching.

### Zero-copy path in AetherDB
For GET responses, AetherDB uses `sendfile`:

- App sends response header normally.
- App calls `sendfile(socket_fd, db_file_fd, &offset, len)`.
- Kernel copies from file-backed page cache directly into socket pipeline, avoiding user-space payload copies.

This cuts CPU cycles and memory bandwidth for large/high-throughput reads.

### Protocol
Request frame:

```text
byte 0      : cmd (1=PUT, 2=GET, 3=DEL)
bytes 1..4  : key_len (u32 little-endian)
bytes 5..8  : value_len (u32 little-endian; only PUT)
bytes ...   : key bytes
bytes ...   : value bytes (PUT only)
```

Response frame:

```text
byte 0      : status (0=OK, 1=NOT_FOUND, 2=ERROR)
bytes 1..4  : value_len (u32 little-endian)
bytes ...   : value bytes only for GET+OK (streamed via sendfile)
```

## 3) Lock-Free Concurrency Control

### Atomicity and memory barriers
Atomic operations guarantee indivisible updates on shared words. In Go, atomic loads/stores/CAS include ordering guarantees (acquire/release style effects) so concurrent goroutines observe consistent pointer publications without mutexes.

### Compare-and-Swap (CAS)
`CAS(addr, old, new)` means:
- if `*addr == old`, write `new` and return success;
- otherwise fail, and caller retries.

This is the foundation of lock-free loops: read state -> derive new state -> CAS -> retry on contention.

### AetherDB index design
- Fixed-size hash table of buckets.
- Each bucket head is an `atomic.Pointer[node]`.
- Insert is head-CAS (Treiber-style prepend).
- Update/delete on existing key use CAS swap of node metadata pointer.
- Delete is logical tombstone (lock-free, no physical unlink needed).

### Trade-offs
- **Pros**: no lock convoying, good scaling under high goroutine counts.
- **Cons**: harder correctness reasoning, potential ABA in some pointer reuse patterns (mitigated here by allocating fresh metadata nodes and never recycling pointers manually).

## 4) Module layout
- `aether/storage`: mmap file lifecycle, format, append log, index snapshot persistence.
- `aether/index`: lock-free hash index.
- `aether/net`: TCP server, protocol parsing, zero-copy GET with `sendfile`.
- `aether/api`: public `GET/PUT/DELETE` + orchestration and recovery.
- `main.go`: runnable demo with concurrent workers.

## 5) Benchmark approach
Benchmarks run `PUT+GET` in parallel with payload sizes 64B, 1KB, 16KB to show scaling trends across data sizes and goroutine contention.
