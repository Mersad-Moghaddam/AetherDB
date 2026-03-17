# AetherDB

AetherDB is a high-performance embedded KV store in Go using:
- `mmap`-based durable storage
- lock-free CAS index
- zero-copy GET transport via `sendfile`

## Run

```bash
go run .
```

The default run now includes the required **100,000 concurrent writes** scenario via Goroutines and prints throughput metrics.

## Dedicated 100k concurrent write scenario

```bash
go run ./cmd/scenario100k -total 100000 -workers 128 -value-size 256
```

## Benchmarks

```bash
go test -bench . -benchmem ./aether/api
```

See detailed architecture in `docs/design.md`.
