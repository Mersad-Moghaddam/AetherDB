# AetherDB

AetherDB is a high-performance embedded KV store in Go using:
- `mmap`-based durable storage
- lock-free CAS index
- zero-copy GET transport via `sendfile`

## Run

```bash
go run .
```

## Benchmarks

```bash
go test -bench . -benchmem ./aether/api
```

See detailed architecture in `docs/design.md`.
