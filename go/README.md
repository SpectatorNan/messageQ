# MessageQ (Go)

Minimal MQ with per-topic WAL segments, retry, and DLQ.

## Quick Run

```bash
cd /Users/spectator/Documents/GitHub/nan/messageQ/go
go run .
```

Default server: `http://localhost:8080`

## API

- Produce: `POST /topics/:topic/messages` JSON body `{"body":"..."}`
- Consume: `GET /topics/:topic/messages`
- Ack: `POST /topics/:topic/messages/:id/ack`
- Nack: `POST /topics/:topic/messages/:id/nack`

## WAL Format

Binary record per segment:

```
[totalSize:4][crc32:4][type:1][id:8][retry:2][ts:8][bodyLen:4][body]
```

- `type`: 1=PRODUCE, 2=ACK, 3=NACK, 4=RETRY, 5=DLQ
- `crc32` is computed over `body` (raw bytes)
- `body` stores application payload; ACK/NACK records have bodyLen=0

Segments are stored under `./data/<topic>/00000001.wal` etc.

## Inspect WAL

```bash
go run ./cmd/mq-inspect -dir ./data -topic your-topic -show
```

## Tests

Run all tests:

```bash
go test ./...
```

Keep testdata on disk for inspection:

```bash
MSGQ_CLEAN_TESTDATA=false go test ./example -run Test -v
```

## Benchmarks

```bash
go test ./example -bench BenchmarkWAL_Intervals -benchtime=10s -run none
```
