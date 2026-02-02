# MessageQ (Go)

Minimal MQ with per-topic WAL segments, retry, and DLQ.

## Quick Run

```bash
cd /Users/spectator/Documents/GitHub/nan/messageQ/go
go run .
```

Default server: `http://localhost:8080`

## API

- Produce: `POST /topics/:topic/messages` JSON body `{"body":"...","tag":"..."}`
- Consume (stateful): `GET /topics/:topic/messages?group=...&queue_id=0&tag=...`
- Ack (processing -> completed): `POST /topics/:topic/messages/:id/ack`
- Nack (processing -> retry): `POST /topics/:topic/messages/:id/nack`
- Get offset: `GET /topics/:topic/offsets/:group?queue_id=0`
- Commit offset: `POST /topics/:topic/offsets/:group` JSON body `{"queue_id":0,"offset":123}`
- Stats: `GET /stats`

Message IDs are UUIDv7 strings.

## Storage Layout (RocketMQ-style)

```
<base>/commitlog/<topic>/<queueId>/*.wal
<base>/consumequeue/<topic>/<queueId>/
<base>/offsets/<group>/<topic>/<queueId>.offset
```

DLQ uses `topic.dlq` and queueId=0.

## CommitLog Format

Binary record per segment:

```
[totalSize:4][crc32:4][id:16][retry:2][ts:8][tagLen:2][tag][bodyLen:4][body]
```

- `id`: UUIDv7 (16 bytes)
- `crc32` is computed over `body` (raw bytes)
- `tag` is stored with the message for filtering

## ConsumeQueue Format

Each entry is 20 bytes:

```
[segId:4][pos:8][size:4][tagHash:4]
```

`tagHash` is CRC32 of the tag (0 when empty).

Segments are stored under `./data/commitlog/<topic>/<queueId>/00000001.wal` etc.

## Inspect WAL / CQ / Offsets

```bash
# WAL (commitlog)
go run ./cmd/mq-inspect -dir ./data -topic your-topic -queue 0 -show

# ConsumeQueue
go run ./cmd/mq-inspect -dir ./data -topic your-topic -queue 0 -cq

# Consumer offsets
go run ./cmd/mq-inspect -dir ./data -topic your-topic -queue 0 -offsets -group your-group
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
