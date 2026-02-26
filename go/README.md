# MessageQ (Go)

Minimal MQ with per-topic WAL segments, retry, DLQ, and structured logging.

## Quick Run

```bash
cd /Users/spectator/Documents/GitHub/nan/messageQ/go
go run .
```

Default server: `http://localhost:8080`

## As a Dependency

Because this module is in the `go/` subdirectory, use this module path:

```bash
go get github.com/SpectatorNan/messageQ/go@latest
```

Import examples:

```go
import (
    "github.com/SpectatorNan/messageQ/go/sdk"
    "github.com/SpectatorNan/messageQ/go/mq/broker"
)
```

## Features

- ✅ **RESTful API** with versioned endpoints (`/api/v1`)
- ✅ **WAL-based persistence** (RocketMQ-style CommitLog)
- ✅ **Retry & DLQ** (Dead Letter Queue)
- ✅ **Consumer groups** with offset management
- ✅ **Delayed messages** with persistent binary scheduler
- ✅ **Tag filtering** for selective consumption
- ✅ **Structured logging** with zap (JSON/console formats)
- ✅ **Graceful shutdown** with proper resource cleanup

## API

See [API documentation](mq/api/ROUTE_OPTIMIZATION.md) for complete endpoint reference.

**Quick reference:**
- Produce: `POST /api/v1/topics/:topic/messages`
- Consume: `GET /api/v1/consumers/:group/topics/:topic/messages`
- Ack: `POST /api/v1/messages/:id/ack`
- Nack: `POST /api/v1/messages/:id/nack`
- Delayed: `POST /api/v1/topics/:topic/delayed-messages`
- Stats: `GET /api/v1/stats`

## Logging

MessageQ uses [zap](https://github.com/uber-go/zap) for structured, high-performance logging.

See [LOGGING.md](docs/LOGGING.md) for detailed documentation.

**Quick configuration:**

```go
// Default: INFO level, console format
logger.InitDefault()

// Custom configuration
cfg := logger.Config{
    Level:      logger.DebugLevel,
    OutputPath: "stdout",  // or file path
    Format:     "json",    // or "console"
}
logger.Init(cfg)
```

**Example logs:**

```
2026-02-03T17:38:00.123+0800    INFO    Starting MessageQ server
2026-02-03T17:38:05.234+0800    INFO    Message produced    topic=orders message_id=550e8400... tag=create
2026-02-03T17:38:05.345+0800    INFO    Message consumed    group=consumers topic=orders message_id=550e8400... queue_id=2
2026-02-03T17:38:10.123+0800    WARN    Message moved to DLQ - max retries exceeded    message_id=550e8400... topic=orders retry_count=3
```

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
