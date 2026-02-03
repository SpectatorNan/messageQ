# Logging System Implementation Summary

## Overview
Added comprehensive structured logging support to MessageQ using [uber-go/zap](https://github.com/uber-go/zap).

## Changes Made

### 1. New Logger Module (`mq/logger/logger.go`)
- Structured logging with zap
- Multiple log levels: DEBUG, INFO, WARN, ERROR, FATAL
- Configurable output: stdout, stderr, or file
- Two formats: console (colored, human-readable) and JSON (machine-parseable)
- Global logger instance for convenience

### 2. Main Application (`main.go`)
**Before:**
```go
log.Fatalf("server failed: %v", err)
fmt.Println("shutting down server...")
```

**After:**
```go
logger.Fatal("Server failed to start", zap.Error(err))
logger.Info("Shutdown signal received, shutting down gracefully...")
```

### 3. API Handlers (`mq/api/gin_handlers.go`)
Added logging for:
- Message production with context (topic, message_id, tag)
- Message consumption with details (group, topic, message_id, queue_id, offset)
- Acknowledgments and rejections
- Validation errors with specific error details
- Request processing errors

**Example:**
```go
logger.Info("Message produced",
    zap.String("topic", topic),
    zap.String("message_id", msg.ID),
    zap.String("tag", payload.Tag))
```

### 4. Queue Module (`mq/queue/queue.go`)
Replaced `log.Println` with structured logging:
- Storage operation errors
- DLQ (Dead Letter Queue) events
- Retry attempts
- Timeout handling

**Before:**
```go
log.Println("DLQ:", im.Msg.ID)
log.Println("storage retry append error:", err)
```

**After:**
```go
logger.Warn("Message moved to DLQ - max retries exceeded",
    zap.String("message_id", im.Msg.ID),
    zap.String("topic", q.topic),
    zap.Int("retry_count", im.Msg.Retry))
logger.Error("Failed to persist retry message",
    zap.String("topic", q.topic),
    zap.String("message_id", msg.ID),
    zap.Error(err))
```

### 5. Broker Module (`mq/broker/broker.go`)
- Initialization logging
- Warning messages for configuration issues

### 6. Documentation
- [LOGGING.md](LOGGING.md) - Comprehensive logging documentation
- [README.md](README.md) - Updated with logging features
- [examples/logging_example.go](examples/logging_example.go) - Usage examples

## Log Output Examples

### Console Format (Development)
```
2026-02-03T17:38:00.123+0800    INFO    Starting MessageQ server
2026-02-03T17:38:05.234+0800    INFO    Message produced    topic=orders message_id=550e8400... tag=create
2026-02-03T17:38:10.123+0800    WARN    Message moved to DLQ - max retries exceeded    message_id=550e8400... retry_count=3
```

### JSON Format (Production)
```json
{"level":"INFO","time":"2026-02-03T17:38:00.123+0800","msg":"Starting MessageQ server"}
{"level":"INFO","time":"2026-02-03T17:38:05.234+0800","msg":"Message produced","topic":"orders","message_id":"550e8400...","tag":"create"}
{"level":"WARN","time":"2026-02-03T17:38:10.123+0800","msg":"Message moved to DLQ - max retries exceeded","message_id":"550e8400...","retry_count":3}
```

## Usage

### Quick Start
```go
// In main.go
logger.InitDefault()
defer logger.Sync()
```

### Custom Configuration
```go
cfg := logger.Config{
    Level:      logger.DebugLevel,  // debug, info, warn, error
    OutputPath: "/var/log/mq.log",  // or "stdout", "stderr"
    Format:     "json",              // or "console"
}
logger.Init(cfg)
```

## Benefits

1. **Structured Context**: All logs include relevant fields (topic, message_id, etc.)
2. **Performance**: zap is one of the fastest logging libraries
3. **Flexibility**: Easy to switch between console/JSON, levels, and outputs
4. **Debugging**: Rich context helps troubleshoot issues quickly
5. **Production Ready**: JSON format integrates with log aggregators (ELK, Loki, CloudWatch)
6. **Type Safety**: Compile-time checks for log fields

## Testing

All existing tests pass with logging enabled:
```bash
$ go test ./mq/api -v
PASS
ok      messageQ/mq/api 0.299s
```

## Dependencies Added

```go
go.uber.org/zap v1.27.1
go.uber.org/multierr v1.11.0
```

## Future Enhancements

- [ ] Environment variable configuration (MQ_LOG_LEVEL, MQ_LOG_FORMAT)
- [ ] Log sampling for high-frequency events
- [ ] HTTP endpoint for dynamic log level adjustment
- [ ] Trace ID propagation for distributed tracing
- [ ] Metrics integration (Prometheus)
- [ ] Built-in log rotation support
