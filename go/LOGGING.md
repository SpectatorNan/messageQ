# Logging Configuration

MessageQ uses [zap](https://github.com/uber-go/zap) for structured, high-performance logging.

## Features

- **Structured Logging**: All logs include contextual fields (topic, message_id, queue_id, etc.)
- **Multiple Log Levels**: DEBUG, INFO, WARN, ERROR, FATAL
- **Flexible Output**: Console or JSON format
- **Configurable**: Environment-based configuration support

## Log Levels

### DEBUG
Detailed information for debugging. Includes:
- Consumer operations with no messages available
- Internal state transitions

### INFO
General operational messages. Includes:
- Server startup/shutdown
- Broker initialization
- Message production/consumption
- Message acknowledgment
- Consumer offset operations

### WARN
Warning conditions that should be investigated. Includes:
- Invalid parameters (topic names, message IDs)
- Failed to load topic manager or delay scheduler
- Enqueue rejections (empty tags)
- Messages moved to DLQ

### ERROR
Error conditions requiring attention. Includes:
- Storage operation failures
- Message consumption errors
- Acknowledgment failures
- Failed to close resources

### FATAL
Critical errors causing application termination. Includes:
- Server startup failures
- Logger initialization failures

## Configuration

### Programmatic Configuration

```go
import "messageQ/mq/logger"

// Use default configuration (INFO level, console format, stdout)
logger.InitDefault()

// Custom configuration
cfg := logger.Config{
    Level:      logger.InfoLevel,  // debug, info, warn, error
    OutputPath: "stdout",           // stdout, stderr, or file path
    Format:     "console",          // console or json
}
logger.Init(cfg)
```

### Environment Variables (Future Enhancement)

```bash
# Set log level
export MQ_LOG_LEVEL=debug

# Set output format
export MQ_LOG_FORMAT=json

# Set output path
export MQ_LOG_OUTPUT=/var/log/messageq.log
```

## Log Format

### Console Format (Default)
Human-readable format with color-coded levels:

```
2026-02-03T17:38:00.123+0800    INFO    Starting MessageQ server
2026-02-03T17:38:00.125+0800    INFO    Broker initialized    queue_count=4
2026-02-03T17:38:00.126+0800    INFO    HTTP server listening    addr=:8080
2026-02-03T17:38:05.234+0800    INFO    Message produced    topic=orders message_id=550e8400-e29b-41d4-a716-446655440000 tag=create
2026-02-03T17:38:05.345+0800    INFO    Message consumed    group=consumers topic=orders message_id=550e8400-e29b-41d4-a716-446655440000 queue_id=2 offset=42
2026-02-03T17:38:05.456+0800    INFO    Message acknowledged    message_id=550e8400-e29b-41d4-a716-446655440000
2026-02-03T17:38:10.123+0800    WARN    Invalid topic name    topic=invalid topic
2026-02-03T17:38:15.234+0800    ERROR    Consume error    group=consumers topic=orders error="offset not supported"
```

### JSON Format
Machine-parseable format for log aggregation:

```json
{"level":"INFO","time":"2026-02-03T17:38:00.123+0800","msg":"Starting MessageQ server"}
{"level":"INFO","time":"2026-02-03T17:38:00.125+0800","msg":"Broker initialized","queue_count":4}
{"level":"INFO","time":"2026-02-03T17:38:00.126+0800","msg":"HTTP server listening","addr":":8080"}
{"level":"INFO","time":"2026-02-03T17:38:05.234+0800","msg":"Message produced","topic":"orders","message_id":"550e8400-e29b-41d4-a716-446655440000","tag":"create"}
{"level":"WARN","time":"2026-02-03T17:38:10.123+0800","msg":"Message moved to DLQ - max retries exceeded","message_id":"550e8400-e29b-41d4-a716-446655440000","topic":"orders","retry_count":3}
```

## Log Categories

### Server Lifecycle
- `Starting MessageQ server`
- `Broker initialized` (queue_count)
- `HTTP server listening` (addr)
- `Shutdown signal received, shutting down gracefully...`
- `Server gracefully stopped`

### Message Operations
- `Message produced` (topic, message_id, tag)
- `Message consumed` (group, topic, message_id, queue_id, offset)
- `Message acknowledged` (message_id)
- `Message nacked for retry` (message_id)

### Storage Operations
- `Failed to load messages from storage` (topic, queue_id, error)
- `Failed to persist retry message` (topic, queue_id, message_id, error)
- `Failed to append message to DLQ` (topic, dlq_topic, message_id, error)

### Error Conditions
- `Invalid topic name` (topic)
- `Invalid message payload` (error)
- `Ack failed - message not found` (message_id)
- `Nack failed - message not found` (message_id)
- `Consume error` (group, topic, error)

### DLQ Events
- `Message moved to DLQ - max retries exceeded` (message_id, topic, retry_count)
- `Message moved to DLQ after nack` (message_id, topic, retry_count)
- `Message moved to DLQ due to timeout` (message_id, topic, retry_count)

## Best Practices

### Production Deployment

1. **Use JSON format** for easy parsing by log aggregators:
```go
cfg := logger.Config{
    Level:      logger.InfoLevel,
    OutputPath: "/var/log/messageq/app.log",
    Format:     "json",
}
```

2. **Set appropriate log level**:
   - Production: `InfoLevel`
   - Staging: `DebugLevel`
   - Development: `DebugLevel`

3. **Rotate log files** using logrotate or similar tools

4. **Monitor ERROR and FATAL logs** with alerting

### Development

1. **Use console format** for readability:
```go
logger.InitDefault() // Console format with colors
```

2. **Use DEBUG level** for troubleshooting:
```go
cfg := logger.Config{
    Level:  logger.DebugLevel,
    Format: "console",
}
```

### Structured Logging

All logs include relevant context fields:

```go
logger.Info("Message produced",
    zap.String("topic", topic),
    zap.String("message_id", msg.ID),
    zap.String("tag", tag))
```

## Log Aggregation

### ELK Stack
Use JSON format and ship logs to Elasticsearch:

```bash
filebeat.inputs:
- type: log
  enabled: true
  paths:
    - /var/log/messageq/*.log
  json.keys_under_root: true
```

### Loki
Use promtail to collect and send logs:

```yaml
- job_name: messageq
  static_configs:
  - targets:
      - localhost
    labels:
      job: messageq
      __path__: /var/log/messageq/*.log
```

### CloudWatch
Use CloudWatch agent with JSON parsing:

```json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/messageq/*.log",
            "log_group_name": "messageq",
            "log_stream_name": "{instance_id}"
          }
        ]
      }
    }
  }
}
```

## Troubleshooting

### No logs appearing

Check logger initialization in main.go:
```go
if err := logger.InitDefault(); err != nil {
    fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
    os.Exit(1)
}
defer logger.Sync() // Ensure logs are flushed
```

### Log level too verbose

Adjust the log level:
```go
cfg := logger.Config{
    Level: logger.WarnLevel, // Only WARN, ERROR, FATAL
}
```

### Performance concerns

- Use JSON format (faster than console)
- Use INFO or WARN level in production
- Avoid DEBUG level in high-throughput scenarios
- Enable log sampling if needed (future enhancement)

## Future Enhancements

- [ ] Log sampling for high-frequency events
- [ ] Metrics integration (Prometheus)
- [ ] Trace ID propagation for distributed tracing
- [ ] Dynamic log level adjustment (HTTP endpoint)
- [ ] Environment variable configuration
- [ ] Log rotation built-in support
