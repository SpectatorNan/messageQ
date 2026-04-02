# syntax=docker/dockerfile:1.6

# MessageQ Docker image
# Config mapping: use MQ_* environment variables to override config.yaml fields.
# Example: MQ_SERVER_PORT=8080 maps to server.port in config.

FROM golang:1.24.4-alpine AS builder
WORKDIR /src

RUN apk add --no-cache ca-certificates git

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/messageq ./main.go

FROM alpine:3.20
WORKDIR /app

RUN apk add --no-cache ca-certificates && update-ca-certificates

COPY --from=builder /out/messageq /app/messageq
COPY config.example.yaml /app/config.yaml

# Default config mapping (override via environment variables)
ENV MQ_ADMIN_KEY="" \
    MQ_SERVER_PORT=8080 \
    MQ_SERVER_HOST="" \
    MQ_SERVER_SHUTDOWN_TIMEOUT=10s \
    MQ_BROKER_QUEUE_COUNT=4 \
    MQ_BROKER_MAX_RETRY=3 \
    MQ_BROKER_PROCESSING_TIMEOUT=30s \
    MQ_BROKER_TIMEOUT_CHECK_INTERVAL=5s \
    MQ_BROKER_DATA_DIR=/app/data \
    MQ_BROKER_RETRY_BACKOFF_BASE=1s \
    MQ_BROKER_RETRY_BACKOFF_MULTIPLIER=2.0 \
    MQ_BROKER_RETRY_BACKOFF_MAX=60s \
    MQ_STORAGE_FLUSH_INTERVAL=100ms \
    MQ_STORAGE_COMPACT_INTERVAL=5m \
    MQ_STORAGE_COMPACT_THRESHOLD=10485760 \
    MQ_STORAGE_MAX_RECORD_SIZE=67108864 \
    MQ_STORAGE_BUFFER_SIZE=65536 \
    MQ_LOGGING_LEVEL=info \
    MQ_LOGGING_OUTPUT_PATH=stdout \
    MQ_LOGGING_FORMAT=console

VOLUME ["/app/data"]
VOLUME ["/app/logs"]
EXPOSE 8080

ENTRYPOINT ["/app/messageq"]
