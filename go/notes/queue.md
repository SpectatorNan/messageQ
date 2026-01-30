MessageQ — Queue package and HTTP endpoints

Overview

This small project provides an in-memory message queue and a minimal HTTP API to produce and consume messages for a given topic. It's intended as a lightweight learning/example implementation.

Key components

- mq/queue: in-memory queue implementation. Exposes NewQueue(), Enqueue(body string) Message, Dequeue() Message, Ack(id int64), Nack(id int64).
- broker.go: maintains a map of topic -> *queue.Queue and returns the queue for a topic on demand.
- handler.go: HTTP handlers that call the broker to produce/consume messages.
- types.go: application-level types and response helpers. Message is an alias to mq/queue.Message.

Message lifecycle

1. Producer POSTs to /produce?topic=foo with JSON {"body":"..."} or raw body text.
2. The handler calls broker.GetQueue("foo").Enqueue(body) which returns a Message with an ID and timestamp.
3. Consumer GETs /consume?topic=foo which blocks until a message is available and returns the Message JSON.
4. Consumers should call Ack (not yet exposed via HTTP endpoint) to acknowledge processing; unacked messages are re-queued after a timeout and retried a limited number of times, then sent to DLQ (logged).

Notes and next steps

- Current implementation is memory-only and unsuitable for multi-process or durable workloads.
- Consider adding an HTTP endpoint for ACK/NACK (e.g., POST /ack?topic=foo body {"id":123}).
- For persistence, implement a Storage interface and provide a file or DB-backed storage.
- Improve API: add long-poll timeouts and client-visible retry counts in the message JSON.

Quick curl examples

Produce:

curl -X POST "http://localhost:8080/produce?topic=foo" -d '{"body":"hello world"}' -H 'Content-Type: application/json'

Consume:

curl "http://localhost:8080/consume?topic=foo"

