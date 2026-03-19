package storage

import "time"

// Message is storage's serializable representation to avoid importing queue (prevents cycles)
type Message struct {
	ID            string    `json:"id"`
	Body          string    `json:"body"`
	Tag           string    `json:"tag,omitempty"`
	CorrelationID string    `json:"correlationId,omitempty"`
	Retry         int       `json:"retry"`
	Timestamp     time.Time `json:"timestamp"`
}

// MessageWithOffset adds consumequeue offset metadata.
type MessageWithOffset struct {
	Message
	Offset int64 `json:"offset"`
}

// Storage is a simple persistence interface for queue messages per topic/queue.
// Implementations should be safe for concurrent use by the application layer.
type Storage interface {
	// Append writes the message to storage for the given topic/queue (persist the message)
	Append(topic string, queueID int, msg Message) error
	// Load loads all persisted messages for the given topic/queue (excluding acked)
	Load(topic string, queueID int) ([]Message, error)
	// Ack marks message id as acknowledged in storage (so future Load will not return it)
	Ack(topic string, queueID int, id string) error
}
