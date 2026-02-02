package storage

import "time"

// Message is storage's serializable representation to avoid importing queue (prevents cycles)
type Message struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	Retry     int       `json:"retry"`
	Timestamp time.Time `json:"timestamp"`
}

// Storage is a simple persistence interface for queue messages per topic.
// Implementations should be safe for concurrent use by the application layer.
type Storage interface {
	// Append writes the message to storage for the given topic (persist the message)
	Append(topic string, msg Message) error
	// Load loads all persisted messages for the given topic (excluding acked)
	Load(topic string) ([]Message, error)
	// Ack marks message id as acknowledged in storage (so future Load will not return it)
	Ack(topic string, id string) error
}
