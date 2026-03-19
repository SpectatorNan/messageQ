package queue

import "time"

type Message struct {
	ID            string    `json:"id"`
	Body          string    `json:"body"`
	Tag           string    `json:"tag,omitempty"`
	CorrelationID string    `json:"correlationId,omitempty"`
	Retry         int       `json:"retry"`
	Timestamp     time.Time `json:"timestamp"`
}
