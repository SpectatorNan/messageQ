package queue

import "time"

type Message struct {
	ID        string    `json:"id"`
	Body      string    `json:"body"`
	Tag       string    `json:"tag,omitempty"`
	Retry     int       `json:"retry"`
	Timestamp time.Time `json:"timestamp"`
}

// InflightMsg is deprecated - inflight tracking is now handled at broker level
