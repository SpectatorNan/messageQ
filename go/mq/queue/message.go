package queue

import "time"

type Message struct {
	ID        int64     `json:"id"`
	Body      string    `json:"body"`
	Retry     int       `json:"retry"`
	Timestamp time.Time `json:"timestamp"`
}

type InflightMsg struct {
	Msg      Message
	Deadline time.Time
}
