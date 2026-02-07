package client

import (
	"messageQ/mq/broker"
	"time"
)

type CreateAccessKeyRequest struct {
	Name      string `json:"name"`
	AccessKey string `json:"accessKey"`
}

type CreateAccessKeyResponse struct {
	Id        string `json:"id"`
	Name      string `json:"name"`
	AccessKey string `json:"accessKey"`
	CreatedAt int64  `json:"createdAt"`
}

type AccessKey struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt int64  `json:"createdAt"`
}

type Resp[T any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}

type ListResp[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

type ErrResp struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ProduceMessageRequest struct {
	Body     string `json:"body"`
	Tag      string `json:"tag"`
	DelayMs  int64  `json:"delay_ms"`  // optional: delay in milliseconds
	DelaySec int64  `json:"delay_sec"` // optional: delay in seconds
}
type ProduceMessageResponse struct {
	ID           string    `json:"id"`
	Topic        string    `json:"topic"`
	Tag          string    `json:"tag"`
	ScheduledAt  time.Time `json:"scheduled_at"`
	ExecuteAt    time.Time `json:"execute_at"`
	DelaySeconds float64   `json:"delay_seconds"`
	DelayMs      int64     `json:"delay_ms"`
}

type CreateTopicRequest struct {
	Name       string           `json:"name" binding:"required"`
	Type       broker.TopicType `json:"type" binding:"required"` // NORMAL or DELAY
	QueueCount int              `json:"queue_count"`
}
type GetTopicRequest struct {
	Topic string `uri:"topic" binding:"required"`
}
type TopicResponse struct {
	Name       string           `json:"name"`
	Type       broker.TopicType `json:"type"`
	QueueCount int              `json:"queue_count"`
	CreatedAt  int64            `json:"created_at,omitempty"`
}

// DeleteTopicResponse is the response for deleting a topic
type DeleteTopicResponse struct {
	Topic   string `json:"topic"`
	Deleted bool   `json:"deleted"`
}

type (
	ConsumeMessageResponse struct {
		Message    ConsumeMessage `json:"message"` // storage.Message
		Group      string         `json:"group"`
		Topic      string         `json:"topic"`
		QueueID    int            `json:"queue_id"`
		Offset     int64          `json:"offset"`
		NextOffset int64          `json:"next_offset"`
		State      string         `json:"state"`
	}
	ConsumeMessage struct {
		ID        string    `json:"id"`
		Body      string    `json:"body"`
		Tag       string    `json:"tag,omitempty"`
		Retry     int       `json:"retry"`
		Timestamp time.Time `json:"timestamp"`
	}
	AckMessageResponse struct {
		MessageID string `json:"message_id"`
		Acked     bool   `json:"acked"`
		Topic     string `json:"topic"`
	}
	NackMessageResponse struct {
		MessageID string `json:"message_id"`
		Nacked    bool   `json:"nacked"`
		Topic     string `json:"topic"`
		Requeued  bool   `json:"requeued"`
	}
)
