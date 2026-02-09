package client

import (
	"messageQ/mq/broker"
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
	DelayMs  int64  `json:"delayMs"`  // optional: delay in milliseconds
	DelaySec int64  `json:"delaySec"` // optional: delay in seconds
}
type ProduceMessageResponse struct {
	ID           string  `json:"id"`
	Topic        string  `json:"topic"`
	Tag          string  `json:"tag"`
	ScheduledAt  int64   `json:"scheduledAt"`
	ExecuteAt    *int64  `json:"executeAt"`
	DelaySeconds float64 `json:"delaySeconds"`
	DelayMs      int64   `json:"delayMs"`
}

type CreateTopicRequest struct {
	Name       string           `json:"name" binding:"required"`
	Type       broker.TopicType `json:"type" binding:"required"` // NORMAL or DELAY
	QueueCount int              `json:"queueCount"`
}
type GetTopicRequest struct {
	Topic string `uri:"topic" binding:"required"`
}
type TopicResponse struct {
	Name       string           `json:"name"`
	Type       broker.TopicType `json:"type"`
	QueueCount int              `json:"queueCount"`
	CreatedAt  int64            `json:"createdAt,omitempty"`
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
		QueueID    int            `json:"queueId"`
		Offset     int64          `json:"offset"`
		NextOffset int64          `json:"nextOffset"`
		State      string         `json:"state"`
	}
	ConsumeMessage struct {
		ID        string `json:"id"`
		Body      string `json:"body"`
		Tag       string `json:"tag,omitempty"`
		Retry     int    `json:"retry"`
		Timestamp int64  `json:"timestamp"`
	}
	MessageStatus struct {
		ID          string `json:"id"`
		Body        string `json:"body"`
		Tag         string `json:"tag,omitempty"`
		Retry       int    `json:"retry"`
		Timestamp   int64  `json:"timestamp"`
		ScheduledAt *int64 `json:"scheduledAt,omitempty"`
		ConsumedAt  *int64 `json:"consumedAt,omitempty"`
		AckedAt     *int64 `json:"ackedAt,omitempty"`
		QueueID     *int   `json:"queueId,omitempty"`
		Offset      *int64 `json:"offset,omitempty"`
		NextOffset  *int64 `json:"nextOffset,omitempty"`
	}
	ListMessagesResponse struct {
		Group      string          `json:"group"`
		Topic      string          `json:"topic"`
		State      string          `json:"state"`
		Messages   []MessageStatus `json:"messages"`
		NextCursor *int64          `json:"nextCursor,omitempty"`
	}
	AckMessageResponse struct {
		MessageID string `json:"messageId"`
		Acked     bool   `json:"acked"`
		Topic     string `json:"topic"`
	}
	NackMessageResponse struct {
		MessageID string `json:"messageId"`
		Nacked    bool   `json:"nacked"`
		Topic     string `json:"topic"`
		Requeued  bool   `json:"requeued"`
	}
)
