package api

import (
	"messageQ/mq/broker"
	"time"
)

type Resp[T any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}
type ListResp[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

const (
	RespCodeOk = "ok"
)

func NewRespEmpty() Resp[string] {
	return Resp[string]{
		Code:    RespCodeOk,
		Message: "success",
	}
}

func NewRespList[T any](items []T, total int) Resp[ListResp[T]] {
	return Resp[ListResp[T]]{
		Code:    RespCodeOk,
		Message: "success",
		Data: ListResp[T]{
			Items: items,
			Total: total,
		},
	}
}

func NewRespSuccess[T any](data T) Resp[T] {
	return Resp[T]{
		Code:    RespCodeOk,
		Message: "success",
		Data:    data,
	}
}

func NewRespFail(code string, message string) Resp[string] {
	return Resp[string]{
		Code:    code,
		Message: message,
	}
}

// Response Structs for API endpoints

// ProduceResponse is the response for producing a message
type ProduceResponse struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Tag       string    `json:"tag"`
	Body      string    `json:"body"`
	Timestamp time.Time `json:"timestamp"`
	Retry     int       `json:"retry"`
}

// ProduceDelayResponse is the response for producing a delayed message
type ProduceDelayResponse struct {
	ID           string    `json:"id"`
	Topic        string    `json:"topic"`
	Tag          string    `json:"tag"`
	ScheduledAt  time.Time `json:"scheduled_at"`
	ExecuteAt    time.Time `json:"execute_at"`
	DelaySeconds float64   `json:"delay_seconds"`
	DelayMs      int64     `json:"delay_ms"`
}

// AckResponse is the response for acking a message
type AckResponse struct {
	MessageID string `json:"message_id"`
	Acked     bool   `json:"acked"`
	Topic     string `json:"topic"`
}

// NackResponse is the response for nacking a message
type NackResponse struct {
	MessageID string `json:"message_id"`
	Nacked    bool   `json:"nacked"`
	Topic     string `json:"topic"`
	Requeued  bool   `json:"requeued"`
}

// OffsetResponse is the response for getting offset
type OffsetResponse struct {
	Group   string `json:"group"`
	Topic   string `json:"topic"`
	QueueID int    `json:"queue_id"`
	Offset  *int64 `json:"offset"` // pointer to distinguish between 0 and null
}

// CommitOffsetResponse is the response for committing offset
type CommitOffsetResponse struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	QueueID   int    `json:"queue_id"`
	Offset    int64  `json:"offset"`
	Committed bool   `json:"committed"`
}

// ListTopicsResponse is the response for listing topics
type ListTopicsResponse struct {
	Topics []*broker.TopicConfig `json:"topics"`
	Total  int                   `json:"total"`
}

// StatsResponse is the response for stats endpoint
type StatsResponse struct {
	Groups         map[string]interface{} `json:"groups,omitempty"`
	DelayScheduler map[string]interface{} `json:"delay_scheduler,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
}
