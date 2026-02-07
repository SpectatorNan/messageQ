package api

import (
	"messageQ/mq/broker"
	"time"
)

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
