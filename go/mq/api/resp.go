package api

import (
	"time"
)

// OffsetResponse is the response for getting offset
type OffsetResponse struct {
	Group   string `json:"group"`
	Topic   string `json:"topic"`
	QueueID int    `json:"queueId"`
	Offset  *int64 `json:"offset"` // pointer to distinguish between 0 and null
}

// CommitOffsetResponse is the response for committing offset
type CommitOffsetResponse struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	QueueID   int    `json:"queueId"`
	Offset    int64  `json:"offset"`
	Committed bool   `json:"committed"`
}

// StatsResponse is the response for stats endpoint
type StatsResponse struct {
	Groups         map[string]interface{} `json:"groups,omitempty"`
	DelayScheduler map[string]interface{} `json:"delayScheduler,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
}
