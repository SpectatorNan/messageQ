package api

import (
	"messageQ/mq/broker"
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

// StatsResponse is the response for the GET /stats endpoint.
type StatsResponse struct {
	Topics         []TopicStatsResponse          `json:"topics"`
	ConsumerGroups []ConsumerGroupStatsResponse   `json:"consumerGroups"`
	DelayScheduler map[string]interface{}         `json:"delayScheduler,omitempty"`
	Total          int64                          `json:"total"`
	Completed      int64                          `json:"completed"`
	Processing     int64                          `json:"processing"`
	Pending        int64                          `json:"pending"`
	Timestamp      time.Time                      `json:"timestamp"`
}

// TopicStatsResponse is the per-topic stats in the response.
type TopicStatsResponse struct {
	Name       string           `json:"name"`
	Type       broker.TopicType `json:"type"`
	QueueCount int              `json:"queueCount"`
	Total      int64            `json:"total"`
	CreatedAt  int64            `json:"createdAt"`
}

// ConsumerGroupStatsResponse is per-consumer-group stats in the response.
type ConsumerGroupStatsResponse struct {
	Group      string                          `json:"group"`
	Total      int64                           `json:"total"`
	Completed  int64                           `json:"completed"`
	Processing int64                           `json:"processing"`
	Pending    int64                           `json:"pending"`
	Topics     []ConsumerGroupTopicStatsResponse `json:"topics"`
}

// ConsumerGroupTopicStatsResponse is per-topic stats within a consumer group.
type ConsumerGroupTopicStatsResponse struct {
	Topic      string `json:"topic"`
	Total      int64  `json:"total"`
	Completed  int64  `json:"completed"`
	Processing int64  `json:"processing"`
	Pending    int64  `json:"pending"`
}

// TopicDetailStatsResponse is the response for the GET /stats/topics/:topic endpoint.
type TopicDetailStatsResponse struct {
	Name       string                          `json:"name"`
	Type       broker.TopicType                `json:"type"`
	QueueCount int                             `json:"queueCount"`
	Total      int64                           `json:"total"`
	Consumers  []TopicConsumerStatsResponse    `json:"consumers"`
	CreatedAt  int64                           `json:"createdAt"`
	Timestamp  time.Time                       `json:"timestamp"`
}

// TopicConsumerStatsResponse shows a consumer group's progress on a topic.
type TopicConsumerStatsResponse struct {
	Group      string `json:"group"`
	Completed  int64  `json:"completed"`
	Processing int64  `json:"processing"`
	Pending    int64  `json:"pending"`
}
