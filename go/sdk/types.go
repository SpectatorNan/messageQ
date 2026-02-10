package client

import (
	"encoding/json"
	"messageQ/mq/broker"
	"strconv"
	"time"
)

// FlexibleUnix handles both numeric unix seconds and RFC3339 time strings.
type FlexibleUnix int64

func (f *FlexibleUnix) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	if len(b) == 0 {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			return nil
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			*f = FlexibleUnix(t.Unix())
			return nil
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			*f = FlexibleUnix(t.Unix())
			return nil
		}
		// fallback: try parse numeric string
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			*f = FlexibleUnix(v)
			return nil
		}
		return nil
	}
	if v, err := strconv.ParseInt(string(b), 10, 64); err == nil {
		*f = FlexibleUnix(v)
		return nil
	}
	return nil
}

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
	Body        string        `json:"body"`
	Tag         string        `json:"tag"`
	DelayMs     int64         `json:"delayMs"`               // optional: delay in milliseconds
	DelaySec    int64         `json:"delaySec"`              // optional: delay in seconds
	ScheduledAt *FlexibleUnix `json:"scheduledAt,omitempty"` // optional: unix seconds or RFC3339
}
type ProduceBatchMessage struct {
	Body        string        `json:"body"`
	Tag         string        `json:"tag"`
	DelayMs     int64         `json:"delayMs"`
	DelaySec    int64         `json:"delaySec"`
	ScheduledAt *FlexibleUnix `json:"scheduledAt,omitempty"`
}
type ProduceBatchRequest struct {
	Messages []ProduceBatchMessage `json:"messages"`
}
type ProduceMessageResponse struct {
	ID           string        `json:"id"`
	Topic        string        `json:"topic"`
	Tag          string        `json:"tag"`
	ScheduledAt  FlexibleUnix  `json:"scheduledAt"`
	ExecuteAt    *FlexibleUnix `json:"executeAt"`
	DelaySeconds float64       `json:"delaySeconds"`
	DelayMs      int64         `json:"delayMs"`
}
type ProduceBatchResponse struct {
	Messages []ProduceMessageResponse `json:"messages"`
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
	ConsumeBatchResponse struct {
		Messages []ConsumeBatchMessage `json:"messages"`
		Group    string                `json:"group"`
		Topic    string                `json:"topic"`
		State    string                `json:"state"`
	}
	ConsumeMessage struct {
		ID        string       `json:"id"`
		Body      string       `json:"body"`
		Tag       string       `json:"tag,omitempty"`
		Retry     int          `json:"retry"`
		Timestamp FlexibleUnix `json:"timestamp"`
	}
	ConsumeBatchMessage struct {
		ID         string       `json:"id"`
		Body       string       `json:"body"`
		Tag        string       `json:"tag,omitempty"`
		Retry      int          `json:"retry"`
		Timestamp  FlexibleUnix `json:"timestamp"`
		QueueID    int          `json:"queueId"`
		Offset     int64        `json:"offset"`
		NextOffset int64        `json:"nextOffset"`
	}
	MessageStatus struct {
		ID          string        `json:"id"`
		Body        string        `json:"body"`
		Tag         string        `json:"tag,omitempty"`
		Retry       int           `json:"retry"`
		Timestamp   FlexibleUnix  `json:"timestamp"`
		ScheduledAt *FlexibleUnix `json:"scheduledAt,omitempty"`
		ConsumedAt  *FlexibleUnix `json:"consumedAt,omitempty"`
		AckedAt     *FlexibleUnix `json:"ackedAt,omitempty"`
		QueueID     *int          `json:"queueId,omitempty"`
		Offset      *int64        `json:"offset,omitempty"`
		NextOffset  *int64        `json:"nextOffset,omitempty"`
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

// Stats types

type (
	StatsResponse struct {
		Topics         []TopicStatsResponse         `json:"topics"`
		ConsumerGroups []ConsumerGroupStatsResponse `json:"consumerGroups"`
		DelayScheduler map[string]interface{}       `json:"delayScheduler,omitempty"`
		Total          int64                        `json:"total"`
		Completed      int64                        `json:"completed"`
		Processing     int64                        `json:"processing"`
		Pending        int64                        `json:"pending"`
		Timestamp      string                       `json:"timestamp"`
	}
	TopicStatsResponse struct {
		Name       string           `json:"name"`
		Type       broker.TopicType `json:"type"`
		QueueCount int              `json:"queueCount"`
		Total      int64            `json:"total"`
		CreatedAt  int64            `json:"createdAt"`
	}
	ConsumerGroupStatsResponse struct {
		Group      string                            `json:"group"`
		Total      int64                             `json:"total"`
		Completed  int64                             `json:"completed"`
		Processing int64                             `json:"processing"`
		Pending    int64                             `json:"pending"`
		Topics     []ConsumerGroupTopicStatsResponse `json:"topics"`
	}
	ConsumerGroupTopicStatsResponse struct {
		Topic      string `json:"topic"`
		Total      int64  `json:"total"`
		Completed  int64  `json:"completed"`
		Processing int64  `json:"processing"`
		Pending    int64  `json:"pending"`
	}
	TopicDetailStatsResponse struct {
		Name       string                       `json:"name"`
		Type       broker.TopicType             `json:"type"`
		QueueCount int                          `json:"queueCount"`
		Total      int64                        `json:"total"`
		Consumers  []TopicConsumerStatsResponse `json:"consumers"`
		CreatedAt  int64                        `json:"createdAt"`
		Timestamp  string                       `json:"timestamp"`
	}
	TopicConsumerStatsResponse struct {
		Group      string `json:"group"`
		Completed  int64  `json:"completed"`
		Processing int64  `json:"processing"`
		Pending    int64  `json:"pending"`
	}
)
