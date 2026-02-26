package api

import (
	"fmt"
	"github.com/SpectatorNan/messageQ/go/mq/errx"
	"github.com/SpectatorNan/messageQ/go/mq/respx"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/SpectatorNan/messageQ/go/mq/broker"
	"github.com/SpectatorNan/messageQ/go/mq/logger"
	"github.com/SpectatorNan/messageQ/go/mq/queue"
	"github.com/SpectatorNan/messageQ/go/mq/storage"
	client "github.com/SpectatorNan/messageQ/go/sdk"

	"github.com/gin-gonic/gin"
)

const (
	maxDelaySeconds = 86400 * 30
	maxDelayMillis  = 86400000 * 30
)

var consumeQueueRR = struct {
	mu   sync.Mutex
	next map[string]int
}{
	next: make(map[string]int),
}

func consumeRRStart(group string, topic string, tag string, queueCount int) int {
	if queueCount <= 0 {
		return 0
	}
	tagKey := tag
	if tagKey == "" {
		tagKey = "__all__"
	}
	key := fmt.Sprintf("%s|%s|%s", group, topic, tagKey)
	consumeQueueRR.mu.Lock()
	start := consumeQueueRR.next[key] % queueCount
	consumeQueueRR.mu.Unlock()
	if start < 0 {
		return 0
	}
	return start
}

func consumeRRAdvance(group string, topic string, tag string, queueCount int, next int) {
	if queueCount <= 0 {
		return
	}
	tagKey := tag
	if tagKey == "" {
		tagKey = "__all__"
	}
	key := fmt.Sprintf("%s|%s|%s", group, topic, tagKey)
	consumeQueueRR.mu.Lock()
	consumeQueueRR.next[key] = next % queueCount
	consumeQueueRR.mu.Unlock()
}

func resolveDelay(delayMs int64, delaySec int64, scheduledAt *client.FlexibleUnix) (time.Duration, error) {
	if scheduledAt != nil {
		if delayMs != 0 || delaySec != 0 {
			return 0, errx.ErrDelayConflict
		}
		scheduledUnix := int64(*scheduledAt)
		if scheduledUnix <= 0 {
			return 0, errx.ErrScheduledAtInvalid
		}
		secUntil := scheduledUnix - time.Now().Unix()
		if secUntil < 1 || secUntil > maxDelaySeconds {
			return 0, errx.ErrScheduledAtInvalid
		}
		return time.Duration(secUntil) * time.Second, nil
	}
	if delayMs < 0 || delaySec < 0 {
		return 0, errx.ErrDelayNonPositive
	}
	if delayMs > 0 && delaySec > 0 {
		return 0, errx.ErrDelayBothSet
	}
	if delayMs == 0 && delaySec == 0 {
		delaySec = 1
	}
	if delayMs > 0 {
		if delayMs > maxDelayMillis {
			return 0, errx.ErrDelayTooLarge
		}
		return time.Duration(delayMs) * time.Millisecond, nil
	}
	if delaySec > maxDelaySeconds {
		return 0, errx.ErrDelayTooLarge
	}
	return time.Duration(delaySec) * time.Second, nil
}

// Handler constructors that accept a broker and return gin.HandlerFunc.

func ProduceHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req ProduceMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		// Check if topic exists
		topicConfig, err := b.GetTopicConfig(req.Topic)
		if err != nil {
			logger.Warn("Topic not found", zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		// Check if this is a delay topic and if delay parameters are provided
		isDelayTopic := topicConfig.Type == broker.TopicTypeDelay

		if isDelayTopic {
			delay, err := resolveDelay(req.DelayMs, req.DelaySec, req.ScheduledAt)
			if err != nil {
				respx.FailGin(c, err)
				return
			}

			msg := b.EnqueueWithDelay(req.Topic, req.Body, req.Tag, delay)
			logger.Info("Delayed message produced",
				zap.String("topic", req.Topic),
				zap.String("message_id", msg.ID),
				zap.String("tag", req.Tag),
				zap.Duration("delay", delay))

			resp := ProduceMessageResponse{
				ID:          msg.ID,
				Topic:       req.Topic,
				Tag:         msg.Tag,
				Body:        msg.Body,
				Timestamp:   msg.Timestamp.Unix(),
				Retry:       msg.Retry,
				ScheduledAt: msg.Timestamp.Add(delay).Unix(),
				ExecutedAt:  nil,
			}

			c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
			return
		}

		// Produce normal message
		msg := b.Enqueue(req.Topic, req.Body, req.Tag)
		logger.Info("Message produced",
			zap.String("topic", req.Topic),
			zap.String("message_id", msg.ID),
			zap.String("tag", req.Tag))

		resp := ProduceMessageResponse{
			ID:          msg.ID,
			Topic:       req.Topic,
			Tag:         msg.Tag,
			Body:        msg.Body,
			Timestamp:   msg.Timestamp.Unix(),
			Retry:       msg.Retry,
			ScheduledAt: msg.Timestamp.Unix(),
			ExecutedAt:  nil,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// ProduceBatchHandler handles batch message production.
func ProduceBatchHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ProduceBatchRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := validateTopicName(req.Topic); err != nil {
			respx.FailGin(c, err)
			return
		}
		if len(req.Messages) == 0 {
			respx.FailGin(c, errx.ErrInvalidMessage)
			return
		}

		// Check if topic exists
		topicConfig, err := b.GetTopicConfig(req.Topic)
		if err != nil {
			logger.Warn("Topic not found", zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		isDelayTopic := topicConfig.Type == broker.TopicTypeDelay
		responses := make([]ProduceMessageResponse, 0, len(req.Messages))

		if isDelayTopic {
			items := make([]broker.DelayEnqueueItem, 0, len(req.Messages))
			for _, item := range req.Messages {
				body := strings.TrimSpace(item.Body)
				tag := strings.TrimSpace(item.Tag)
				if body == "" || tag == "" {
					respx.FailGin(c, errx.ErrInvalidMessage)
					return
				}

				delayMs := item.DelayMs
				delaySec := item.DelaySec
				if delayMs == 0 && req.DelayMs > 0 {
					delayMs = req.DelayMs
				}
				if delaySec == 0 && req.DelaySec > 0 {
					delaySec = req.DelaySec
				}

				delay, err := resolveDelay(delayMs, delaySec, item.ScheduledAt)
				if err != nil {
					respx.FailGin(c, err)
					return
				}

				items = append(items, broker.DelayEnqueueItem{
					Body:  body,
					Tag:   tag,
					Delay: delay,
				})
			}

			msgs := b.EnqueueWithDelayBatch(req.Topic, items)
			for i, msg := range msgs {
				delay := items[i].Delay
				responses = append(responses, ProduceMessageResponse{
					ID:          msg.ID,
					Topic:       req.Topic,
					Tag:         msg.Tag,
					Body:        msg.Body,
					Timestamp:   msg.Timestamp.Unix(),
					Retry:       msg.Retry,
					ScheduledAt: msg.Timestamp.Add(delay).Unix(),
					ExecutedAt:  nil,
				})
			}

			c.JSON(http.StatusOK, respx.NewRespSuccess(ProduceBatchResponse{Messages: responses}))
			return
		}

		items := make([]queue.Message, 0, len(req.Messages))
		for _, item := range req.Messages {
			body := strings.TrimSpace(item.Body)
			tag := strings.TrimSpace(item.Tag)
			if body == "" || tag == "" {
				respx.FailGin(c, errx.ErrInvalidMessage)
				return
			}
			items = append(items, queue.Message{Body: body, Tag: tag})
		}

		msgs := b.EnqueueBatch(req.Topic, items)
		for _, msg := range msgs {
			responses = append(responses, ProduceMessageResponse{
				ID:          msg.ID,
				Topic:       req.Topic,
				Tag:         msg.Tag,
				Body:        msg.Body,
				Timestamp:   msg.Timestamp.Unix(),
				Retry:       msg.Retry,
				ScheduledAt: msg.Timestamp.Unix(),
				ExecutedAt:  nil,
			})
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(ProduceBatchResponse{Messages: responses}))
	}
}

func ConsumeHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req ConsumeMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindQuery(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		if _, err := b.GetTopicConfig(req.Topic); err != nil {
			logger.Warn("Topic not found for consumption", zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		tag := req.Tag
		var msgs []storage.Message
		var offset, next int64
		var queueID int
		var err error

		if req.QueueId != nil {

			queueID = *req.QueueId
			msgs, offset, next, err = b.ConsumeWithRetry(req.GroupName, req.Topic, queueID, tag, 1)
			if err != nil {
				logger.Error("Consume error", zap.String("group", req.GroupName), zap.String("topic", req.Topic), zap.Error(err))
				respx.FailGin(c, errx.ErrOffsetUnsupported)
				return
			}
		} else {
			// 没有指定 queue_id，轮询所有队列
			queueCount := b.GetQueueCount(req.Topic)
			start := consumeRRStart(req.GroupName, req.Topic, tag, queueCount)
			for i := 0; i < queueCount; i++ {
				idx := (start + i) % queueCount
				msgs, offset, next, err = b.ConsumeWithRetry(req.GroupName, req.Topic, idx, tag, 1)
				if err != nil {
					continue // 跳过出错的队列
				}
				if len(msgs) > 0 {
					queueID = idx
					break
				}
			}
			if queueCount > 0 {
				nextIdx := start + 1
				if len(msgs) > 0 {
					nextIdx = queueID + 1
				}
				consumeRRAdvance(req.GroupName, req.Topic, tag, queueCount, nextIdx)
			}
		}

		if len(msgs) == 0 {
			logger.Debug("No messages available", zap.String("group", req.GroupName), zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		msg := msgs[0]
		b.BeginProcessing(req.GroupName, req.Topic, queueID, offset, next, queue.Message{
			ID:        msg.ID,
			Body:      msg.Body,
			Tag:       msg.Tag,
			Retry:     msg.Retry,
			Timestamp: msg.Timestamp,
		})

		logger.Info("Message consumed",
			zap.String("group", req.GroupName),
			zap.String("topic", req.Topic),
			zap.String("message_id", msg.ID),
			zap.Int("queue_id", queueID),
			zap.Int64("offset", offset))

		resp := ConsumeMessageResponse{
			Message: ConsumeMessage{
				ID:        msg.ID,
				Body:      msg.Body,
				Tag:       msg.Tag,
				Retry:     msg.Retry,
				Timestamp: msg.Timestamp.Unix(),
			},
			Group:      req.GroupName,
			Topic:      req.Topic,
			QueueID:    queueID,
			Offset:     offset,
			NextOffset: next,
			State:      "processing",
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// ConsumeBatchHandler returns up to max messages for a group/topic.
func ConsumeBatchHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ConsumeBatchRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindQuery(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		if _, err := b.GetTopicConfig(req.Topic); err != nil {
			logger.Warn("Topic not found for consumption", zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		max := 10
		if req.Max != nil {
			max = *req.Max
		}
		if max <= 0 {
			respx.FailGin(c, errx.ErrInvalidMessage)
			return
		}
		if max > 100 {
			max = 100
		}

		tag := req.Tag
		var msgs []storage.Message
		var offset int64
		var queueID int
		var err error
		out := make([]ConsumeBatchMessage, 0, max)
		batchByQueue := make([]queue.Message, 0, max)

		if req.QueueId != nil {
			queueID = *req.QueueId
			msgs, offset, _, err = b.ConsumeWithRetry(req.GroupName, req.Topic, queueID, tag, max)
			if err != nil {
				logger.Error("Consume batch error", zap.String("group", req.GroupName), zap.String("topic", req.Topic), zap.Error(err))
				respx.FailGin(c, errx.ErrOffsetUnsupported)
				return
			}
			for i, msg := range msgs {
				batchByQueue = append(batchByQueue, queue.Message{
					ID:        msg.ID,
					Body:      msg.Body,
					Tag:       msg.Tag,
					Retry:     msg.Retry,
					Timestamp: msg.Timestamp,
				})
				out = append(out, ConsumeBatchMessage{
					ID:         msg.ID,
					Body:       msg.Body,
					Tag:        msg.Tag,
					Retry:      msg.Retry,
					Timestamp:  msg.Timestamp.Unix(),
					QueueID:    queueID,
					Offset:     offset + int64(i),
					NextOffset: offset + int64(i) + 1,
				})
			}
			b.BeginProcessingBatch(req.GroupName, req.Topic, queueID, offset, batchByQueue)
		} else {
			queueCount := b.GetQueueCount(req.Topic)
			start := consumeRRStart(req.GroupName, req.Topic, tag, queueCount)
			remaining := max
			lastIdx := start
			for i := 0; i < queueCount && remaining > 0; i++ {
				idx := (start + i) % queueCount
				lastIdx = idx
				msgs, offset, _, err = b.ConsumeWithRetry(req.GroupName, req.Topic, idx, tag, remaining)
				if err != nil {
					continue
				}
				if len(msgs) == 0 {
					continue
				}
				batchByQueue = batchByQueue[:0]
				for j, msg := range msgs {
					batchByQueue = append(batchByQueue, queue.Message{
						ID:        msg.ID,
						Body:      msg.Body,
						Tag:       msg.Tag,
						Retry:     msg.Retry,
						Timestamp: msg.Timestamp,
					})
					out = append(out, ConsumeBatchMessage{
						ID:         msg.ID,
						Body:       msg.Body,
						Tag:        msg.Tag,
						Retry:      msg.Retry,
						Timestamp:  msg.Timestamp.Unix(),
						QueueID:    idx,
						Offset:     offset + int64(j),
						NextOffset: offset + int64(j) + 1,
					})
				}
				b.BeginProcessingBatch(req.GroupName, req.Topic, idx, offset, batchByQueue)
				remaining = max - len(out)
			}
			if queueCount > 0 {
				nextIdx := lastIdx + 1
				consumeRRAdvance(req.GroupName, req.Topic, tag, queueCount, nextIdx)
			}
		}

		if len(out) == 0 {
			logger.Debug("No messages available", zap.String("group", req.GroupName), zap.String("topic", req.Topic))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		resp := ConsumeBatchResponse{
			Messages: out,
			Group:    req.GroupName,
			Topic:    req.Topic,
			State:    "processing",
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// ListMessagesHandler returns processing or acked message list for a group/topic.
func ListMessagesHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req ListMessagesRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindQuery(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}
		if req.State == "" {
			req.State = "processing"
		}
		limit := 50
		if req.Limit != nil {
			limit = *req.Limit
		}
		if limit > 500 {
			limit = 500
		}

		resp := ListMessagesResponse{
			Group: req.GroupName,
			Topic: req.Topic,
			State: req.State,
		}

		switch req.State {
		case "pending":
			queueID := *req.QueueId
			msgs, nextCursor, err := b.ListPending(req.GroupName, req.Topic, queueID, req.Cursor, limit, req.Tag)
			if err != nil {
				respx.FailGin(c, errx.ErrOffsetUnsupported)
				return
			}
			resp.State = "pending"
			out := make([]MessageStatus, 0, len(msgs))
			for _, msg := range msgs {
				qid := queueID
				off := msg.Offset
				out = append(out, MessageStatus{
					ID:        msg.ID,
					Body:      msg.Body,
					Tag:       msg.Tag,
					Retry:     msg.Retry,
					Timestamp: msg.Timestamp.Unix(),
					QueueID:   &qid,
					Offset:    &off,
				})
			}
			resp.Messages = out
			resp.NextCursor = &nextCursor
		case "scheduled":
			var next *int64
			queueID := req.QueueId
			var cursor int64
			if req.Cursor != nil {
				cursor = *req.Cursor
			}
			if ds := b.GetDelayScheduler(); ds != nil {
				items, nc := ds.ListScheduled(req.Topic, queueID, cursor, limit)
				next = nc
				resp.State = "scheduled"
				out := make([]MessageStatus, 0, len(items))
				for _, item := range items {
					qid := item.QueueID
					scheduledAt := item.ExecuteAt.Unix()
					out = append(out, MessageStatus{
						ID:          item.Message.ID,
						Body:        item.Message.Body,
						Tag:         item.Message.Tag,
						Retry:       item.Message.Retry,
						Timestamp:   item.Message.Timestamp.Unix(),
						ScheduledAt: &scheduledAt,
						QueueID:     &qid,
					})
				}
				resp.Messages = out
				resp.NextCursor = next
			} else {
				respx.FailGin(c, errx.ErrNotFound)
				return
			}
		case "processing":
			entries := b.ListProcessing(req.GroupName, req.Topic, limit)
			if req.Tag != "" {
				filtered := entries[:0]
				for _, entry := range entries {
					if entry.Tag == req.Tag {
						filtered = append(filtered, entry)
					}
				}
				entries = filtered
			}
			cursor := int64(0)
			if req.Cursor != nil {
				cursor = *req.Cursor
			}
			start := int(cursor)
			if start < 0 {
				start = 0
			}
			if start > len(entries) {
				start = len(entries)
			}
			end := start + limit
			if end > len(entries) {
				end = len(entries)
			}
			page := entries[start:end]
			if end < len(entries) {
				nc := int64(end)
				resp.NextCursor = &nc
			}
			msgs := make([]MessageStatus, 0, len(entries))
			for _, entry := range page {
				consumedAt := entry.ConsumedAt.Unix()
				qid := entry.QueueID
				off := entry.Offset
				next := entry.NextOffset
				msgs = append(msgs, MessageStatus{
					ID:         entry.MsgID,
					Body:       entry.Body,
					Tag:        entry.Tag,
					Retry:      entry.Retry,
					Timestamp:  entry.Timestamp.Unix(),
					ConsumedAt: &consumedAt,
					AckedAt:    nil,
					QueueID:    &qid,
					Offset:     &off,
					NextOffset: &next,
				})
			}
			resp.Messages = msgs
		case "acked", "completed":
			entries := b.ListCompleted(req.GroupName, req.Topic, limit)
			if req.Tag != "" {
				filtered := entries[:0]
				for _, entry := range entries {
					if entry.Tag == req.Tag {
						filtered = append(filtered, entry)
					}
				}
				entries = filtered
			}
			cursor := int64(0)
			if req.Cursor != nil {
				cursor = *req.Cursor
			}
			start := int(cursor)
			if start < 0 {
				start = 0
			}
			if start > len(entries) {
				start = len(entries)
			}
			end := start + limit
			if end > len(entries) {
				end = len(entries)
			}
			page := entries[start:end]
			if end < len(entries) {
				nc := int64(end)
				resp.NextCursor = &nc
			}
			resp.State = "completed"
			msgs := make([]MessageStatus, 0, len(entries))
			for _, entry := range page {
				consumedAt := entry.ConsumedAt.Unix()
				ackedAt := entry.AckedAt.Unix()
				qid := entry.QueueID
				off := entry.Offset
				next := entry.NextOffset
				msgs = append(msgs, MessageStatus{
					ID:         entry.MsgID,
					Body:       entry.Body,
					Tag:        entry.Tag,
					Retry:      entry.Retry,
					Timestamp:  entry.Timestamp.Unix(),
					ConsumedAt: &consumedAt,
					AckedAt:    &ackedAt,
					QueueID:    &qid,
					Offset:     &off,
					NextOffset: &next,
				})
			}
			resp.Messages = msgs
		default:
			respx.FailGin(c, errx.ErrInvalidMessage)
			return
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

func AckHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req AckMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		if !b.ValidateProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Ack failed - message mismatch", zap.String("message_id", req.ID))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		if !b.CompleteProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Ack failed - message not found", zap.String("message_id", req.ID))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		logger.Info("Message acknowledged",
			zap.String("message_id", req.ID),
			zap.String("group", req.GroupName),
			zap.String("topic", req.Topic))

		resp := AckResponse{
			MessageID: req.ID,
			Acked:     true,
			Topic:     req.Topic,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

func NackHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req NackMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		if !b.ValidateProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Nack failed - message mismatch", zap.String("message_id", req.ID))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		if !b.RetryProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Nack failed - message not found", zap.String("message_id", req.ID))
			respx.FailGin(c, errx.ErrNotFound)
			return
		}

		logger.Info("Message nacked for retry",
			zap.String("message_id", req.ID),
			zap.String("group", req.GroupName),
			zap.String("topic", req.Topic))

		resp := NackResponse{
			MessageID: req.ID,
			Nacked:    true,
			Topic:     req.Topic,
			Requeued:  true,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// StatsHandler returns processing statistics per group.
func StatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		c.JSON(http.StatusOK, respx.NewRespSuccess(stats))
	}
}

// GetOffsetHandler returns the committed offset for a group/topic/queue.
func GetOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req GetOffsetRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := c.ShouldBindQuery(&req); err != nil {
			respx.FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		// Check if topic exists
		if _, err := b.GetTopicConfig(req.Topic); err != nil {
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}
		queueCount := b.GetQueueCount(req.Topic)

		queueID := 0
		if req.QueueID != nil {
			queueID = *req.QueueID
		}
		if queueID < 0 || queueID >= queueCount {
			respx.FailGin(c, errx.ErrInvalidQueueID)
			return
		}

		offset, ok, err := b.GetOffset(req.GroupName, req.Topic, queueID)
		if err != nil {
			respx.FailGin(c, errx.ErrOffsetUnsupported)
			return
		}

		resp := OffsetResponse{
			Group:   req.GroupName,
			Topic:   req.Topic,
			QueueID: queueID,
			Offset:  nil,
		}

		if ok {
			resp.Offset = &offset
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// CommitOffsetHandler commits the offset for a group/topic/queue.
func CommitOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CommitOffsetRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		// Check if topic exists
		if err := c.ShouldBindJSON(&req); err != nil {
			respx.FailGin(c, errx.ErrInvalidOffset)
			return
		}

		if err := req.Validate(); err != nil {
			respx.FailGin(c, err)
			return
		}

		if _, err := b.GetTopicConfig(req.Topic); err != nil {
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}
		queueCount := b.GetQueueCount(req.Topic)
		if req.QueueID >= queueCount {
			respx.FailGin(c, errx.ErrInvalidQueueID)
			return
		}

		if err := b.CommitOffset(req.GroupName, req.Topic, req.QueueID, req.Offset); err != nil {
			respx.FailGin(c, errx.ErrOffsetUnsupported)
			return
		}

		resp := CommitOffsetResponse{
			Group:     req.GroupName,
			Topic:     req.Topic,
			QueueID:   req.QueueID,
			Offset:    req.Offset,
			Committed: true,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// helper to use api.Fail behavior but for gin.Context

// Validation helpers
