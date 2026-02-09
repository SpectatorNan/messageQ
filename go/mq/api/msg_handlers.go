package api

import (
	"messageQ/mq/errx"
	"messageQ/mq/respx"
	"net/http"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/queue"
	"messageQ/mq/storage"

	"github.com/gin-gonic/gin"
)

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

		err := req.Validate()
		if err != nil {
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
			hasDelay := req.DelayMs > 0 || req.DelaySec > 0

			// Validate delay parameters
			if req.DelayMs > 0 && req.DelaySec > 0 {
				// Can't specify both
				respx.FailGin(c, errx.ErrInvalidDelay)
				return
			}

			if !hasDelay {
				// Use default delay of 1 second for delay topic
				req.DelaySec = 1
			}

			var delay time.Duration
			if req.DelayMs > 0 {
				if req.DelayMs < 0 || req.DelayMs > 86400000*30 { // max 30 days
					respx.FailGin(c, errx.ErrInvalidDelay)
					return
				}
				delay = time.Duration(req.DelayMs) * time.Millisecond
			} else {
				if req.DelaySec < 1 || req.DelaySec > 86400*30 { // max 30 days, min 1 second
					respx.FailGin(c, errx.ErrInvalidDelay)
					return
				}
				delay = time.Duration(req.DelaySec) * time.Second
			}

			// Produce delayed message
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

		err := req.Validate()
		if err != nil {
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
			for i := 0; i < queueCount; i++ {
				msgs, offset, next, err = b.ConsumeWithRetry(req.GroupName, req.Topic, i, tag, 1)
				if err != nil {
					continue // 跳过出错的队列
				}
				if len(msgs) > 0 {
					queueID = i
					break
				}
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
