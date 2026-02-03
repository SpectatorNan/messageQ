package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/queue"
	"messageQ/mq/storage"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// Handler constructors that accept a broker and return gin.HandlerFunc.

func ProduceHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}

		// Validate topic name
		if err := validateTopicName(topic); err != nil {
			logger.Warn("Invalid topic name", zap.String("topic", topic))
			FailGin(c, err)
			return
		}

		// Check if topic exists
		topicConfig, err := b.GetTopicConfig(topic)
		if err != nil {
			logger.Warn("Topic not found", zap.String("topic", topic))
			FailGin(c, ErrTopicNotFound)
			return
		}

		var payload struct {
			Body     string `json:"body" binding:"required"`
			Tag      string `json:"tag" binding:"required"`
			DelayMs  int64  `json:"delay_ms"`  // optional: delay in milliseconds
			DelaySec int64  `json:"delay_sec"` // optional: delay in seconds
		}

		if err := c.ShouldBindJSON(&payload); err != nil {
			logger.Warn("Invalid message payload", zap.Error(err))
			FailGin(c, ErrInvalidMessage)
			return
		}

		// Validate body not empty after trim
		if strings.TrimSpace(payload.Body) == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}

		// Check if this is a delay topic and if delay parameters are provided
		isDelayTopic := topicConfig.Type == broker.TopicTypeDelay
		 
		if isDelayTopic {
			hasDelay := payload.DelayMs > 0 || payload.DelaySec > 0
			
			// Validate delay parameters
			if payload.DelayMs > 0 && payload.DelaySec > 0 {
				// Can't specify both
				FailGin(c, ErrInvalidDelay)
				return
			}
			
			if !hasDelay {
				// Use default delay of 1 second for delay topic
				payload.DelaySec = 1 
			}
			
			var delay time.Duration
			if payload.DelayMs > 0 {
				if payload.DelayMs < 0 || payload.DelayMs > 86400000*30 { // max 30 days
					FailGin(c, ErrInvalidDelay)
					return
				}
				delay = time.Duration(payload.DelayMs) * time.Millisecond
			} else {
				if payload.DelaySec < 1 || payload.DelaySec > 86400*30 { // max 30 days, min 1 second
					FailGin(c, ErrInvalidDelay)
					return
				}
				delay = time.Duration(payload.DelaySec) * time.Second
			}

			// Produce delayed message
			msg := b.EnqueueWithDelay(topic, payload.Body, payload.Tag, delay)
			logger.Info("Delayed message produced",
				zap.String("topic", topic),
				zap.String("message_id", msg.ID),
				zap.String("tag", payload.Tag),
				zap.Duration("delay", delay))

			resp := ProduceDelayResponse{
				ID:           msg.ID,
				Topic:        topic,
				Tag:          msg.Tag,
				ScheduledAt:  msg.Timestamp,
				ExecuteAt:    msg.Timestamp.Add(delay),
				DelaySeconds: delay.Seconds(),
				DelayMs:      delay.Milliseconds(),
			}

			c.JSON(http.StatusOK, NewRespSuccess(resp))
			return
		}

		// Produce normal message
		msg := b.Enqueue(topic, payload.Body, payload.Tag)
		logger.Info("Message produced",
			zap.String("topic", topic),
			zap.String("message_id", msg.ID),
			zap.String("tag", payload.Tag))

		resp := ProduceResponse{
			ID:        msg.ID,
			Topic:     topic,
			Tag:       msg.Tag,
			Body:      msg.Body,
			Timestamp: msg.Timestamp,
			Retry:     msg.Retry,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

func ConsumeHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}

		// Validate topic name
		if err := validateTopicName(topic); err != nil {
			FailGin(c, err)
			return
		}

		// Check if topic exists
		if _, err := b.GetTopicConfig(topic); err != nil {
			logger.Warn("Topic not found for consumption", zap.String("topic", topic))
			FailGin(c, ErrTopicNotFound)
			return
		}

		// Get group from URL parameter
		group := c.Param("group")
		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}

		// Validate group name
		if err := validateGroupName(group); err != nil {
			FailGin(c, err)
			return
		}

		tag := c.Query("tag")
		
		// 如果指定了 queue_id，直接使用
		var msgs []storage.Message
		var offset, next int64
		var err error
		var queueID int
		
		if q := c.Query("queue_id"); q != "" {
			v, parseErr := strconv.Atoi(q)
			if parseErr != nil || v < 0 {
				FailGin(c, ErrInvalidQueueID)
				return
			}
			queueID = v
			// 使用指定的 queue_id
			msgs, offset, next, err = b.ConsumeWithLock(group, topic, queueID, tag, 1)
			if err != nil {
				logger.Error("Consume error", zap.String("group", group), zap.String("topic", topic), zap.Error(err))
				FailGin(c, ErrOffsetUnsupported)
				return
			}
		} else {
			// 没有指定 queue_id，轮询所有队列
			queueCount := b.GetQueueCount(topic)
			for i := 0; i < queueCount; i++ {
				msgs, offset, next, err = b.ConsumeWithLock(group, topic, i, tag, 1)
				if err != nil {
					continue // 跳过出错的队列
				}
				if len(msgs) > 0 {
					queueID = i
					break // 找到消息，退出循环
				}
			}
		}
		
		if len(msgs) == 0 {
			logger.Debug("No messages available", zap.String("group", group), zap.String("topic", topic))
			FailGin(c, ErrNotFound)
			return
		}

		msg := msgs[0]
		b.BeginProcessing(group, topic, queueID, offset, next, queue.Message{
			ID:        msg.ID,
			Body:      msg.Body,
			Tag:       msg.Tag,
			Retry:     msg.Retry,
			Timestamp: msg.Timestamp,
		})

		logger.Info("Message consumed",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.String("message_id", msg.ID),
			zap.Int("queue_id", queueID),
			zap.Int64("offset", offset))

		resp := ConsumeResponse{
			Message:    msg,
			Group:      group,
			Topic:      topic,
			QueueID:    queueID,
			Offset:     offset,
			NextOffset: next,
			State:      "processing",
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

func AckHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			FailGin(c, ErrInvalidID)
			return
		}

		// Validate UUID format
		if _, err := uuid.Parse(id); err != nil {
			logger.Warn("Invalid message ID format", zap.String("message_id", id), zap.Error(err))
			FailGin(c, ErrInvalidID)
			return
		}

		if !b.CompleteProcessing(id) {
			logger.Error("Ack failed - message not found", zap.String("message_id", id))
			FailGin(c, ErrNotFound)
			return
		}

		logger.Info("Message acknowledged", zap.String("message_id", id))

		resp := AckResponse{
			MessageID: id,
			Acked:     true,
			Topic:     "", // topic not required for global message ack
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

func NackHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		if id == "" {
			FailGin(c, ErrInvalidID)
			return
		}

		// Validate UUID format
		if _, err := uuid.Parse(id); err != nil {
			logger.Warn("Invalid message ID format for nack", zap.String("message_id", id), zap.Error(err))
			FailGin(c, ErrInvalidID)
			return
		}

		if !b.RetryProcessing(id) {
			logger.Error("Nack failed - message not found", zap.String("message_id", id))
			FailGin(c, ErrNotFound)
			return
		}

		logger.Info("Message nacked for retry", zap.String("message_id", id))

		resp := NackResponse{
			MessageID: id,
			Nacked:    true,
			Topic:     "", // topic not required for global message nack
			Requeued:  true,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// StatsHandler returns processing statistics per group.
func StatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		c.JSON(http.StatusOK, NewRespSuccess(stats))
	}
}

// GetOffsetHandler returns the committed offset for a group/topic/queue.
func GetOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		group := c.Param("group")

		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}

		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}

		// Validate names
		if err := validateTopicName(topic); err != nil {
			FailGin(c, err)
			return
		}

		if err := validateGroupName(group); err != nil {
			FailGin(c, err)
			return
		}

		// Check if topic exists
		if _, err := b.GetTopicConfig(topic); err != nil {
			FailGin(c, ErrTopicNotFound)
			return
		}

		queueID := 0
		if q := c.Query("queue_id"); q != "" {
			v, err := strconv.Atoi(q)
			if err != nil || v < 0 {
				FailGin(c, ErrInvalidQueueID)
				return
			}
			queueID = v
		}

		offset, ok, err := b.GetOffset(group, topic, queueID)
		if err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}

		resp := OffsetResponse{
			Group:   group,
			Topic:   topic,
			QueueID: queueID,
			Offset:  nil,
		}

		if ok {
			resp.Offset = &offset
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// CommitOffsetHandler commits the offset for a group/topic/queue.
func CommitOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		group := c.Param("group")

		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}

		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}

		// Validate names
		if err := validateTopicName(topic); err != nil {
			FailGin(c, err)
			return
		}

		if err := validateGroupName(group); err != nil {
			FailGin(c, err)
			return
		}

		// Check if topic exists
		if _, err := b.GetTopicConfig(topic); err != nil {
			FailGin(c, ErrTopicNotFound)
			return
		}

		var payload struct {
			QueueID int   `json:"queue_id"`
			Offset  int64 `json:"offset" binding:"required"`
		}

		if err := c.ShouldBindJSON(&payload); err != nil {
			FailGin(c, ErrInvalidOffset)
			return
		}

		if payload.Offset < 0 {
			FailGin(c, ErrInvalidOffset)
			return
		}

		if payload.QueueID < 0 {
			FailGin(c, ErrInvalidQueueID)
			return
		}

		if err := b.CommitOffset(group, topic, payload.QueueID, payload.Offset); err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}

		resp := CommitOffsetResponse{
			Group:     group,
			Topic:     topic,
			QueueID:   payload.QueueID,
			Offset:    payload.Offset,
			Committed: true,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// helper to use api.Fail behavior but for gin.Context
func FailGin(c *gin.Context, err error) {
	// if RespErr, use its code/message
	if re, ok := err.(RespErr); ok {
		// Determine HTTP status code based on error type
		statusCode := http.StatusBadRequest
		switch re.Code {
		case ErrCodeNotFound, ErrCodeTopicNotFound:
			statusCode = http.StatusNotFound
		}
		c.JSON(statusCode, NewRespFail(re.Code, re.Message))
		return
	}
	c.JSON(http.StatusBadRequest, NewRespFail("error", err.Error()))
}

// Validation helpers

func validateTopicName(topic string) error {
	if topic == "" {
		return ErrMissingTopic
	}
	if len(topic) > 255 {
		return ErrInvalidTopicName
	}
	// Topic name should not contain special characters
	if strings.ContainsAny(topic, " \t\n\r/\\") {
		return ErrInvalidTopicName
	}
	return nil
}

func validateGroupName(group string) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if len(group) > 255 {
		return ErrInvalidGroup
	}
	// Group name should not contain special characters
	if strings.ContainsAny(group, " \t\n\r/\\") {
		return ErrInvalidGroup
	}
	return nil
}
