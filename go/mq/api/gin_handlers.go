package api

import (
	"net/http"
	"strconv"
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
			FailGin(c, err)
			return
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			FailGin(c, err)
			return
		}

		// Check if topic exists
		topicConfig, err := b.GetTopicConfig(req.Topic)
		if err != nil {
			logger.Warn("Topic not found", zap.String("topic", req.Topic))
			FailGin(c, ErrTopicNotFound)
			return
		}

		// Check if this is a delay topic and if delay parameters are provided
		isDelayTopic := topicConfig.Type == broker.TopicTypeDelay

		if isDelayTopic {
			hasDelay := req.DelayMs > 0 || req.DelaySec > 0

			// Validate delay parameters
			if req.DelayMs > 0 && req.DelaySec > 0 {
				// Can't specify both
				FailGin(c, ErrInvalidDelay)
				return
			}

			if !hasDelay {
				// Use default delay of 1 second for delay topic
				req.DelaySec = 1
			}

			var delay time.Duration
			if req.DelayMs > 0 {
				if req.DelayMs < 0 || req.DelayMs > 86400000*30 { // max 30 days
					FailGin(c, ErrInvalidDelay)
					return
				}
				delay = time.Duration(req.DelayMs) * time.Millisecond
			} else {
				if req.DelaySec < 1 || req.DelaySec > 86400*30 { // max 30 days, min 1 second
					FailGin(c, ErrInvalidDelay)
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

			resp := ProduceDelayResponse{
				ID:           msg.ID,
				Topic:        req.Topic,
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
		msg := b.Enqueue(req.Topic, req.Body, req.Tag)
		logger.Info("Message produced",
			zap.String("topic", req.Topic),
			zap.String("message_id", msg.ID),
			zap.String("tag", req.Tag))

		resp := ProduceResponse{
			ID:        msg.ID,
			Topic:     req.Topic,
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

		var req ConsumeMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			FailGin(c, err)
			return
		}
		if err := c.ShouldBindJSON(&req); err != nil {
			FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			FailGin(c, err)
			return
		}

		if _, err := b.GetTopicConfig(req.Topic); err != nil {
			logger.Warn("Topic not found for consumption", zap.String("topic", req.Topic))
			FailGin(c, ErrTopicNotFound)
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
				FailGin(c, ErrOffsetUnsupported)
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
			FailGin(c, ErrNotFound)
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
				Timestamp: msg.Timestamp,
			},
			Group:      req.GroupName,
			Topic:      req.Topic,
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
		var req AckMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			FailGin(c, err)
			return
		}

		if !b.ValidateProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Ack failed - message mismatch", zap.String("message_id", req.ID))
			FailGin(c, ErrNotFound)
			return
		}

		if !b.CompleteProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Ack failed - message not found", zap.String("message_id", req.ID))
			FailGin(c, ErrNotFound)
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

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

func NackHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req NackMessageRequest
		if err := c.ShouldBindUri(&req); err != nil {
			FailGin(c, err)
			return
		}
		if err := req.Validate(); err != nil {
			FailGin(c, err)
			return
		}

		if !b.ValidateProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Nack failed - message mismatch", zap.String("message_id", req.ID))
			FailGin(c, ErrNotFound)
			return
		}

		if !b.RetryProcessing(req.ID, req.GroupName, req.Topic) {
			logger.Error("Nack failed - message not found", zap.String("message_id", req.ID))
			FailGin(c, ErrNotFound)
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
