package api

import (
	"net/http"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/queue"

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
		
		var payload struct {
			Body string `json:"body" binding:"required"`
			Tag  string `json:"tag" binding:"required"`
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
		
		queueID := 0
		if q := c.Query("queue_id"); q != "" {
			v, err := strconv.Atoi(q)
			if err != nil || v < 0 {
				FailGin(c, ErrInvalidQueueID)
				return
			}
			queueID = v
		}
		
		tag := c.Query("tag")
		
		// 使用线程安全的消费方法，防止多consumer重复消费
		msgs, offset, next, err := b.ConsumeWithLock(group, topic, queueID, tag, 1)
		if err != nil {
			logger.Error("Consume error", zap.String("group", group), zap.String("topic", topic), zap.Error(err))
			FailGin(c, ErrOffsetUnsupported)
			return
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
		c.JSON(http.StatusBadRequest, NewRespFail(re.Code, re.Message))
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
