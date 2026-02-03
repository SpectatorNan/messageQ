package api

import (
	"messageQ/mq/broker"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// CreateTopicRequest represents a topic creation request
type CreateTopicRequest struct {
	Name       string           `json:"name" binding:"required"`
	Type       broker.TopicType `json:"type" binding:"required"` // NORMAL or DELAY
	QueueCount int              `json:"queue_count"`
}

// CreateTopicHandler handles POST /topics
func CreateTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateTopicRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}

		// Validate topic name
		if req.Name == "" {
			FailGin(c, ErrInvalidTopicName)
			return
		}
		
		if len(req.Name) > 255 {
			FailGin(c, ErrInvalidTopicName)
			return
		}
		
		// Topic name should not contain special characters
		if strings.ContainsAny(req.Name, " \t\n\r/\\") {
			FailGin(c, ErrInvalidTopicName)
			return
		}

		// Validate topic type
		if req.Type != broker.TopicTypeNormal && req.Type != broker.TopicTypeDelay {
			FailGin(c, ErrInvalidTopicType)
			return
		}

		// Default queue count
		if req.QueueCount <= 0 {
			req.QueueCount = 4
		}
		
		// Validate queue count range
		if req.QueueCount > 128 {
			c.JSON(http.StatusBadRequest, NewRespFail("invalid_queue_count", "queue_count must be between 1 and 128"))
			return
		}

		if err := b.CreateTopic(req.Name, req.Type, req.QueueCount); err != nil {
			// Check if topic already exists
			if strings.Contains(err.Error(), "already exists") {
				FailGin(c, ErrTopicExists)
				return
			}
			c.JSON(http.StatusBadRequest, NewRespFail("400", err.Error()))
			return
		}

		now := time.Now()
		resp := TopicResponse{
			Name:       req.Name,
			Type:       req.Type,
			QueueCount: req.QueueCount,
			CreatedAt:  now.Unix(),
		}

		c.JSON(http.StatusCreated, NewRespSuccess(resp))
	}
}

// GetTopicHandler handles GET /topics/:topic
func GetTopicHandler(b *broker.Broker) gin.HandlerFunc {
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

		config, err := b.GetTopicConfig(topic)
		if err != nil {
			FailGin(c, ErrTopicNotFound)
			return
		}

		resp := TopicResponse{
			Name:       config.Name,
			Type:       config.Type,
			QueueCount: config.QueueCount,
			CreatedAt:  config.CreatedAt,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// ListTopicsHandler handles GET /topics
func ListTopicsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topics := b.ListTopics()
		
		resp := ListTopicsResponse{
			Topics: topics,
			Count:  len(topics),
		}
		
		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// DeleteTopicHandler handles DELETE /topics/:topic
func DeleteTopicHandler(b *broker.Broker) gin.HandlerFunc {
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

		if err := b.DeleteTopic(topic); err != nil {
			if strings.Contains(err.Error(), "not found") {
				FailGin(c, ErrTopicNotFound)
				return
			}
			c.JSON(http.StatusBadRequest, NewRespFail("400", err.Error()))
			return
		}

		resp := DeleteTopicResponse{
			Topic:   topic,
			Deleted: true,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// ProduceToDelayTopicHandler handles POST /topics/:topic/messages (for delay topics)
func ProduceToDelayTopicHandler(b *broker.Broker) gin.HandlerFunc {
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

		// Check if it's a delay topic
		if !b.IsDelayTopic(topic) {
			c.JSON(http.StatusBadRequest, NewRespFail("invalid_topic_type", "topic is not a DELAY type topic"))
			return
		}

		var payload struct {
			Body     string `json:"body" binding:"required"`
			Tag      string `json:"tag" binding:"required"`
			DelayMs  int64  `json:"delay_ms"`  // delay in milliseconds
			DelaySec int64  `json:"delay_sec"` // delay in seconds (alternative)
		}

		if err := c.ShouldBindJSON(&payload); err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}
		
		// Validate body not empty after trim
		if strings.TrimSpace(payload.Body) == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}

		// Calculate delay duration
		var delay time.Duration
		if payload.DelayMs > 0 && payload.DelaySec > 0 {
			// Can't specify both
			FailGin(c, ErrInvalidDelay)
			return
		}
		
		if payload.DelayMs > 0 {
			if payload.DelayMs < 0 || payload.DelayMs > 86400000*30 { // max 30 days
				FailGin(c, ErrInvalidDelay)
				return
			}
			delay = time.Duration(payload.DelayMs) * time.Millisecond
		} else if payload.DelaySec > 0 {
			if payload.DelaySec < 0 || payload.DelaySec > 86400*30 { // max 30 days
				FailGin(c, ErrInvalidDelay)
				return
			}
			delay = time.Duration(payload.DelaySec) * time.Second
		} else {
			delay = 0 // immediate
		}

		// Produce to delay topic
		msg := b.EnqueueWithDelay(topic, payload.Body, payload.Tag, delay)

		resp := ProduceDelayResponse{
			ID:           msg.ID,
			Topic:        topic,
			Tag:          msg.Tag,
			ScheduledAt:  msg.Timestamp,
			ExecuteAt:    msg.Timestamp.Add(delay),
			DelaySeconds: delay.Seconds(),
			DelayMs:      delay.Milliseconds(),
		}

		c.JSON(http.StatusCreated, NewRespSuccess(resp))
	}
}
