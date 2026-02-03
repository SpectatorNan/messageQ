package api

import (
	"messageQ/mq/broker"
	"net/http"
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
			c.JSON(http.StatusBadRequest, NewRespFail("400", err.Error()))
			return
		}

		// Validate topic type
		if req.Type != broker.TopicTypeNormal && req.Type != broker.TopicTypeDelay {
			c.JSON(http.StatusBadRequest, NewRespFail("400", "type must be NORMAL or DELAY"))
			return
		}

		// Default queue count
		if req.QueueCount <= 0 {
			req.QueueCount = 4
		}

		if err := b.CreateTopic(req.Name, req.Type, req.QueueCount); err != nil {
			c.JSON(http.StatusBadRequest, NewRespFail("400", err.Error()))
			return
		}

		c.JSON(http.StatusCreated, NewRespSuccess(map[string]interface{}{
			"name":        req.Name,
			"type":        req.Type,
			"queue_count": req.QueueCount,
		}))
	}
}

// GetTopicHandler handles GET /topics/:topic
func GetTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")

		config, err := b.GetTopicConfig(topic)
		if err != nil {
			c.JSON(http.StatusNotFound, NewRespFail("404", err.Error()))
			return
		}

		c.JSON(http.StatusOK, NewRespSuccess(config))
	}
}

// ListTopicsHandler handles GET /topics
func ListTopicsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topics := b.ListTopics()
		c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{
			"topics": topics,
			"count":  len(topics),
		}))
	}
}

// DeleteTopicHandler handles DELETE /topics/:topic
func DeleteTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")

		if err := b.DeleteTopic(topic); err != nil {
			c.JSON(http.StatusNotFound, NewRespFail("404", err.Error()))
			return
		}

		c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{
			"topic":   topic,
			"deleted": true,
		}))
	}
}

// ProduceToDelayTopicHandler handles POST /topics/:topic/messages (for delay topics)
func ProduceToDelayTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")

		// Check if it's a delay topic
		if !b.IsDelayTopic(topic) {
			c.JSON(http.StatusBadRequest, NewRespFail("400", "topic is not a DELAY type topic"))
			return
		}

		var payload struct {
			Body     string `json:"body"`
			Tag      string `json:"tag"`
			DelayMs  int64  `json:"delay_ms"`  // delay in milliseconds
			DelaySec int64  `json:"delay_sec"` // delay in seconds (alternative)
		}

		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, NewRespFail("400", err.Error()))
			return
		}

		// Calculate delay duration
		var delay time.Duration
		if payload.DelayMs > 0 {
			delay = time.Duration(payload.DelayMs) * time.Millisecond
		} else if payload.DelaySec > 0 {
			delay = time.Duration(payload.DelaySec) * time.Second
		} else {
			delay = 0 // immediate
		}

		// Produce to delay topic
		msg := b.EnqueueWithDelay(topic, payload.Body, payload.Tag, delay)

		c.JSON(http.StatusCreated, NewRespSuccess(map[string]interface{}{
			"id":           msg.ID,
			"topic":        topic,
			"scheduled_at": msg.Timestamp,
			"delay_ms":     delay.Milliseconds(),
		}))
	}
}
