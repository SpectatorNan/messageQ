package api

import (
	"net/http"
	"strings"
	"time"

	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

// ProduceDelayedHandler creates a handler for producing delayed messages
func ProduceDelayedHandler(b *broker.Broker) gin.HandlerFunc {
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
		
		var payload struct {
			Body      string `json:"body" binding:"required"`
			Tag       string `json:"tag" binding:"required"`
			DelayMs   int64  `json:"delay_ms"`   // delay in milliseconds
			DelaySec  int64  `json:"delay_sec"`  // delay in seconds (alternative)
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
			// No delay specified
			FailGin(c, ErrInvalidDelay)
			return
		}
		
		// Produce delayed message
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
		
		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// DelayStatsHandler returns delay scheduler statistics
func DelayStatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		result := StatsResponse{
			Groups:    make(map[string]interface{}),
			Timestamp: time.Now(),
		}
		
		// Convert groupStats to interface{}
		for k, v := range stats {
			result.Groups[k] = v
		}
		
		// Add delay scheduler stats if available
		if ds := b.GetDelayScheduler(); ds != nil {
			delayStats := ds.Stats()
			result.DelayScheduler = delayStats
		}
		
		c.JSON(http.StatusOK, NewRespSuccess(result))
	}
}
