package api

import (
	"net/http"
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
		
		var payload struct {
			Body      string `json:"body"`
			Tag       string `json:"tag"`
			DelayMs   int64  `json:"delay_ms"`   // delay in milliseconds
			DelaySec  int64  `json:"delay_sec"`  // delay in seconds (alternative)
		}
		
		if err := c.ShouldBindJSON(&payload); err != nil || payload.Body == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}

		if payload.Tag == "" {
			FailGin(c, ErrMissingTag)
			return
		}
		
		// Calculate delay duration
		var delay time.Duration
		if payload.DelayMs > 0 {
			delay = time.Duration(payload.DelayMs) * time.Millisecond
		} else if payload.DelaySec > 0 {
			delay = time.Duration(payload.DelaySec) * time.Second
		} else {
			// No delay specified, treat as normal message
			msg := b.Enqueue(topic, payload.Body, payload.Tag)
			c.JSON(http.StatusOK, NewRespSuccess(msg))
			return
		}
		
		// Produce delayed message
		msg := b.EnqueueWithDelay(topic, payload.Body, payload.Tag, delay)
		c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{
			"id":            msg.ID,
			"scheduled_at":  msg.Timestamp,
			"execute_at":    msg.Timestamp.Add(delay),
			"delay_seconds": delay.Seconds(),
		}))
	}
}

// DelayStatsHandler returns delay scheduler statistics
func DelayStatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		result := make(map[string]interface{})
		
		// Convert groupStats to interface{}
		for k, v := range stats {
			result[k] = v
		}
		
		// Add delay scheduler stats if available
		if ds := b.GetDelayScheduler(); ds != nil {
			delayStats := ds.Stats()
			result["delay_scheduler"] = delayStats
		}
		
		c.JSON(http.StatusOK, NewRespSuccess(result))
	}
}
