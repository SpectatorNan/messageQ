package api

import (
	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

// NewRouter creates and returns a configured Gin engine with the queue API routes.
func NewRouter(b *broker.Broker) *gin.Engine {
	r := gin.Default()

	// API version 1
	v1 := r.Group("/api/v1")
	{
		// Topic management
		v1.POST("/topics", CreateTopicHandler(b))
		v1.GET("/topics", ListTopicsHandler(b))
		v1.GET("/topics/:topic", GetTopicHandler(b))
		v1.DELETE("/topics/:topic", DeleteTopicHandler(b))

		// Message production (supports both normal and delayed messages via optional delay_ms/delay_sec parameters)
		v1.POST("/topics/:topic/messages", ProduceHandler(b))

		// Message consumption (consumer-centric)
		consumers := v1.Group("/consumers/:group")
		{
			consumers.GET("/topics/:topic/messages", ConsumeHandler(b))
			consumers.GET("/topics/:topic/offsets", GetOffsetHandler(b))
			consumers.POST("/topics/:topic/offsets", CommitOffsetHandler(b))
		}

		// Message acknowledgment (message-centric)
		v1.POST("/messages/:id/ack", AckHandler(b))
		v1.POST("/messages/:id/nack", NackHandler(b))

		// Monitoring
		v1.GET("/stats", DelayStatsHandler(b))
	}

	return r
}
