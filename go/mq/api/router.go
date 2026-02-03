package api

import (
	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

// NewRouter creates and returns a configured Gin engine with the queue API routes.
func NewRouter(b *broker.Broker) *gin.Engine {
	r := gin.Default()

	// RESTful routes
	r.POST("/topics/:topic/messages", ProduceHandler(b))
	r.POST("/topics/:topic/messages/delay", ProduceDelayedHandler(b))
	r.GET("/topics/:topic/messages", ConsumeHandler(b))
	r.POST("/topics/:topic/messages/:id/ack", AckHandler(b))
	r.POST("/topics/:topic/messages/:id/nack", NackHandler(b))

	// consumer group offsets
	r.GET("/topics/:topic/offsets/:group", GetOffsetHandler(b))
	r.POST("/topics/:topic/offsets/:group", CommitOffsetHandler(b))

	// stats
	r.GET("/stats", DelayStatsHandler(b))

	return r
}
