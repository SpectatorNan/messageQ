package api

import (
	"github.com/gin-gonic/gin"
	"messageQ/mq/broker"
)

// NewRouter creates and returns a configured Gin engine with the queue API routes.
func NewRouter(b *broker.Broker) *gin.Engine {
	r := gin.Default()

	// RESTful routes
	r.POST("/topics/:topic/messages", ProduceHandler(b))
	r.GET("/topics/:topic/messages", ConsumeHandler(b))
	r.POST("/topics/:topic/messages/:id/ack", AckHandler(b))
	r.POST("/topics/:topic/messages/:id/nack", NackHandler(b))

	return r
}
