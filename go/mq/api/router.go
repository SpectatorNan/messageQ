package api

import (
	"messageQ/mq/auth"
	"messageQ/mq/broker"
	"messageQ/mq/config"
	"messageQ/mq/errx"
	"net/http"

	"github.com/gin-gonic/gin"
)

// NewRouter creates and returns a configured Gin engine with the queue API routes.
func NewRouter(b *broker.Broker) *gin.Engine {
	r := gin.Default()

	cfg, err := config.Load()
	adminAK := ""
	if err == nil && cfg != nil {
		adminAK = cfg.AdminAK
	}

	// if adminAK is not set, log a fatal error and exit
	if adminAK == "" {
		panic("Admin access key is not set in configuration")
	}

	r.NoRoute(func(context *gin.Context) {
		context.JSON(http.StatusNotFound, errx.ErrInvalidApi)
	})

	// API version 1
	v1 := r.Group("/api/v1")
	{
		protected := v1.Group("/")
		protected.Use(auth.AuthMiddleware(b, adminAK))
		{
			// Topic management
			protected.POST("/topics", CreateTopicHandler(b))
			protected.GET("/topics", ListTopicsHandler(b))
			protected.GET("/topics/:topic", GetTopicHandler(b))
			protected.DELETE("/topics/:topic", DeleteTopicHandler(b))

			// Message production (supports both normal and delayed messages via optional delay_ms/delay_sec parameters)
			protected.POST("/topics/:topic/messages", ProduceHandler(b))
			protected.POST("/topics/:topic/messages/batch", ProduceBatchHandler(b))

			// Message consumption (consumer-centric)
			consumers := protected.Group("/consumers/:group")
			{
				consumers.GET("/topics/:topic/messages", ConsumeHandler(b))
				consumers.GET("/topics/:topic/messages/batch", ConsumeBatchHandler(b))
				consumers.GET("/topics/:topic/messages/status", ListMessagesHandler(b))
				consumers.GET("/topics/:topic/offsets", GetOffsetHandler(b))
				consumers.POST("/topics/:topic/offsets", CommitOffsetHandler(b))

				consumers.POST("/topics/:topic/messages/:id/ack", AckHandler(b))
				consumers.POST("/topics/:topic/messages/:id/nack", NackHandler(b))
			}

			// Monitoring
			protected.GET("/stats", DelayStatsHandler(b))
			protected.GET("/stats/topics/:topic", TopicStatsHandler(b))
		}

		admin := v1.Group("/admin")
		admin.Use(auth.AdminAuthMiddleware(adminAK))
		{
			admin.GET("/aks", ListAKHandler(b))
			admin.POST("/aks", AddAKHandler(b))
			admin.DELETE("/aks/:id", DeleteAKHandler(b))
		}
	}

	return r
}
