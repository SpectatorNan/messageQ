package api

import (
	"messageQ/mq/broker"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// CreateTopicHandler handles POST /topics
func CreateTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CreateTopicRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			FailGin(c, ErrInvalidMessage)
			return
		}

		err := req.Validate()
		if err != nil {
			FailGin(c, err)
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

		var req GetTopicRequest
		if err := c.ShouldBindUri(&req); err != nil {
			FailGin(c, err)
			return
		}

		config, err := b.GetTopicConfig(req.Topic)
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
			Total:  len(topics),
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}

// DeleteTopicHandler handles DELETE /topics/:topic
func DeleteTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req DeleteTopicRequest
		if err := c.ShouldBindUri(&req); err != nil {
			FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			FailGin(c, err)
			return
		}

		topic := req.Topic

		if err := b.DeleteTopic(topic); err != nil {
			FailGin(c, err)
			return
		}

		resp := DeleteTopicResponse{
			Topic:   topic,
			Deleted: true,
		}

		c.JSON(http.StatusOK, NewRespSuccess(resp))
	}
}
