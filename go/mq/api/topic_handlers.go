package api

import (
	"messageQ/mq/broker"
	"messageQ/mq/errx"
	"messageQ/mq/respx"
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
			respx.FailGin(c, errx.ErrInvalidMessage)
			return
		}

		err := req.Validate()
		if err != nil {
			respx.FailGin(c, err)
			return
		}

		if err := b.CreateTopic(req.Name, req.Type, req.QueueCount); err != nil {
			// Check if topic already exists
			if strings.Contains(err.Error(), "already exists") {
				respx.FailGin(c, errx.ErrTopicExists)
				return
			}
			c.JSON(http.StatusBadRequest, respx.NewRespFail("400", err.Error()))
			return
		}

		now := time.Now()
		resp := TopicResponse{
			Name:       req.Name,
			Type:       req.Type,
			QueueCount: req.QueueCount,
			CreatedAt:  now.Unix(),
		}

		c.JSON(http.StatusCreated, respx.NewRespSuccess(resp))
	}
}

// GetTopicHandler handles GET /topics/:topic
func GetTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req GetTopicRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		config, err := b.GetTopicConfig(req.Topic)
		if err != nil {
			respx.FailGin(c, errx.ErrTopicNotFound)
			return
		}

		resp := TopicResponse{
			Name:       config.Name,
			Type:       config.Type,
			QueueCount: config.QueueCount,
			CreatedAt:  config.CreatedAt,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
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

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}

// DeleteTopicHandler handles DELETE /topics/:topic
func DeleteTopicHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req DeleteTopicRequest
		if err := c.ShouldBindUri(&req); err != nil {
			respx.FailGin(c, err)
			return
		}

		err := req.Validate()
		if err != nil {
			respx.FailGin(c, err)
			return
		}

		topic := req.Topic

		if err := b.DeleteTopic(topic); err != nil {
			respx.FailGin(c, err)
			return
		}

		resp := DeleteTopicResponse{
			Topic:   topic,
			Deleted: true,
		}

		c.JSON(http.StatusOK, respx.NewRespSuccess(resp))
	}
}
