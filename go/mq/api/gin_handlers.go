package api

import (
	"net/http"
	"strconv"

	"messageQ/mq/broker"

	"github.com/gin-gonic/gin"
)

// Handler constructors that accept a broker and return gin.HandlerFunc.

func ProduceHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		var payload struct {
			Body string `json:"body"`
		}
		if err := c.ShouldBindJSON(&payload); err != nil || payload.Body == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}
		q := b.GetQueue(topic)
		msg := q.Enqueue(payload.Body)
		c.JSON(http.StatusOK, NewRespSuccess(msg))
	}
}

func ConsumeHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		q := b.GetQueue(topic)
		msg := q.Dequeue()
		c.JSON(http.StatusOK, NewRespSuccess(msg))
	}
}

func AckHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		idStr := c.Param("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			FailGin(c, ErrInvalidID)
			return
		}
		q := b.GetQueue(topic)
		ok := q.Ack(id)
		if !ok {
			FailGin(c, ErrNotFound)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(map[string]bool{"acked": true}))
	}
}

func NackHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		idStr := c.Param("id")
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			FailGin(c, ErrInvalidID)
			return
		}
		q := b.GetQueue(topic)
		ok := q.Nack(id)
		if !ok {
			FailGin(c, ErrNotFound)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(map[string]bool{"nacked": true}))
	}
}

// helper to use api.Fail behavior but for gin.Context
func FailGin(c *gin.Context, err error) {
	// if RespErr, use its code/message
	if re, ok := err.(RespErr); ok {
		c.JSON(http.StatusBadRequest, NewRespFail(re.Code, re.Message))
		return
	}
	c.JSON(http.StatusBadRequest, NewRespFail("error", err.Error()))
}
