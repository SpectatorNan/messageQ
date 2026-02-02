package api

import (
	"net/http"
	"strconv"

	"messageQ/mq/broker"
	"messageQ/mq/queue"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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
			Tag  string `json:"tag"`
		}
		if err := c.ShouldBindJSON(&payload); err != nil || payload.Body == "" {
			FailGin(c, ErrInvalidMessage)
			return
		}

		if payload.Tag == "" {
			FailGin(c, ErrMissingTag)
			return
		}

		msg := b.Enqueue(topic, payload.Body, payload.Tag)
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
		group := c.Query("group")
		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}
		queueID := 0
		if q := c.Query("queue_id"); q != "" {
			v, err := strconv.Atoi(q)
			if err != nil || v < 0 {
				FailGin(c, ErrInvalidOffset)
				return
			}
			queueID = v
		}
		offset, ok, err := b.GetOffset(group, topic, queueID)
		if err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}
		if !ok {
			offset = 0
		}
		tag := c.Query("tag")

		msgs, next, err := b.ReadFromConsumeQueue(topic, queueID, offset, 1, tag)
		if err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}
		if len(msgs) == 0 {
			FailGin(c, ErrNotFound)
			return
		}
		msg := msgs[0]
		b.BeginProcessing(group, topic, queueID, offset, next, queue.Message{
			ID:        msg.ID,
			Body:      msg.Body,
			Tag:       msg.Tag,
			Retry:     msg.Retry,
			Timestamp: msg.Timestamp,
		})
		c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{
			"message":     msg,
			"group":       group,
			"queue_id":    queueID,
			"offset":      offset,
			"next_offset": next,
			"state":       "processing",
		}))
	}
}

func AckHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		id := c.Param("id")
		if id == "" {
			FailGin(c, ErrInvalidID)
			return
		}
		if _, err := uuid.Parse(id); err != nil {
			FailGin(c, ErrInvalidID)
			return
		}
		if !b.CompleteProcessing(id) {
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
		id := c.Param("id")
		if id == "" {
			FailGin(c, ErrInvalidID)
			return
		}
		if _, err := uuid.Parse(id); err != nil {
			FailGin(c, ErrInvalidID)
			return
		}
		if !b.RetryProcessing(id) {
			FailGin(c, ErrNotFound)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(map[string]bool{"nacked": true}))
	}
}

// StatsHandler returns processing statistics per group.
func StatsHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		stats := b.Stats()
		c.JSON(http.StatusOK, NewRespSuccess(stats))
	}
}

// GetOffsetHandler returns the committed offset for a group/topic/queue.
func GetOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		group := c.Param("group")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}
		queueID := 0
		if q := c.Query("queue_id"); q != "" {
			if v, err := strconv.Atoi(q); err == nil {
				queueID = v
			} else {
				FailGin(c, ErrInvalidOffset)
				return
			}
		}
		offset, ok, err := b.GetOffset(group, topic, queueID)
		if err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}
		if !ok {
			c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{"offset": nil}))
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(map[string]interface{}{"offset": offset}))
	}
}

// CommitOffsetHandler commits the offset for a group/topic/queue.
func CommitOffsetHandler(b *broker.Broker) gin.HandlerFunc {
	return func(c *gin.Context) {
		topic := c.Param("topic")
		group := c.Param("group")
		if topic == "" {
			FailGin(c, ErrMissingTopic)
			return
		}
		if group == "" {
			FailGin(c, ErrInvalidGroup)
			return
		}
		var payload struct {
			QueueID int   `json:"queue_id"`
			Offset  int64 `json:"offset"`
		}
		if err := c.ShouldBindJSON(&payload); err != nil {
			FailGin(c, ErrInvalidOffset)
			return
		}
		if payload.Offset < 0 {
			FailGin(c, ErrInvalidOffset)
			return
		}
		if err := b.CommitOffset(group, topic, payload.QueueID, payload.Offset); err != nil {
			FailGin(c, ErrOffsetUnsupported)
			return
		}
		c.JSON(http.StatusOK, NewRespSuccess(map[string]bool{"committed": true}))
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
