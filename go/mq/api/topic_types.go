package api

import (
	"messageQ/mq/broker"
	"messageQ/mq/errx"
	client "messageQ/sdk"
)

type (

	// CreateTopicRequest represents a topic creation request
	CreateTopicRequest struct {
		Name       string           `json:"name" binding:"required"`
		Type       broker.TopicType `json:"type" binding:"required"` // NORMAL or DELAY
		QueueCount int              `json:"queueCount"`
		QueueCountAlt int           `json:"queue_count"`
	}
	GetTopicRequest struct {
		Topic string `uri:"topic" binding:"required"`
	}

	// TopicResponse is the response for topic operations
	TopicResponse struct {
		Name       string           `json:"name"`
		Type       broker.TopicType `json:"type"`
		QueueCount int              `json:"queueCount"`
		CreatedAt  int64            `json:"createdAt,omitempty"`
	}
	DeleteTopicRequest struct {
		Topic string `uri:"topic" binding:"required"`
	}

	// DeleteTopicResponse is the response for deleting a topic
	DeleteTopicResponse client.DeleteTopicResponse
)

func (r *DeleteTopicRequest) Validate() error {
	err := validateTopicName(r.Topic)
	if err != nil {
		return err
	}
	return nil
}
func (r *GetTopicRequest) Validate() error {
	err := validateTopicName(r.Topic)
	if err != nil {
		return err
	}
	return nil
}

func (r *CreateTopicRequest) Validate() error {
	err := validateTopicName(r.Name)
	if err != nil {
		return err
	}
	if r.Type != broker.TopicTypeNormal && r.Type != broker.TopicTypeDelay {
		return errx.ErrInvalidTopicType
	}
	if r.QueueCount < 0 || r.QueueCount > 128 {
		return errx.ErrInvalidQueueCount
	}
	return nil
}
