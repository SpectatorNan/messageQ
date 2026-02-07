package api

import (
	"messageQ/mq/logger"
	client "messageQ/sdk"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type (
	ProduceMessageRequest struct {
		Topic string `uri:"topic" binding:"required"`
		client.ProduceMessageRequest
	}

	ConsumeMessageRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		Tag       string `form:"tag"`
		QueueId   *int   `form:"queue_id"`
	}
	AckMessageRequest struct {
		ID        string `uri:"id" binding:"required"`
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
	}
	NackMessageRequest struct {
		ID        string `uri:"id" binding:"required"`
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
	}
	// ConsumeMessageResponse is the response for consuming a message
	ConsumeMessageResponse struct {
		Message    ConsumeMessage `json:"message"` // storage.Message
		Group      string         `json:"group"`
		Topic      string         `json:"topic"`
		QueueID    int            `json:"queue_id"`
		Offset     int64          `json:"offset"`
		NextOffset int64          `json:"next_offset"`
		State      string         `json:"state"`
	}
	ConsumeMessage struct {
		ID        string    `json:"id"`
		Body      string    `json:"body"`
		Tag       string    `json:"tag,omitempty"`
		Retry     int       `json:"retry"`
		Timestamp time.Time `json:"timestamp"`
	}
)

func (r *ProduceMessageRequest) Validate() error {

	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	body := strings.TrimSpace(r.Body)
	if body == "" {
		return ErrInvalidMessage
	}

	return nil
}
func (r *ConsumeMessageRequest) Validate() error {

	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}

	return nil
}

func (r *AckMessageRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if _, err := uuid.Parse(r.ID); err != nil {
		logger.Warn("Invalid message ID format", zap.String("message_id", r.ID), zap.Error(err))
		return ErrInvalidID
	}
	return nil
}

func (r *NackMessageRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if _, err := uuid.Parse(r.ID); err != nil {
		logger.Warn("Invalid message ID format", zap.String("message_id", r.ID), zap.Error(err))
		return ErrInvalidID
	}
	return nil
}

func validateTopicName(topic string) error {
	if topic == "" {
		return ErrMissingTopic
	}
	if len(topic) > 128 {
		return ErrInvalidTopicName
	}
	// Topic name should not contain special characters
	if strings.ContainsAny(topic, " \t\n\r/\\") {
		return ErrInvalidTopicName
	}
	return nil
}

func validateGroupName(group string) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if len(group) > 255 {
		return ErrInvalidGroup
	}
	// Group name should not contain special characters
	if strings.ContainsAny(group, " \t\n\r/\\") {
		return ErrInvalidGroup
	}
	return nil
}
