package api

import (
	"strings"

	"github.com/SpectatorNan/messageQ/go/mq/errx"
	"github.com/SpectatorNan/messageQ/go/mq/logger"
	client "github.com/SpectatorNan/messageQ/go/sdk"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type (
	ProduceMessageRequest struct {
		Topic string `uri:"topic" binding:"required"`
		client.ProduceMessageRequest
		DelayMs     int64                `json:"delayMs"`
		DelaySec    int64                `json:"delaySec"`
		ScheduledAt *client.FlexibleUnix `json:"scheduledAt,omitempty"`
	}
	ProduceMessageResponse struct {
		ID            string `json:"id"`
		Topic         string `json:"topic"`
		Tag           string `json:"tag"`
		Body          string `json:"body"`
		CorrelationID string `json:"correlationId,omitempty"`
		Timestamp     int64  `json:"timestamp"`
		Retry         int    `json:"retry"`
		ScheduledAt   int64  `json:"scheduledAt"`
		ExecutedAt    *int64 `json:"executedAt"`
	}
	ProduceBatchMessage struct {
		Body          string               `json:"body"`
		Tag           string               `json:"tag"`
		CorrelationID string               `json:"correlationId,omitempty"`
		DelayMs       int64                `json:"delayMs"`
		DelaySec      int64                `json:"delaySec"`
		ScheduledAt   *client.FlexibleUnix `json:"scheduledAt,omitempty"`
	}
	ProduceBatchRequest struct {
		Topic    string                `uri:"topic" binding:"required"`
		Messages []ProduceBatchMessage `json:"messages"`
		DelayMs  int64                 `json:"delayMs"`
		DelaySec int64                 `json:"delaySec"`
	}
	ProduceBatchResponse struct {
		Messages []ProduceMessageResponse `json:"messages"`
	}

	ConsumeMessageRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		Tag       string `form:"tag"`
		QueueId   *int   `form:"queueId"`
	}
	ConsumeBatchRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		Tag       string `form:"tag"`
		QueueId   *int   `form:"queueId"`
		Max       *int   `form:"max"`
	}
	ListMessagesRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		State     string `form:"state"`
		Tag       string `form:"tag"`
		QueueId   *int   `form:"queueId"`
		Cursor    *int64 `form:"cursor"`
		Limit     *int   `form:"limit"`
	}
	GetOffsetRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		QueueID   *int   `form:"queueId"`
	}
	CommitOffsetRequest struct {
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
		QueueID   int    `json:"queueId"`
		Offset    int64  `json:"offset" binding:"required"`
	}
	AckMessageRequest struct {
		ID        string `uri:"id" binding:"required"`
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
	}
	AckResponse struct {
		MessageID string `json:"messageId"`
		Acked     bool   `json:"acked"`
		Topic     string `json:"topic"`
	}
	NackMessageRequest struct {
		ID        string `uri:"id" binding:"required"`
		Topic     string `uri:"topic" binding:"required"`
		GroupName string `uri:"group" binding:"required"`
	}
	NackResponse struct {
		MessageID string `json:"messageId"`
		Nacked    bool   `json:"nacked"`
		Topic     string `json:"topic"`
		Requeued  bool   `json:"requeued"`
	}
	TerminateMessageRequest struct {
		ID    string `uri:"id" binding:"required"`
		Topic string `uri:"topic" binding:"required"`
	}
	TerminateResponse struct {
		MessageID  string `json:"messageId"`
		Terminated bool   `json:"terminated"`
		Topic      string `json:"topic"`
		State      string `json:"state"`
	}
	TerminateBatchRequest struct {
		Topic      string   `uri:"topic" binding:"required"`
		MessageIDs []string `json:"messageIds"`
	}
	TerminateBatchResponse struct {
		MessageIDs      []string `json:"messageIds"`
		TerminatedCount int      `json:"terminatedCount"`
		Topic           string   `json:"topic"`
		State           string   `json:"state"`
	}
	// ConsumeMessageResponse is the response for consuming a message
	ConsumeMessageResponse struct {
		Message    ConsumeMessage `json:"message"` // storage.Message
		Group      string         `json:"group"`
		Topic      string         `json:"topic"`
		QueueID    int            `json:"queueId"`
		Offset     int64          `json:"offset"`
		NextOffset int64          `json:"nextOffset"`
		State      string         `json:"state"`
	}
	ConsumeMessage struct {
		ID            string `json:"id"`
		Body          string `json:"body"`
		Tag           string `json:"tag,omitempty"`
		CorrelationID string `json:"correlationId,omitempty"`
		Retry         int    `json:"retry"`
		Timestamp     int64  `json:"timestamp"`
	}
	ConsumeBatchMessage struct {
		ID            string `json:"id"`
		Body          string `json:"body"`
		Tag           string `json:"tag,omitempty"`
		CorrelationID string `json:"correlationId,omitempty"`
		Retry         int    `json:"retry"`
		Timestamp     int64  `json:"timestamp"`
		QueueID       int    `json:"queueId"`
		Offset        int64  `json:"offset"`
		NextOffset    int64  `json:"nextOffset"`
	}
	MessageStatus struct {
		ID            string `json:"id"`
		Body          string `json:"body"`
		Tag           string `json:"tag,omitempty"`
		CorrelationID string `json:"correlationId,omitempty"`
		Retry         int    `json:"retry"`
		Timestamp     int64  `json:"timestamp"`
		ScheduledAt   *int64 `json:"scheduledAt,omitempty"`
		ConsumedAt    *int64 `json:"consumedAt,omitempty"`
		AckedAt       *int64 `json:"ackedAt,omitempty"`
		EventAt       *int64 `json:"eventAt,omitempty"`
		QueueID       *int   `json:"queueId,omitempty"`
		Offset        *int64 `json:"offset,omitempty"`
		NextOffset    *int64 `json:"nextOffset,omitempty"`
	}
	ConsumeBatchResponse struct {
		Messages []ConsumeBatchMessage `json:"messages"`
		Group    string                `json:"group"`
		Topic    string                `json:"topic"`
		State    string                `json:"state"`
	}
	ListMessagesResponse struct {
		Group      string          `json:"group"`
		Topic      string          `json:"topic"`
		State      string          `json:"state"`
		Messages   []MessageStatus `json:"messages"`
		NextCursor *int64          `json:"nextCursor,omitempty"`
	}
)

const maxCorrelationIDBytes = 128

func (r *ProduceMessageRequest) Validate() error {

	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	body := strings.TrimSpace(r.Body)
	if body == "" {
		return errx.ErrInvalidMessage
	}
	if strings.TrimSpace(r.Tag) == "" {
		return errx.ErrInvalidMessage
	}
	if err := validateCorrelationID(r.CorrelationID); err != nil {
		return err
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

func (r *ConsumeBatchRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if r.QueueId != nil && *r.QueueId < 0 {
		return errx.ErrInvalidQueueID
	}
	if r.Max != nil && *r.Max <= 0 {
		return errx.ErrInvalidMessage
	}
	return nil
}

func (r *ListMessagesRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if r.State != "" && r.State != "processing" && r.State != "acked" && r.State != "completed" && r.State != "pending" && r.State != "scheduled" && r.State != "cancelled" && r.State != "expired" && r.State != "retry" && r.State != "dlq" {
		return errx.ErrInvalidMessage
	}
	if (r.State == "pending" || r.State == "scheduled") && r.QueueId != nil && *r.QueueId < 0 {
		return errx.ErrInvalidQueueID
	}
	if r.State == "pending" && r.QueueId == nil {
		return errx.ErrInvalidQueueID
	}
	if r.Limit != nil && *r.Limit <= 0 {
		return errx.ErrInvalidMessage
	}
	return nil
}

func (r *GetOffsetRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if r.QueueID != nil && *r.QueueID < 0 {
		return errx.ErrInvalidQueueID
	}
	return nil
}

func (r *CommitOffsetRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if err := validateGroupName(r.GroupName); err != nil {
		logger.Warn("Invalid group name", zap.String("group", r.GroupName))
		return err
	}
	if r.QueueID < 0 {
		return errx.ErrInvalidQueueID
	}
	if r.Offset < 0 {
		return errx.ErrInvalidOffset
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
		return errx.ErrInvalidID
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
		return errx.ErrInvalidID
	}
	return nil
}

func (r *TerminateMessageRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if _, err := uuid.Parse(r.ID); err != nil {
		logger.Warn("Invalid message ID format", zap.String("message_id", r.ID), zap.Error(err))
		return errx.ErrInvalidID
	}
	return nil
}

func (r *TerminateBatchRequest) Validate() error {
	if err := validateTopicName(r.Topic); err != nil {
		logger.Warn("Invalid topic name", zap.String("topic", r.Topic))
		return err
	}
	if len(r.MessageIDs) == 0 {
		return errx.ErrInvalidMessage
	}
	for _, id := range r.MessageIDs {
		if _, err := uuid.Parse(id); err != nil {
			logger.Warn("Invalid message ID format", zap.String("message_id", id), zap.Error(err))
			return errx.ErrInvalidID
		}
	}
	return nil
}

func validateCorrelationID(correlationID string) error {
	if len([]byte(correlationID)) > maxCorrelationIDBytes {
		return errx.ErrInvalidMessage
	}
	return nil
}

func validateTopicName(topic string) error {
	if topic == "" {
		return errx.ErrMissingTopic
	}
	if len(topic) > 128 {
		return errx.ErrInvalidTopicName
	}
	// Topic name should not contain special characters
	if strings.ContainsAny(topic, " \t\n\r/\\") {
		return errx.ErrInvalidTopicName
	}
	return nil
}

func validateGroupName(group string) error {
	if group == "" {
		return errx.ErrInvalidGroup
	}
	if len(group) > 255 {
		return errx.ErrInvalidGroup
	}
	// Group name should not contain special characters
	if strings.ContainsAny(group, " \t\n\r/\\") {
		return errx.ErrInvalidGroup
	}
	return nil
}
