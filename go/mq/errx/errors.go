package errx

var (
	ErrInvalidApi           = NewRespErr(ErrCodeInvalidApi, "invalid API endpoint or method")
	ErrInternal             = NewRespErr(ErrCodeInternal, "internal server error")
	ErrMissingTopic         = NewRespErr(ErrCodeMissingTopic, "missing topic parameter")
	ErrInvalidMessage       = NewRespErr(ErrCodeInvalidMsg, "invalid message format or empty body")
	ErrInvalidID            = NewRespErr(ErrCodeInvalidID, "invalid message id format")
	ErrNotFound             = NewRespErr(ErrCodeNotFound, "message not found or already acked/nacked")
	ErrInvalidGroup         = NewRespErr(ErrCodeInvalidGroup, "invalid or missing consumer group")
	ErrInvalidOffset        = NewRespErr(ErrCodeInvalidOffset, "invalid offset value")
	ErrOffsetUnsupported    = NewRespErr(ErrCodeOffsetUnsupported, "offset store not supported")
	ErrMissingTag           = NewRespErr(ErrCodeMissingTag, "missing tag parameter")
	ErrBusy                 = NewRespErr(ErrCodeBusy, "message is currently being processed")
	ErrInvalidDelay         = NewRespErr(ErrCodeInvalidDelay, "invalid delay parameter")
	ErrDelayConflict        = NewRespErr(ErrCodeDelayConflict, "scheduledAt cannot be combined with delayMs or delaySec")
	ErrDelayBothSet         = NewRespErr(ErrCodeDelayBothSet, "delayMs and delaySec cannot both be set")
	ErrDelayNonPositive     = NewRespErr(ErrCodeDelayNonPositive, "delay values must be positive")
	ErrDelayTooLarge        = NewRespErr(ErrCodeDelayTooLarge, "delay exceeds maximum")
	ErrScheduledAtInvalid   = NewRespErr(ErrCodeScheduledAtInvalid, "scheduledAt must be in the future and within the max window")
	ErrInvalidTopicType     = NewRespErr(ErrCodeInvalidTopicType, "invalid topic type, must be NORMAL or DELAY")
	ErrTopicExists          = NewRespErr(ErrCodeTopicExists, "topic already exists")
	ErrInvalidQueueID       = NewRespErr(ErrCodeInvalidQueueID, "invalid queue_id parameter")
	ErrTopicNotFound        = NewRespErr(ErrCodeTopicNotFound, "topic not found")
	ErrInvalidTopicName     = NewRespErr(ErrCodeInvalidTopicName, "invalid topic name")
	ErrUnauthorized         = NewRespErr(ErrCodeUnauthorized, "unauthorized")
	ErrMissingSetAdminKey   = NewRespErr(ErrCodeMissingSetAdminKey, "missing set admin access key")
	ErrInvalidQueueCount    = NewRespErr(ErrCodeInvalidQueueCount, "queue_count must be between 1 and 128")
	ErrSubscriptionConflict = NewRespErr(ErrCodeSubscriptionConflict, "consumer group subscription does not match the existing tag filter")
)

// Optionally define error codes for API responses
const (
	ErrCodeInvalidApi           = "invalid_api"
	ErrCodeInternal             = "internal_error"
	ErrCodeMissingTopic         = "missing_topic"
	ErrCodeInvalidMsg           = "invalid_message"
	ErrCodeInvalidID            = "invalid_id"
	ErrCodeNotFound             = "not_found"
	ErrCodeInvalidGroup         = "invalid_group"
	ErrCodeInvalidOffset        = "invalid_offset"
	ErrCodeOffsetUnsupported    = "offset_unsupported"
	ErrCodeMissingTag           = "missing_tag"
	ErrCodeBusy                 = "busy"
	ErrCodeInvalidDelay         = "invalid_delay"
	ErrCodeDelayConflict        = "delay_conflict"
	ErrCodeDelayBothSet         = "delay_both_set"
	ErrCodeDelayNonPositive     = "delay_non_positive"
	ErrCodeDelayTooLarge        = "delay_too_large"
	ErrCodeScheduledAtInvalid   = "scheduled_at_invalid"
	ErrCodeInvalidTopicType     = "invalid_topic_type"
	ErrCodeTopicExists          = "topic_exists"
	ErrCodeInvalidQueueID       = "invalid_queue_id"
	ErrCodeTopicNotFound        = "topic_not_found"
	ErrCodeInvalidTopicName     = "invalid_topic_name"
	ErrCodeUnauthorized         = "unauthorized"
	ErrCodeMissingSetAdminKey   = "missing_set_admin_key"
	ErrCodeInvalidQueueCount    = "invalid_queue_count"
	ErrCodeSubscriptionConflict = "subscription_conflict"
)

func NewRespErr(code, message string) RespErr {
	return RespErr{
		Code:    code,
		Message: message,
	}
}

// RespErr is an error that carries an API error code and message and implements error
type RespErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e RespErr) Error() string {
	return e.Message
}
