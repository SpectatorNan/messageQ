package errx

var (
	ErrInvalidApi         = NewRespErr(ErrCodeInvalidApi, "invalid API endpoint or method")
	ErrMissingTopic       = NewRespErr(ErrCodeMissingTopic, "missing topic parameter")
	ErrInvalidMessage     = NewRespErr(ErrCodeInvalidMsg, "invalid message format or empty body")
	ErrInvalidID          = NewRespErr(ErrCodeInvalidID, "invalid message id format")
	ErrNotFound           = NewRespErr(ErrCodeNotFound, "message not found or already acked/nacked")
	ErrInvalidGroup       = NewRespErr(ErrCodeInvalidGroup, "invalid or missing consumer group")
	ErrInvalidOffset      = NewRespErr(ErrCodeInvalidOffset, "invalid offset value")
	ErrOffsetUnsupported  = NewRespErr(ErrCodeOffsetUnsupported, "offset store not supported")
	ErrMissingTag         = NewRespErr(ErrCodeMissingTag, "missing tag parameter")
	ErrBusy               = NewRespErr(ErrCodeBusy, "message is currently being processed")
	ErrInvalidDelay       = NewRespErr(ErrCodeInvalidDelay, "invalid delay parameter")
	ErrInvalidTopicType   = NewRespErr(ErrCodeInvalidTopicType, "invalid topic type, must be NORMAL or DELAY")
	ErrTopicExists        = NewRespErr(ErrCodeTopicExists, "topic already exists")
	ErrInvalidQueueID     = NewRespErr(ErrCodeInvalidQueueID, "invalid queue_id parameter")
	ErrTopicNotFound      = NewRespErr(ErrCodeTopicNotFound, "topic not found")
	ErrInvalidTopicName   = NewRespErr(ErrCodeInvalidTopicName, "invalid topic name")
	ErrUnauthorized       = NewRespErr(ErrCodeUnauthorized, "unauthorized")
	ErrMissingSetAdminKey = NewRespErr(ErrCodeMissingSetAdminKey, "missing set admin access key")
	ErrInvalidQueueCount  = NewRespErr(ErrCodeInvalidQueueCount, "queue_count must be between 1 and 128")
)

// Optionally define error codes for API responses
const (
	ErrCodeInvalidApi         = "invalid_api"
	ErrCodeMissingTopic       = "missing_topic"
	ErrCodeInvalidMsg         = "invalid_message"
	ErrCodeInvalidID          = "invalid_id"
	ErrCodeNotFound           = "not_found"
	ErrCodeInvalidGroup       = "invalid_group"
	ErrCodeInvalidOffset      = "invalid_offset"
	ErrCodeOffsetUnsupported  = "offset_unsupported"
	ErrCodeMissingTag         = "missing_tag"
	ErrCodeBusy               = "busy"
	ErrCodeInvalidDelay       = "invalid_delay"
	ErrCodeInvalidTopicType   = "invalid_topic_type"
	ErrCodeTopicExists        = "topic_exists"
	ErrCodeInvalidQueueID     = "invalid_queue_id"
	ErrCodeTopicNotFound      = "topic_not_found"
	ErrCodeInvalidTopicName   = "invalid_topic_name"
	ErrCodeUnauthorized       = "unauthorized"
	ErrCodeMissingSetAdminKey = "missing_set_admin_key"
	ErrCodeInvalidQueueCount  = "invalid_queue_count"
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
