package api

var (
	ErrMissingTopic      = NewRespErr(ErrCodeMissingTopic, "missing topic")
	ErrInvalidMessage    = NewRespErr(ErrCodeInvalidMsg, "invalid message format")
	ErrInvalidID         = NewRespErr(ErrCodeInvalidID, "invalid message id")
	ErrNotFound          = NewRespErr(ErrCodeNotFound, "message not found or already acked/nacked")
	ErrInvalidGroup      = NewRespErr(ErrCodeInvalidGroup, "invalid consumer group")
	ErrInvalidOffset     = NewRespErr(ErrCodeInvalidOffset, "invalid offset")
	ErrOffsetUnsupported = NewRespErr(ErrCodeOffsetUnsupported, "offset store not supported")
)

// Optionally define error codes for API responses
const (
	ErrCodeMissingTopic      = "missing_topic"
	ErrCodeInvalidMsg        = "invalid_message"
	ErrCodeInvalidID         = "invalid_id"
	ErrCodeNotFound          = "not_found"
	ErrCodeInvalidGroup      = "invalid_group"
	ErrCodeInvalidOffset     = "invalid_offset"
	ErrCodeOffsetUnsupported = "offset_unsupported"
)

// RespErr is an error that carries an API error code and message and implements error
type RespErr struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e RespErr) Error() string {
	return e.Message
}

func NewRespErr(code, message string) RespErr {
	return RespErr{Code: code, Message: message}
}
