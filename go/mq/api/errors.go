package api

var (
	ErrMissingTopic   = NewRespErr(ErrCodeMissingTopic, "missing topic")
	ErrInvalidMessage = NewRespErr(ErrCodeInvalidMsg, "invalid message format")
)

// Optionally define error codes for API responses
const (
	ErrCodeMissingTopic = "missing_topic"
	ErrCodeInvalidMsg   = "invalid_message"
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
