package api

type Resp[T any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}

const (
	RespCodeOk = "ok"
)

func NewRespSuccess[T any](data T) Resp[T] {
	return Resp[T]{
		Code:    RespCodeOk,
		Message: "success",
		Data:    data,
	}
}

func NewRespFail(code string, message string) Resp[string] {
	return Resp[string]{
		Code:    code,
		Message: message,
	}
}
