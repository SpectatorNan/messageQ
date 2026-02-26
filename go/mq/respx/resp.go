package respx

import (
	"github.com/SpectatorNan/messageQ/go/mq/errx"
	"net/http"

	"github.com/gin-gonic/gin"
)

func FailGin(c *gin.Context, err error) {
	// if RespErr, use its code/message
	if re, ok := err.(errx.RespErr); ok {
		// Determine HTTP status code based on error type
		statusCode := http.StatusBadRequest
		switch re.Code {
		case errx.ErrCodeNotFound, errx.ErrCodeTopicNotFound:
			statusCode = http.StatusNotFound
		}
		c.JSON(statusCode, NewRespFail(re.Code, re.Message))
		return
	}
	c.JSON(http.StatusBadRequest, NewRespFail("error", err.Error()))
}

type Resp[T any] struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    T      `json:"data,omitempty"`
}
type ListResp[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

const (
	RespCodeOk = "ok"
)

func NewRespEmpty() Resp[string] {
	return Resp[string]{
		Code:    RespCodeOk,
		Message: "success",
	}
}

func NewRespList[T any](items []T, total int) Resp[ListResp[T]] {
	return Resp[ListResp[T]]{
		Code:    RespCodeOk,
		Message: "success",
		Data: ListResp[T]{
			Items: items,
			Total: total,
		},
	}
}

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
