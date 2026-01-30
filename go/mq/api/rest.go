package api

import (
	"encoding/json"
	"net/http"
)

func OK[T any](w http.ResponseWriter, data T) {
	resp := NewRespSuccess(data)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// Fail accepts an error; if it's a RespErr it uses the contained code/message,
// otherwise it returns a generic error response with code "error".
func Fail(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	if re, ok := err.(RespErr); ok {
		resp := NewRespFail(re.Code, re.Message)
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	resp := NewRespFail("error", err.Error())
	_ = json.NewEncoder(w).Encode(resp)
}
