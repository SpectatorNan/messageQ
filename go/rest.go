package main

import (
	"net/http"

	"messageQ/mq/api"
)

// Shims to keep compatibility with existing code that calls OK/Fail
func OK[T any](w http.ResponseWriter, data T) {
	api.OK(w, data)
}

func Fail(w http.ResponseWriter, err error) {
	api.Fail(w, err)
}
