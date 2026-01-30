package main

import (
	"encoding/json"
	"errors"
	"io"
	"messageQ/mq/api"
	"net/http"
)

var (
	errMissingTopic   = errors.New("missing topic")
	errInvalidMessage = errors.New("invalid message format")
)

var broker = NewBroker()

// Legacy shims kept for compatibility with older code.
func produceHandler(w http.ResponseWriter, r *http.Request) {
	api.ProduceHandler(w, r)
}

func consumeHandler(w http.ResponseWriter, r *http.Request) {
	api.ConsumeHandler(w, r)
}

func newProduceHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		Fail(w, api.ErrMissingTopic)
		return
	}

	// try to decode JSON body as {"body": "..."} or accept raw string
	var payload struct {
		Body string `json:"body"`
	}

	bodyBytes, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(bodyBytes, &payload)
	if payload.Body == "" {
		// fallback to raw body as string
		payload.Body = string(bodyBytes)
	}

	q := broker.GetQueue(topic)
	msg := q.Enqueue(payload.Body)

	OK(w, msg)
}

func newConsumeHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		Fail(w, api.ErrMissingTopic)
		return
	}

	q := broker.GetQueue(topic)
	msg := q.Dequeue()

	OK(w, msg)
}
