package api

import (
	"encoding/json"
	"io"
	"net/http"

	"messageQ/mq/broker"
)

var b = broker.NewBroker()

func ProduceHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		Fail(w, RespErrMissingTopic())
		return
	}

	var payload struct {
		Body string `json:"body"`
	}
	bodyBytes, _ := io.ReadAll(r.Body)
	if len(bodyBytes) == 0 {
		Fail(w, RespErrInvalidMessage())
		return
	}
	_ = json.Unmarshal(bodyBytes, &payload)
	if payload.Body == "" {
		payload.Body = string(bodyBytes)
	}

	q := b.GetQueue(topic)
	msg := q.Enqueue(payload.Body)

	OK(w, msg)
}

func ConsumeHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		Fail(w, RespErrMissingTopic())
		return
	}

	q := b.GetQueue(topic)
	msg := q.Dequeue()
	OK(w, msg)
}
