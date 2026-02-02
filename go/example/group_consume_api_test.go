package example

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

func TestGroupConsumeAPI(t *testing.T) {
	dir := getTestDataDir(t, "groupapi")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 1)
	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// produce messages with tags
	for _, tag := range []string{"a", "b", "a"} {
		body := map[string]string{"body": "x", "tag": tag}
		bts, _ := json.Marshal(body)
		resp, err := client.Post(s.URL+"/topics/t/messages", "application/json", bytes.NewReader(bts))
		if err != nil {
			t.Fatalf("produce failed: %v", err)
		}
		resp.Body.Close()
	}

	// read group messages with tag filter (standard consume requires group)
	resp, err := client.Get(s.URL + "/topics/t/messages?group=g&queue_id=0&tag=b")
	if err != nil {
		t.Fatalf("group consume failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("group consume status=%d", resp.StatusCode)
	}
	var payload struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&payload)
	msgAny, ok := payload.Data["message"]
	if !ok {
		t.Fatalf("missing message in response")
	}
	msgMap, ok := msgAny.(map[string]interface{})
	if !ok {
		t.Fatalf("invalid message payload")
	}
	if msgMap["tag"] != "b" {
		t.Fatalf("expected tag b, got %v", msgMap["tag"])
	}
}
