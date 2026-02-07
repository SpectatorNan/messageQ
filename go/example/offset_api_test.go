package example

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/auth"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

func TestOffsetAPI(t *testing.T) {
	t.Setenv("MQ_ADMIN_KEY", "test-ak")

	dir := getTestDataDir(t, "offsetapi")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithPersistence(store, 1, dir)
	defer b.Close()

	if err := b.CreateTopic("t", broker.TopicTypeNormal, 1); err != nil {
		t.Fatalf("create topic failed: %v", err)
	}

	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	type offsetData struct {
		Group   string `json:"group"`
		Topic   string `json:"topic"`
		QueueID int    `json:"queue_id"`
		Offset  *int64 `json:"offset"`
	}
	type offsetResp struct {
		Code    string     `json:"code"`
		Message string     `json:"message"`
		Data    offsetData `json:"data"`
	}

	getOffset := func() offsetResp {
		req, err := http.NewRequest(http.MethodGet, s.URL+"/api/v1/consumers/g1/topics/t/offsets?queue_id=0", nil)
		if err != nil {
			t.Fatalf("new request failed: %v", err)
		}
		req.Header.Set(auth.AuthHeaderKey, "test-ak")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("get offset failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get offset status=%d", resp.StatusCode)
		}
		var payload offsetResp
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			t.Fatalf("decode get offset response failed: %v", err)
		}
		return payload
	}

	// before commit, offset should be null
	first := getOffset()
	if first.Data.Offset != nil {
		t.Fatalf("expected nil offset before commit, got %v", *first.Data.Offset)
	}

	commitBody := map[string]interface{}{"queue_id": 0, "offset": 5}
	bodyBytes, _ := json.Marshal(commitBody)
	req, err := http.NewRequest(http.MethodPost, s.URL+"/api/v1/consumers/g1/topics/t/offsets", bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("new commit request failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.AuthHeaderKey, "test-ak")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("commit offset failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("commit offset status=%d", resp.StatusCode)
	}

	// after commit, offset should be returned
	second := getOffset()
	if second.Data.Offset == nil || *second.Data.Offset != 5 {
		if second.Data.Offset == nil {
			t.Fatalf("expected offset 5 after commit, got nil")
		}
		t.Fatalf("expected offset 5 after commit, got %d", *second.Data.Offset)
	}
}
