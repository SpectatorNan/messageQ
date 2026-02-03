package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestTopicExistenceValidation(t *testing.T) {
	dataDir := "testdata/topic_validation"
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 4)
	defer b.Close()

	router := api.NewRouter(b)

	t.Run("produce_to_nonexistent_topic", func(t *testing.T) {
		payload := map[string]interface{}{
			"body": "test message",
			"tag":  "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/nonexistent-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeTopicNotFound, resp.Code)
		}
	})

	t.Run("consume_from_nonexistent_topic", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/consumers/test-group/topics/nonexistent-topic/messages", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeTopicNotFound, resp.Code)
		}
	})

	t.Run("produce_delayed_to_nonexistent_topic", func(t *testing.T) {
		payload := map[string]interface{}{
			"body":      "delayed message",
			"tag":       "test",
			"delay_sec": 10,
		}
		body, _ := json.Marshal(payload)

		// Use the unified /messages endpoint with delay parameters
		req := httptest.NewRequest("POST", "/api/v1/topics/nonexistent-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeTopicNotFound, resp.Code)
		}
	})

	t.Run("get_offset_for_nonexistent_topic", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/consumers/test-group/topics/nonexistent-topic/offsets?queue_id=0", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeTopicNotFound, resp.Code)
		}
	})

	t.Run("commit_offset_for_nonexistent_topic", func(t *testing.T) {
		payload := map[string]interface{}{
			"queue_id": 0,
			"offset":   10,
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/consumers/test-group/topics/nonexistent-topic/offsets", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeTopicNotFound, resp.Code)
		}
	})

	t.Run("produce_to_existing_topic", func(t *testing.T) {
		// First create a topic with unique name
		topicName := fmt.Sprintf("test-topic-%d", time.Now().UnixNano())
		createPayload := map[string]interface{}{
			"name":        topicName,
			"type":        "NORMAL",
			"queue_count": 4,
		}
		createBody, _ := json.Marshal(createPayload)
		createReq := httptest.NewRequest("POST", "/api/v1/topics", bytes.NewReader(createBody))
		createReq.Header.Set("Content-Type", "application/json")
		createW := httptest.NewRecorder()
		router.ServeHTTP(createW, createReq)

		if createW.Code != http.StatusCreated {
			t.Fatalf("Failed to create topic: %d - %s", createW.Code, createW.Body.String())
		}

		// Now produce a message
		payload := map[string]interface{}{
			"body": "test message",
			"tag":  "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/topics/%s/messages", topicName), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[api.ProduceResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}
	})
}
