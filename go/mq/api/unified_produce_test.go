package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/storage"
)

func TestUnifiedProduceAPI(t *testing.T) {
	logger.Init(logger.Config{
		Level:      "info",
		OutputPath: "stdout",
		Format:     "console",
	})

	// Create a temporary data directory for testing
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("mq-test-unified-%d", time.Now().Unix()))
	defer os.RemoveAll(tmpDir)

	// Initialize storage
	store := storage.NewWALStorage(tmpDir, 10*time.Millisecond)
	defer store.Close()

	// Initialize the broker
	b := broker.NewBrokerWithPersistence(store, 1, tmpDir)
	defer b.Close()

	router := api.NewRouter(b)

	// Create a test topic first
	topicName := "test-unified"
	createBody, _ := json.Marshal(map[string]interface{}{
		"name":        topicName,
		"type":        "NORMAL",
		"queue_count": 1,
	})

	createReq := httptest.NewRequest("POST", "/api/v1/topics", bytes.NewReader(createBody))
	createReq.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()
	router.ServeHTTP(createW, createReq)

	if createW.Code != http.StatusCreated {
		t.Fatalf("Failed to create topic: %d - %s", createW.Code, createW.Body.String())
	}

	t.Run("ProduceNormalMessage", func(t *testing.T) {
		// Produce a normal message (no delay parameters)
		payload := map[string]interface{}{
			"body": "normal message",
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

		if resp.Data.ID == "" {
			t.Error("Expected message ID, got empty string")
		}
	})

	t.Run("ProduceDelayedMessageWithMilliseconds", func(t *testing.T) {
		// Produce a delayed message using delay_ms
		payload := map[string]interface{}{
			"body":     "delayed message with ms",
			"tag":      "test",
			"delay_ms": 5000, // 5 seconds
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/topics/%s/messages", topicName), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[api.ProduceDelayResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}

		if resp.Data.ID == "" {
			t.Error("Expected message ID, got empty string")
		}

		if resp.Data.ScheduledAt.IsZero() {
			t.Error("Expected scheduled_at time, got zero time")
		}

		// Check that execute_at is approximately 5 seconds in the future
		expectedExecuteAt := time.Now().Add(5 * time.Second)
		diff := resp.Data.ExecuteAt.Sub(expectedExecuteAt).Abs()
		if diff > 2*time.Second {
			t.Errorf("Expected execute_at around %v, got %v (diff: %v)", expectedExecuteAt, resp.Data.ExecuteAt, diff)
		}
	})

	t.Run("ProduceDelayedMessageWithSeconds", func(t *testing.T) {
		// Produce a delayed message using delay_sec
		payload := map[string]interface{}{
			"body":      "delayed message with sec",
			"tag":       "test",
			"delay_sec": 10, // 10 seconds
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/topics/%s/messages", topicName), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[api.ProduceDelayResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}

		if resp.Data.ID == "" {
			t.Error("Expected message ID, got empty string")
		}

		if resp.Data.ScheduledAt.IsZero() {
			t.Error("Expected scheduled_at time, got zero time")
		}

		// Check that execute_at is approximately 10 seconds in the future
		expectedExecuteAt := time.Now().Add(10 * time.Second)
		diff := resp.Data.ExecuteAt.Sub(expectedExecuteAt).Abs()
		if diff > 2*time.Second {
			t.Errorf("Expected execute_at around %v, got %v (diff: %v)", expectedExecuteAt, resp.Data.ExecuteAt, diff)
		}
	})

	t.Run("ProduceBothDelayParametersError", func(t *testing.T) {
		// Try to produce with both delay_ms and delay_sec (should fail)
		payload := map[string]interface{}{
			"body":      "invalid message",
			"tag":       "test",
			"delay_ms":  5000,
			"delay_sec": 10,
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/topics/%s/messages", topicName), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[interface{}]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code == api.RespCodeOk {
			t.Error("Expected error code, got 'ok'")
		}
	})

	t.Run("ProduceDelayOutOfRangeError", func(t *testing.T) {
		// Try to produce with delay exceeding 30 days (should fail)
		payload := map[string]interface{}{
			"body":      "invalid message",
			"tag":       "test",
			"delay_sec": 2592001, // 30 days + 1 second
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", fmt.Sprintf("/api/v1/topics/%s/messages", topicName), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[interface{}]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code == api.RespCodeOk {
			t.Error("Expected error code, got 'ok'")
		}
	})

	t.Run("ProduceToNonExistentTopic", func(t *testing.T) {
		// Try to produce to a non-existent topic
		payload := map[string]interface{}{
			"body": "message to nowhere",
			"tag":  "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/non-existent/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("Expected 404, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[interface{}]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeTopicNotFound {
			t.Errorf("Expected code 'topic_not_found', got %s", resp.Code)
		}
	})
}
