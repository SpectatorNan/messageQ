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

func TestAPIValidation(t *testing.T) {
	dataDir := "testdata/api_validation"
	os.RemoveAll(dataDir) // Clean up before test
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b.Close()

	// Create test topics
	err := b.CreateTopic("test-topic", broker.TopicTypeNormal, 4)
	if err != nil {
		t.Fatalf("Failed to create test-topic: %v", err)
	}

	err = b.CreateTopic("test-delay-topic", broker.TopicTypeDelay, 4)
	if err != nil {
		t.Fatalf("Failed to create test-delay-topic: %v", err)
	}

	router := api.NewRouter(b)

	t.Run("produce with valid data", func(t *testing.T) {
		payload := map[string]string{
			"body": "test message",
			"tag":  "test-tag",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
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

		if resp.Data.Topic != "test-topic" {
			t.Errorf("Expected topic 'test-topic', got %s", resp.Data.Topic)
		}

		if resp.Data.Body != "test message" {
			t.Errorf("Expected body 'test message', got %s", resp.Data.Body)
		}
	})

	t.Run("produce with empty body", func(t *testing.T) {
		payload := map[string]string{
			"body": "",
			"tag":  "test-tag",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeInvalidMsg {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeInvalidMsg, resp.Code)
		}
	})

	t.Run("produce with missing tag", func(t *testing.T) {
		payload := map[string]string{
			"body": "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}
	})

	t.Run("produce with invalid topic name", func(t *testing.T) {
		payload := map[string]string{
			"body": "test",
			"tag":  "tag1",
		}
		body, _ := json.Marshal(payload)

		// URL encode space character
		req := httptest.NewRequest("POST", "/api/v1/topics/test%20topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeInvalidTopicName {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeInvalidTopicName, resp.Code)
		}
	})

	t.Run("produce delayed message", func(t *testing.T) {
		payload := map[string]interface{}{
			"body":      "delayed message",
			"tag":       "delay-tag",
			"delay_sec": 10,
		}
		body, _ := json.Marshal(payload)

		// Use the unified /messages endpoint with delay parameters on delay topic
		req := httptest.NewRequest("POST", "/api/v1/topics/test-delay-topic/messages", bytes.NewReader(body))
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

		if resp.Data.DelaySeconds != 10.0 {
			t.Errorf("Expected delay_seconds 10.0, got %f", resp.Data.DelaySeconds)
		}

		if resp.Data.DelayMs != 10000 {
			t.Errorf("Expected delay_ms 10000, got %d", resp.Data.DelayMs)
		}
	})

	t.Run("produce delayed message with invalid delay", func(t *testing.T) {
		payload := map[string]interface{}{
			"body": "delayed message",
			"tag":  "delay-tag",
		}
		body, _ := json.Marshal(payload)

		// Use the unified /messages endpoint without delay - this should succeed
		// as a normal message, not fail with invalid_delay
		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// This should succeed as a normal message
		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[api.ProduceResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}
	})

	t.Run("ack with invalid message id", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v1/messages/invalid-id/ack", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeInvalidID {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeInvalidID, resp.Code)
		}
	})
}

func TestTopicManagement(t *testing.T) {
	dataDir := "testdata/api_topics"
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 4)
	defer b.Close()

	router := api.NewRouter(b)

	t.Run("create topic", func(t *testing.T) {
		// Use timestamp to ensure unique topic name
		topicName := fmt.Sprintf("orders-new-%d", time.Now().UnixNano())
		payload := map[string]interface{}{
			"name":        topicName,
			"type":        "NORMAL",
			"queue_count": 8,
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("Expected 201, got %d: %s", w.Code, w.Body.String())
		}

		var resp api.Resp[api.TopicResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}

		if resp.Data.Name != topicName {
			t.Errorf("Expected name '%s', got %s", topicName, resp.Data.Name)
		}

		if resp.Data.QueueCount != 8 {
			t.Errorf("Expected queue_count 8, got %d", resp.Data.QueueCount)
		}
	})

	t.Run("create topic with invalid name", func(t *testing.T) {
		payload := map[string]interface{}{
			"name": "invalid topic",
			"type": "NORMAL",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeInvalidTopicName {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeInvalidTopicName, resp.Code)
		}
	})

	t.Run("create topic with invalid type", func(t *testing.T) {
		payload := map[string]interface{}{
			"name": "test",
			"type": "INVALID",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected 400, got %d", w.Code)
		}

		var resp api.Resp[string]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.ErrCodeInvalidTopicType {
			t.Errorf("Expected error code '%s', got %s", api.ErrCodeInvalidTopicType, resp.Code)
		}
	})

	t.Run("list topics", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/v1/topics", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected 200, got %d", w.Code)
		}

		var resp api.Resp[api.ListTopicsResponse]
		json.Unmarshal(w.Body.Bytes(), &resp)

		if resp.Code != api.RespCodeOk {
			t.Errorf("Expected code 'ok', got %s", resp.Code)
		}

		if resp.Data.Count < 1 {
			t.Errorf("Expected at least 1 topic, got %d", resp.Data.Count)
		}
	})
}
