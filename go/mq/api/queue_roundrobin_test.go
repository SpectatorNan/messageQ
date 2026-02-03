package api_test

import (
	"bytes"
	"encoding/json"
	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

// TestQueueRoundRobin 测试多队列轮询消费
func TestQueueRoundRobin(t *testing.T) {
	dataDir := "testdata/roundrobin"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	// 创建 broker，4 个队列
	b := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b.Close()

	// 创建普通 topic，4个队列
	err := b.CreateTopic("test-topic", broker.TopicTypeNormal, 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	router := api.NewRouter(b)

	// 生产 8 条消息，应该分布到 4 个队列
	for i := 0; i < 8; i++ {
		payload := map[string]interface{}{
			"body": "test message " + string(rune('A'+i)),
			"tag":  "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("Failed to produce message %d: %v", i, w.Body.String())
		}
	}

	// 等待消息持久化
	time.Sleep(100 * time.Millisecond)

	// 不指定 queue_id，连续消费 8 次，应该能从不同队列消费到消息
	consumedQueueIDs := make(map[int]int) // queueID -> count
	consumedMessages := make(map[string]bool)

	for i := 0; i < 8; i++ {
		// 不指定 queue_id 参数
		req := httptest.NewRequest("GET", "/api/v1/consumers/test-group/topics/test-topic/messages", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Logf("Consume attempt %d failed: %v", i, w.Body.String())
			continue
		}

		var resp struct {
			Code string `json:"code"`
			Data struct {
				Message struct {
					ID   string `json:"id"`
					Body string `json:"body"`
				} `json:"message"`
				QueueID int `json:"queue_id"`
			} `json:"data"`
		}

		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response: %v", err)
		}

		queueID := resp.Data.QueueID
		msgID := resp.Data.Message.ID
		
		consumedQueueIDs[queueID]++
		consumedMessages[msgID] = true

		t.Logf("Consumed message %d: queue_id=%d, msg_id=%s, body=%s", 
			i, queueID, msgID, resp.Data.Message.Body)

		// Ack 消息
		ackReq := httptest.NewRequest("POST", "/api/v1/messages/"+msgID+"/ack", nil)
		ackW := httptest.NewRecorder()
		router.ServeHTTP(ackW, ackReq)
	}

	// 验证：应该从多个队列消费到消息（至少 2 个队列）
	t.Logf("Queue distribution: %v", consumedQueueIDs)
	
	if len(consumedQueueIDs) < 2 {
		t.Errorf("Expected messages from at least 2 queues, got %d queues: %v", 
			len(consumedQueueIDs), consumedQueueIDs)
	}

	// 验证：应该消费到 8 条不同的消息
	if len(consumedMessages) != 8 {
		t.Errorf("Expected 8 unique messages, got %d", len(consumedMessages))
	}

	t.Logf("✅ Round-robin queue consumption test passed: consumed from %d queues, %d unique messages",
		len(consumedQueueIDs), len(consumedMessages))
}

// TestQueueRoundRobinWithSpecificQueue 测试指定 queue_id 时的消费
func TestQueueRoundRobinWithSpecificQueue(t *testing.T) {
	dataDir := "testdata/specific_queue"
	os.RemoveAll(dataDir)
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b.Close()

	err := b.CreateTopic("test-topic", broker.TopicTypeNormal, 4)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	router := api.NewRouter(b)

	// 生产 4 条消息
	for i := 0; i < 4; i++ {
		payload := map[string]interface{}{
			"body": "message " + string(rune('A'+i)),
			"tag":  "test",
		}
		body, _ := json.Marshal(payload)

		req := httptest.NewRequest("POST", "/api/v1/topics/test-topic/messages", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != 200 {
			t.Fatalf("Failed to produce: %v", w.Body.String())
		}
	}

	time.Sleep(100 * time.Millisecond)

	// 指定消费 queue_id=1
	req := httptest.NewRequest("GET", "/api/v1/consumers/test-group/topics/test-topic/messages?queue_id=1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("Failed to consume from queue 1: %v", w.Body.String())
	}

	var resp struct {
		Code string `json:"code"`
		Data struct {
			QueueID int `json:"queue_id"`
		} `json:"data"`
	}

	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	// 验证：应该从 queue_id=1 消费
	if resp.Data.QueueID != 1 {
		t.Errorf("Expected queue_id=1, got %d", resp.Data.QueueID)
	}

	t.Logf("✅ Specific queue consumption test passed: consumed from queue_id=%d", resp.Data.QueueID)
}
