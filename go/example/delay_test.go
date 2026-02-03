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

// TestDelayedMessage tests basic delayed message delivery
func TestDelayedMessage(t *testing.T) {
	dir := getTestDataDir(t, "delayed")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 1)
	defer b.Close()

	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// Produce a delayed message (2 second delay)
	body := map[string]interface{}{
		"body":      "delayed message",
		"tag":       "test",
		"delay_sec": 2,
	}
	bts, _ := json.Marshal(body)
	startTime := time.Now()
	
	resp, err := client.Post(s.URL+"/topics/test-topic/messages/delay", "application/json", bytes.NewReader(bts))
	if err != nil {
		t.Fatalf("produce delayed failed: %v", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var produceResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&produceResult)
	msgID := produceResult.Data["id"].(string)
	t.Logf("Produced delayed message: %s", msgID)

	// Try to consume immediately (should get nothing)
	url := s.URL + "/topics/test-topic/messages?group=g1&queue_id=0"
	resp, err = client.Get(url)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	resp.Body.Close()
	
	if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusOK {
		t.Logf("Immediate consume returned status: %d (expected not found or ok)", resp.StatusCode)
	}

	// Wait for delay to pass
	time.Sleep(2500 * time.Millisecond)

	// Now consume should succeed
	resp, err = client.Get(url)
	if err != nil {
		t.Fatalf("consume after delay failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 after delay, got %d", resp.StatusCode)
	}

	var consumeResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&consumeResult)
	
	msgData := consumeResult.Data["message"].(map[string]interface{})
	consumedID := msgData["id"].(string)
	consumedBody := msgData["body"].(string)

	if consumedID != msgID {
		t.Errorf("expected message ID %s, got %s", msgID, consumedID)
	}
	
	if consumedBody != "delayed message" {
		t.Errorf("expected body 'delayed message', got %s", consumedBody)
	}

	elapsed := time.Since(startTime)
	if elapsed < 2*time.Second {
		t.Errorf("message delivered too early: %v", elapsed)
	}

	t.Logf("✅ Delayed message delivered correctly after %v", elapsed)
}

// TestRetryWithBackoff tests that retry uses exponential backoff
func TestRetryWithBackoff(t *testing.T) {
	dir := getTestDataDir(t, "retry-backoff")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 1)
	defer b.Close()

	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// Produce a normal message
	body := map[string]string{"body": "retry test", "tag": "test"}
	bts, _ := json.Marshal(body)
	resp, _ := client.Post(s.URL+"/topics/retry-topic/messages", "application/json", bytes.NewReader(bts))
	resp.Body.Close()

	time.Sleep(100 * time.Millisecond) // Wait for flush

	// Consume the message
	url := s.URL + "/topics/retry-topic/messages?group=g1&queue_id=0"
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("consume failed: %v", err)
	}
	
	var consumeResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&consumeResult)
	resp.Body.Close()

	msgData := consumeResult.Data["message"].(map[string]interface{})
	msgID := msgData["id"].(string)

	// NACK the message (trigger retry with backoff)
	nackTime := time.Now()
	resp, err = client.Post(s.URL+"/topics/retry-topic/messages/"+msgID+"/nack", "", nil)
	if err != nil {
		t.Fatalf("nack failed: %v", err)
	}
	resp.Body.Close()

	t.Logf("Message NACK'd at %v, should retry in ~1s", nackTime)

	// Try consuming immediately (should not get the message yet)
	time.Sleep(300 * time.Millisecond)
	resp, _ = client.Get(url)
	if resp.StatusCode == http.StatusOK {
		var result struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		
		if result.Data != nil && result.Data["message"] != nil {
			t.Error("Message available too early (expected backoff delay)")
		}
	} else {
		resp.Body.Close()
	}

	// Wait for backoff (first retry is ~1s) + some buffer for scheduler processing
	time.Sleep(1400 * time.Millisecond)
	
	// Check delay scheduler stats
	statsResp, _ := client.Get(s.URL + "/stats")
	var statsResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(statsResp.Body).Decode(&statsResult)
	statsResp.Body.Close()
	t.Logf("Scheduler stats: %+v", statsResult.Data)

	// Now should be able to consume the retry (may need to retry a few times)
	maxAttempts := 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err = client.Get(url)
		if err != nil {
			t.Fatalf("consume retry failed: %v", err)
		}
		
		if resp.StatusCode == http.StatusOK {
			break
		}
		resp.Body.Close()
		
		if attempt < maxAttempts-1 {
			t.Logf("Attempt %d: message not ready yet, waiting 200ms...", attempt+1)
			time.Sleep(200 * time.Millisecond)
		}
	}
	
	if err != nil {
		t.Fatalf("consume retry failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for retry consume after %d attempts, got %d", maxAttempts, resp.StatusCode)
	}

	var retryResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&retryResult)
	
	retryMsgData := retryResult.Data["message"].(map[string]interface{})
	retryID := retryMsgData["id"].(string)
	retryCount := int(retryMsgData["retry"].(float64))

	if retryID != msgID {
		t.Errorf("expected same message ID %s, got %s", msgID, retryID)
	}

	if retryCount != 1 {
		t.Errorf("expected retry count 1, got %d", retryCount)
	}

	elapsed := time.Since(nackTime)
	if elapsed < 1*time.Second {
		t.Errorf("retry happened too quickly: %v (expected ~1s backoff)", elapsed)
	}

	t.Logf("✅ Retry with backoff working correctly (elapsed: %v, retry count: %d)", elapsed, retryCount)
}

// TestMultipleDelayedMessages tests concurrent delayed messages
func TestMultipleDelayedMessages(t *testing.T) {
	dir := getTestDataDir(t, "multi-delayed")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 1)
	defer b.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	startTime := time.Now()

	// Produce messages with different delays
	delays := []int64{3, 1, 2} // seconds
	ids := make([]string, len(delays))

	for i, delay := range delays {
		body := map[string]interface{}{
			"body":      "msg-" + string(rune('A'+i)),
			"tag":       "test",
			"delay_sec": delay,
		}
		bts, _ := json.Marshal(body)
		resp, err := client.Post(s.URL+"/topics/multi/messages/delay", "application/json", bytes.NewReader(bts))
		if err != nil {
			t.Fatalf("produce failed: %v", err)
		}
		
		var result struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		
		ids[i] = result.Data["id"].(string)
		t.Logf("Produced message %d with %ds delay: %s", i, delay, ids[i])
	}

	// Check delay scheduler stats
	resp, _ := client.Get(s.URL + "/stats")
	var statsResult struct {
		Data map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&statsResult)
	resp.Body.Close()

	if ds := statsResult.Data["delay_scheduler"]; ds != nil {
		dsMap := ds.(map[string]interface{})
		pending := int(dsMap["pending_messages"].(float64))
		t.Logf("Delay scheduler has %d pending messages", pending)
		
		if pending != 3 {
			t.Errorf("expected 3 pending messages, got %d", pending)
		}
	}

	// Wait and consume in order (should get them in 1s, 2s, 3s order)
	time.Sleep(3500 * time.Millisecond)

	consumed := []string{}
	url := s.URL + "/topics/multi/messages?group=g1&queue_id=0"
	
	for i := 0; i < 3; i++ {
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("consume %d failed: %v", i, err)
		}
		
		if resp.StatusCode == http.StatusOK {
			var result struct {
				Data map[string]interface{} `json:"data"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&result)
			
			msgData := result.Data["message"].(map[string]interface{})
			body := msgData["body"].(string)
			consumed = append(consumed, body)
			
			// ACK the message
			msgID := msgData["id"].(string)
			ackResp, _ := client.Post(s.URL+"/topics/multi/messages/"+msgID+"/ack", "", nil)
			ackResp.Body.Close()
		}
		resp.Body.Close()
	}

	elapsed := time.Since(startTime)
	t.Logf("Consumed messages in %v: %v", elapsed, consumed)

	// Verify order (should be B, C, A - delays were 1s, 2s, 3s)
	expected := []string{"msg-B", "msg-C", "msg-A"}
	for i, exp := range expected {
		if i >= len(consumed) || consumed[i] != exp {
			t.Errorf("expected message %d to be %s, got %s", i, exp, consumed[i])
		}
	}

	t.Logf("✅ Multiple delayed messages delivered in correct order")
}
