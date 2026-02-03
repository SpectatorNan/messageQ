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

func TestStatefulRetryOrder(t *testing.T) {
	dir := getTestDataDir(t, "stateful")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 1)
	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	// produce 5 messages
	ids := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		body := map[string]string{"body": "msg", "tag": "t"}
		bts, _ := json.Marshal(body)
		resp, err := client.Post(s.URL+"/topics/t/messages", "application/json", bytes.NewReader(bts))
		if err != nil {
			t.Fatalf("produce failed: %v", err)
		}
		var payload struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		idAny := payload.Data["id"]
		if id, ok := idAny.(string); ok {
			ids = append(ids, id)
		}
	}

	consume := func() string {
		url := s.URL + "/topics/t/messages?group=g&queue_id=0"
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("consume failed: %v", err)
		}
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			return "" // No message available
		}
		
		var payload struct {
			Data map[string]interface{} `json:"data"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&payload)
		msgAny := payload.Data["message"]
		msgMap, _ := msgAny.(map[string]interface{})
		id, _ := msgMap["id"].(string)
		return id
	}

	ack := func(id string) {
		resp, err := client.Post(s.URL+"/topics/t/messages/"+id+"/ack", "", nil)
		if err != nil {
			t.Fatalf("ack failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("ack status=%d", resp.StatusCode)
		}
	}

	nack := func(id string) {
		resp, err := client.Post(s.URL+"/topics/t/messages/"+id+"/nack", "", nil)
		if err != nil {
			t.Fatalf("nack failed: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("nack status=%d", resp.StatusCode)
		}
	}

	// consume first three with state transitions
	id1 := consume()
	ack(id1)

	id2 := consume()
	nack(id2)

	// With delay scheduler: NACK triggers ~1s retry delay
	// So next consume will get id3 immediately
	id3 := consume()
	ack(id3)

	id4 := consume()
	ack(id4)

	id5 := consume()
	ack(id5)

	// Wait for retry delay (~1s + buffer)
	time.Sleep(1500 * time.Millisecond)

	// Now we should get the retried id2 (after delay) - may need multiple attempts
	var id2Retry string
	maxAttempts := 5
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id2Retry = consume()
		if id2Retry != "" {
			break
		}
		if attempt < maxAttempts-1 {
			time.Sleep(300 * time.Millisecond)
		}
	}
	
	if id2Retry != id2 {
		t.Fatalf("expected retry id %s, got %s", id2, id2Retry)
	}
	ack(id2Retry)

	t.Logf("✅ Consumed all 5 messages with delay-scheduled retry (id2 retried after ~1s backoff)")
	_ = ids
}
