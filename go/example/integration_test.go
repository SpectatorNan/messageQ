package example

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

func TestProduceConsumeAckNack_PersistentWAL(t *testing.T) {
	// use helper to determine data directory
	dir := getTestDataDir(t, "integration")

	// create WAL-backed broker and router
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer func() { _ = store.Close() }()
	b := broker.NewBrokerWithStorage(store, 4)
	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	// produce a message
	body := map[string]string{"body": "hello persistent", "tag": "t"}
	bts, _ := json.Marshal(body)
	resp, err := client.Post(s.URL+"/topics/test/messages", "application/json", bytes.NewReader(bts))
	if err != nil {
		t.Fatalf("produce request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		buf, _ := io.ReadAll(resp.Body)
		t.Fatalf("produce returned status %d: %s", resp.StatusCode, string(buf))
	}

	// close server and storage to simulate process restart
	s.Close()
	if err := store.Close(); err != nil {
		t.Fatalf("close store failed: %v", err)
	}

	// reopen WAL and new broker/router
	store2 := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer func() { _ = store2.Close() }()
	b2 := broker.NewBrokerWithStorage(store2, 4)
	r2 := api.NewRouter(b2)
	s2 := httptest.NewServer(r2)
	defer s2.Close()

	// consume the message after restart
	resp2, err := client.Get(s2.URL + "/topics/test/messages?group=g1")
	if err != nil {
		t.Fatalf("consume request failed after restart: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 200 {
		buf, _ := io.ReadAll(resp2.Body)
		t.Fatalf("consume returned status %d after restart: %s", resp2.StatusCode, string(buf))
	}
	var rbody struct {
		Code    string                 `json:"code"`
		Message string                 `json:"message"`
		Data    map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&rbody)
	msgAny, ok := rbody.Data["message"]
	if !ok {
		t.Fatalf("consume data missing message after restart")
	}
	msgMap, ok := msgAny.(map[string]interface{})
	if !ok {
		t.Fatalf("invalid message payload")
	}
	idf, ok := msgMap["id"]
	if !ok {
		t.Fatalf("consume data missing id after restart")
	}
	id, ok := idf.(string)
	if !ok || id == "" {
		t.Fatalf("expected string id, got %#v", idf)
	}

	// ack to advance offset
	ackResp, err := client.Post(s2.URL+"/topics/test/messages/"+id+"/ack", "", nil)
	if err != nil {
		t.Fatalf("ack request failed after restart: %v", err)
	}
	ackResp.Body.Close()
	if ackResp.StatusCode != 200 {
		buf, _ := io.ReadAll(ackResp.Body)
		t.Fatalf("ack returned status %d after restart: %s", ackResp.StatusCode, string(buf))
	}

	// produce another message and consume again
	body2 := map[string]string{"body": "hello next", "tag": "t"}
	b2ts, _ := json.Marshal(body2)
	_, _ = client.Post(s2.URL+"/topics/test/messages", "application/json", bytes.NewReader(b2ts))

	resp3, _ := client.Get(s2.URL + "/topics/test/messages?group=g1")
	var rbody3 struct {
		Code    string                 `json:"code"`
		Message string                 `json:"message"`
		Data    map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp3.Body).Decode(&rbody3)
	msgAny2, ok := rbody3.Data["message"]
	if !ok {
		t.Fatalf("consume data missing message")
	}
	msgMap2, ok := msgAny2.(map[string]interface{})
	if !ok {
		t.Fatalf("invalid message payload")
	}
	idf2 := msgMap2["id"]
	id2, ok := idf2.(string)
	if !ok || id2 == "" {
		t.Fatalf("expected string id, got %#v", idf2)
	}
	if id2 == id {
		t.Fatalf("expected different message on second consume")
	}
}

func TestProduceData(t *testing.T) {

	topic := "normal"
	client := &http.Client{Timeout: 5 * time.Second}
	produce := func() error {
		msg := fmt.Sprintf("hello concurrent %d", time.Now().UnixNano())
		body := map[string]string{"body": msg, "tag": "testProduce"}
		bts, _ := json.Marshal(body)
		_, err := client.Post("http://localhost:8080/api/v1/topics/"+topic+"/messages", "application/json", bytes.NewReader(bts))
		if err != nil {
			//t.Errorf("produce request failed: %v", err)
			return err
		}
		return nil
	}
	for i := 0; i < 5; i++ {
		err := produce()
		if err != nil {
			t.Errorf("produce failed: %v", err)
			break
		}
	}
}
