package example

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"messageQ/mq/api"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
)

var debugCleanup = false

// Global switch controlled by environment variable MSGQ_CLEAN_TESTDATA.
// If unset, default is true (testdata will be removed before/after the test).
// To keep the testdata for inspection set MSGQ_CLEAN_TESTDATA=false in your environment.
var cleanTestData = func() bool {
	v := os.Getenv("MSGQ_CLEAN_TESTDATA")
	if v == "" {
		return debugCleanup
	}
	switch strings.ToLower(v) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}()

func TestProduceConsumeAckNack_PersistentWAL(t *testing.T) {
	// clean testdata dir conditionally
	if cleanTestData {
		_ = os.RemoveAll("./testdata")
		defer func() { _ = os.RemoveAll("./testdata") }()
	}

	// create WAL-backed broker and router
	store := storage.NewWALStorage("./testdata", 10*time.Millisecond)
	defer func() { _ = store.Close() }()
	b := broker.NewBrokerWithStorage(store)
	r := api.NewRouter(b)
	s := httptest.NewServer(r)
	defer s.Close()

	client := &http.Client{Timeout: 5 * time.Second}
	// produce a message
	body := map[string]string{"body": "hello persistent"}
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
	store2 := storage.NewWALStorage("./testdata", 10*time.Millisecond)
	defer func() { _ = store2.Close() }()
	b2 := broker.NewBrokerWithStorage(store2)
	r2 := api.NewRouter(b2)
	s2 := httptest.NewServer(r2)
	defer s2.Close()

	// consume the message after restart
	resp2, err := client.Get(s2.URL + "/topics/test/messages")
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
	idf, ok := rbody.Data["id"]
	if !ok {
		t.Fatalf("consume data missing id after restart")
	}
	id := int64(idf.(float64))

	// ack the message
	ackResp, err := client.Post(s2.URL+"/topics/test/messages/"+strconv.FormatInt(id, 10)+"/ack", "", nil)
	if err != nil {
		t.Fatalf("ack request failed after restart: %v", err)
	}
	defer ackResp.Body.Close()
	if ackResp.StatusCode != 200 {
		buf, _ := io.ReadAll(ackResp.Body)
		t.Fatalf("ack returned status %d after restart: %s", ackResp.StatusCode, string(buf))
	}

	// produce another message and test nack->requeue on the live broker
	body2 := map[string]string{"body": "hello nack"}
	b2ts, _ := json.Marshal(body2)
	_, _ = client.Post(s2.URL+"/topics/test/messages", "application/json", bytes.NewReader(b2ts))

	resp3, _ := client.Get(s2.URL + "/topics/test/messages")
	var rbody3 struct {
		Code    string                 `json:"code"`
		Message string                 `json:"message"`
		Data    map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp3.Body).Decode(&rbody3)
	idf2 := rbody3.Data["id"]
	id2 := int64(idf2.(float64))

	// nack it
	_, _ = client.Post(s2.URL+"/topics/test/messages/"+strconv.FormatInt(id2, 10)+"/nack", "", nil)

	// consume again (it should be requeued)
	resp4, err := client.Get(s2.URL + "/topics/test/messages")
	if err != nil {
		t.Fatalf("consume after nack failed: %v", err)
	}
	defer resp4.Body.Close()
	if resp4.StatusCode != 200 {
		buf, _ := io.ReadAll(resp4.Body)
		t.Fatalf("consume after nack returned status %d: %s", resp4.StatusCode, string(buf))
	}
	var rbody4 struct {
		Code    string                 `json:"code"`
		Message string                 `json:"message"`
		Data    map[string]interface{} `json:"data"`
	}
	_ = json.NewDecoder(resp4.Body).Decode(&rbody4)
	if rbody4.Data["id"] == nil {
		t.Fatalf("consume after nack missing id")
	}
}
