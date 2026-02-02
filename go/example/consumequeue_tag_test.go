package example

import (
	"testing"
	"time"

	"messageQ/mq/storage"
)

func TestConsumeQueueTagFilter(t *testing.T) {
	dir := getTestDataDir(t, "cqtag")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	_ = store.Append("t", 0, storage.Message{ID: "00000000-0000-0000-0000-000000000001", Body: "a1", Tag: "a", Retry: 0, Timestamp: time.Now()})
	_ = store.Append("t", 0, storage.Message{ID: "00000000-0000-0000-0000-000000000002", Body: "b1", Tag: "b", Retry: 0, Timestamp: time.Now()})
	_ = store.Append("t", 0, storage.Message{ID: "00000000-0000-0000-0000-000000000003", Body: "a2", Tag: "a", Retry: 0, Timestamp: time.Now()})

	msgs, next, err := store.ReadFromConsumeQueue("t", 0, 0, 10, "b")
	if err != nil {
		t.Fatalf("read consumequeue failed: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Tag != "b" {
		t.Fatalf("expected 1 tag=b msg, got %#v", msgs)
	}
	if next <= 0 {
		t.Fatalf("expected next offset > 0")
	}
}
