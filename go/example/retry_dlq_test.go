package example

import (
	"testing"
	"time"

	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

func TestRetryPersistedAcrossRestart(t *testing.T) {
	dir := getTestDataDir(t, "retry")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	q := queue.NewQueueWithStorage(store, "retry-topic", 0)
	msg := q.Enqueue("retry me", "t")
	received := q.Dequeue()
	if received.ID != msg.ID {
		t.Fatalf("expected dequeue id %s, got %s", msg.ID, received.ID)
	}
	if !q.Nack(received.ID) {
		t.Fatalf("nack failed for id %s", received.ID)
	}

	// ensure data flushed
	if err := store.Close(); err != nil {
		t.Fatalf("store close failed: %v", err)
	}

	store2 := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store2.Close()
	q2 := queue.NewQueueWithStorage(store2, "retry-topic", 0)
	// commitlog-only mode may include multiple entries; ensure at least one retry>=1
	var maxRetry int
	for i := 0; i < 3; i++ {
		m := q2.Dequeue()
		if m.Retry > maxRetry {
			maxRetry = m.Retry
		}
		if maxRetry >= 1 {
			break
		}
	}
	if maxRetry < 1 {
		t.Fatalf("expected retry>=1 after restart, got %d", maxRetry)
	}
}

func TestDLQPersisted(t *testing.T) {
	dir := getTestDataDir(t, "dlq")
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	topic := "dlq-topic"
	q := queue.NewQueueWithStorage(store, topic, 0)
	msg := q.Enqueue("dead letter", "t")

	// consume and nack enough times to exceed maxRetry (default 3)
	for i := 0; i < 4; i++ {
		received := q.Dequeue()
		if received.ID != msg.ID {
			t.Fatalf("unexpected id at iteration %d: %s", i, received.ID)
		}
		if !q.Nack(received.ID) {
			t.Fatalf("nack failed at iteration %d", i)
		}
	}

	// flush to disk
	if err := store.Close(); err != nil {
		t.Fatalf("store close failed: %v", err)
	}

	store2 := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store2.Close()

	// active topic may still contain messages in commitlog-only mode
	active, err := store2.Load(topic, 0)
	if err != nil {
		t.Fatalf("load active topic failed: %v", err)
	}
	if len(active) == 0 {
		t.Fatalf("expected active topic to contain messages in commitlog-only mode")
	}

	dlqMsgs, err := store2.Load(topic+".dlq", 0)
	if err != nil {
		t.Fatalf("load dlq failed: %v", err)
	}
	if len(dlqMsgs) == 0 {
		t.Fatalf("expected dlq to contain msg %s, got empty", msg.ID)
	}
	if dlqMsgs[len(dlqMsgs)-1].ID != msg.ID {
		t.Fatalf("expected dlq to contain msg %s, got %#v", msg.ID, dlqMsgs)
	}
}
