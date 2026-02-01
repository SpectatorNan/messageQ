package example

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

func TestRetryPersistedAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	q := queue.NewQueueWithStorage(store, "retry-topic")
	msg := q.Enqueue("retry me")
	received := q.Dequeue()
	if received.ID != msg.ID {
		t.Fatalf("expected dequeue id %d, got %d", msg.ID, received.ID)
	}
	if !q.Nack(received.ID) {
		t.Fatalf("nack failed for id %d", received.ID)
	}

	// ensure data flushed
	if err := store.Close(); err != nil {
		t.Fatalf("store close failed: %v", err)
	}

	store2 := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store2.Close()
	q2 := queue.NewQueueWithStorage(store2, "retry-topic")
	recovered := q2.Dequeue()
	if recovered.Retry != 1 {
		t.Fatalf("expected retry=1 after restart, got %d", recovered.Retry)
	}
}

func TestDLQPersisted(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()

	topic := "dlq-topic"
	q := queue.NewQueueWithStorage(store, topic)
	msg := q.Enqueue("dead letter")

	// consume and nack enough times to exceed maxRetry (default 3)
	for i := 0; i < 4; i++ {
		received := q.Dequeue()
		if received.ID != msg.ID {
			t.Fatalf("unexpected id at iteration %d: %d", i, received.ID)
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

	// active topic should be empty
	active, err := store2.Load(topic)
	if err != nil {
		t.Fatalf("load active topic failed: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected active topic empty, got %d messages", len(active))
	}

	dlqMsgs, err := store2.Load(topic + ".dlq")
	if err != nil {
		t.Fatalf("load dlq failed: %v", err)
	}
	if len(dlqMsgs) == 0 {
		// log directory contents for diagnostics
		dlqDir := filepath.Join(dir, topic+".dlq")
		entries, _ := os.ReadDir(dlqDir)
		names := []string{}
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("expected dlq to contain msg %d, got empty. dlq dir=%s entries=%v", msg.ID, dlqDir, names)
	}
	if dlqMsgs[len(dlqMsgs)-1].ID != msg.ID {
		t.Fatalf("expected dlq to contain msg %d, got %#v", msg.ID, dlqMsgs)
	}
}
