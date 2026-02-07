package example

import (
	"fmt"
	"messageQ/mq/broker"
	"os"
	"path/filepath"
	"testing"
	"time"

	"messageQ/mq/storage"
)

func TestQueueInitializationOnBrokerStart(t *testing.T) {
	// Create temporary directory
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("broker-queue-init-test-%d", time.Now().Unix()))
	defer os.RemoveAll(tmpDir)

	// Create storage
	store := storage.NewWALStorage(tmpDir, 10*time.Millisecond)
	defer store.Close()

	// Step 1: Create a broker and some topics
	b1 := broker.NewBrokerWithPersistence(store, 2, tmpDir)

	// Create topics
	err := b1.CreateTopic("topic1", broker.TopicTypeNormal, 2)
	if err != nil {
		t.Fatalf("Failed to create topic1: %v", err)
	}

	err = b1.CreateTopic("topic2", broker.TopicTypeDelay, 3)
	if err != nil {
		t.Fatalf("Failed to create topic2: %v", err)
	}

	// Enqueue some messages
	msg1 := b1.Enqueue("topic1", "message 1", "tag1")
	if msg1.ID == "" {
		t.Error("Failed to enqueue message to topic1")
	}

	msg2 := b1.Enqueue("topic2", "message 2", "tag2")
	if msg2.ID == "" {
		t.Error("Failed to enqueue message to topic2")
	}

	// Verify queues are created for topic1
	b1.lock.Lock()
	queues1, ok1 := b1.queues["topic1"]
	if !ok1 {
		t.Error("topic1 queues not found in broker")
	}
	if len(queues1) != 2 {
		t.Errorf("Expected 2 queues for topic1, got %d", len(queues1))
	}

	// Verify queues are created for topic2
	queues2, ok2 := b1.queues["topic2"]
	if !ok2 {
		t.Error("topic2 queues not found in broker")
	}
	if len(queues2) != 3 {
		t.Errorf("Expected 3 queues for topic2, got %d", len(queues2))
	}
	b1.lock.Unlock()

	// Close broker
	b1.Close()

	// Step 2: Create a new broker instance (simulating restart)
	store2 := storage.NewWALStorage(tmpDir, 10*time.Millisecond)
	defer store2.Close()

	b2 := broker.NewBrokerWithPersistence(store2, 2, tmpDir)
	defer b2.Close()

	// Give some time for initialization
	time.Sleep(100 * time.Millisecond)

	// Step 3: Verify that queues are automatically initialized for existing topics
	b2.lock.Lock()
	queues1Again, ok1Again := b2.queues["topic1"]
	queues2Again, ok2Again := b2.queues["topic2"]
	b2.lock.Unlock()

	if !ok1Again {
		t.Error("topic1 queues not automatically initialized on broker restart")
	}
	if len(queues1Again) != 2 {
		t.Errorf("Expected 2 queues for topic1 after restart, got %d", len(queues1Again))
	}

	if !ok2Again {
		t.Error("topic2 queues not automatically initialized on broker restart")
	}
	if len(queues2Again) != 3 {
		t.Errorf("Expected 3 queues for topic2 after restart, got %d", len(queues2Again))
	}

	// Step 4: Verify that reclaimLoop is running by checking queue functionality
	// Enqueue a message and try to dequeue it
	testMsg := b2.Enqueue("topic1", "test after restart", "test-tag")
	if testMsg.ID == "" {
		t.Error("Failed to enqueue message after restart")
	}

	// Try to consume the message
	group := "test-group"
	msgs, _, _, err := b2.ConsumeWithLock(group, "topic1", 0, "", 1)
	if err != nil {
		t.Errorf("Failed to consume message after restart: %v", err)
	}
	if len(msgs) == 0 {
		t.Error("Expected to consume at least one message")
	}

	t.Log("✅ Queue initialization and reclaimLoop verification passed")
}

func TestQueueInitializationWithNoTopics(t *testing.T) {
	// Test broker initialization when there are no topics
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("broker-no-topics-test-%d", time.Now().Unix()))
	defer os.RemoveAll(tmpDir)

	store := storage.NewWALStorage(tmpDir, 10*time.Millisecond)
	defer store.Close()

	b := broker.NewBrokerWithPersistence(store, 2, tmpDir)
	defer b.Close()

	// Should not panic, queues map should be empty
	b.lock.Lock()
	queueCount := len(b.queues)
	b.lock.Unlock()

	if queueCount != 0 {
		t.Errorf("Expected 0 queues for empty broker, got %d", queueCount)
	}

	t.Log("✅ Empty broker initialization passed")
}
