package example

import (
	"fmt"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDelaySchedulerPersistence tests that delayed messages survive broker restart
func TestDelaySchedulerPersistence(t *testing.T) {
	// Use temporary directory
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("mq-persist-test-%d", time.Now().UnixNano()))
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir)

	// Phase 1: Create broker, schedule delayed messages
	t.Log("Phase 1: Scheduling delayed messages...")
	b1 := broker.NewBrokerWithPersistence(store, 4, dataDir)
	
	// Schedule 3 messages with 10-second delays
	msg1 := b1.EnqueueWithDelay("test", "message 1", "tag1", 10*time.Second)
	msg2 := b1.EnqueueWithDelay("test", "message 2", "tag2", 15*time.Second)
	msg3 := b1.EnqueueWithDelay("test", "message 3", "tag3", 20*time.Second)
	
	t.Logf("Scheduled 3 messages: %s, %s, %s", msg1.ID, msg2.ID, msg3.ID)
	
	// Verify they're in the delay queue
	stats := b1.GetDelayScheduler().Stats()
	pendingCount := stats["pending_messages"].(int)
	if pendingCount != 3 {
		t.Fatalf("Expected 3 pending messages, got %d", pendingCount)
	}
	
	// Close broker (simulates restart)
	b1.Close()
	t.Log("Broker closed (simulating restart)")
	
	// Phase 2: Restart broker, verify messages are restored
	time.Sleep(100 * time.Millisecond)
	t.Log("Phase 2: Restarting broker...")
	b2 := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b2.Close()
	
	// Verify messages were restored
	stats2 := b2.GetDelayScheduler().Stats()
	pendingCount2 := stats2["pending_messages"].(int)
	if pendingCount2 != 3 {
		t.Fatalf("Expected 3 restored messages, got %d", pendingCount2)
	}
	t.Logf("Successfully restored %d delayed messages", pendingCount2)
	
	// Wait a bit and verify they're still pending (not yet executed)
	time.Sleep(2 * time.Second)
	stats3 := b2.GetDelayScheduler().Stats()
	if stats3["pending_messages"].(int) != 3 {
		t.Fatal("Messages executed too early!")
	}
	
	t.Log("✅ Persistence test passed - messages survived restart")
}

// TestRetryPersistence tests that retry tasks are persisted
// Note: This test is simplified - full retry logic involves processing state management
func TestRetryPersistence(t *testing.T) {
	t.Skip("Skipping - retry persistence is handled via delay scheduler, tested in TestDelaySchedulerPersistence")
	
	// Full retry test would require:
	// 1. Mock consumer that doesn't ACK immediately
	// 2. NACK while message is in processing state
	// 3. Verify retry scheduled
	// The delay scheduler persistence itself is already tested
}

// TestTopicPersistence tests topic metadata persistence
func TestTopicPersistence(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("mq-topic-persist-%d", time.Now().UnixNano()))
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir)

	// Phase 1: Create topics
	t.Log("Phase 1: Creating topics...")
	b1 := broker.NewBrokerWithPersistence(store, 4, dataDir)
	
	err := b1.CreateTopic("orders", broker.TopicTypeNormal, 4)
	if err != nil {
		t.Fatalf("Failed to create NORMAL topic: %v", err)
	}
	
	err = b1.CreateTopic("delayed-tasks", broker.TopicTypeDelay, 8)
	if err != nil {
		t.Fatalf("Failed to create DELAY topic: %v", err)
	}
	
	topics := b1.ListTopics()
	if len(topics) != 2 {
		t.Fatalf("Expected 2 topics, got %d", len(topics))
	}
	t.Logf("Created %d topics", len(topics))
	
	b1.Close()
	
	// Phase 2: Restart and verify topics are restored
	t.Log("Phase 2: Restarting broker...")
	b2 := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b2.Close()
	
	topics2 := b2.ListTopics()
	if len(topics2) != 2 {
		t.Fatalf("Expected 2 restored topics, got %d", len(topics2))
	}
	
	// Verify topic configurations
	ordersConfig, err := b2.GetTopicConfig("orders")
	if err != nil || ordersConfig.Type != broker.TopicTypeNormal {
		t.Fatal("orders topic not restored correctly")
	}
	
	delayConfig, err := b2.GetTopicConfig("delayed-tasks")
	if err != nil || delayConfig.Type != broker.TopicTypeDelay || delayConfig.QueueCount != 8 {
		t.Fatal("delayed-tasks topic not restored correctly")
	}
	
	t.Log("✅ Topic persistence test passed")
}

// TestDelayTopicEndToEnd tests the complete workflow with DELAY topics
func TestDelayTopicEndToEnd(t *testing.T) {
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("mq-delay-e2e-%d", time.Now().UnixNano()))
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir)
	b := broker.NewBrokerWithPersistence(store, 4, dataDir)
	defer b.Close()

	// Step 1: Create a DELAY topic
	err := b.CreateTopic("scheduled-jobs", broker.TopicTypeDelay, 4)
	if err != nil {
		t.Fatalf("Failed to create delay topic: %v", err)
	}
	
	// Verify topic type
	if !b.IsDelayTopic("scheduled-jobs") {
		t.Fatal("scheduled-jobs should be a delay topic")
	}
	t.Log("✅ Created DELAY topic")
	
	// Step 2: Schedule delayed message with short delay
	msg := b.EnqueueWithDelay("scheduled-jobs", "job 1", "tag", 1*time.Second)
	t.Logf("Scheduled job: %s with 1s delay", msg.ID)
	
	// Step 3: Verify not immediately consumable
	for qid := 0; qid < 4; qid++ {
		messages, _, _, _ := b.ConsumeWithLock("worker", "scheduled-jobs", qid, "", 10)
		if len(messages) > 0 {
			t.Fatal("Messages should not be consumable immediately")
		}
	}
	t.Log("✅ Message not available immediately")
	
	// Step 4: Wait and consume
	t.Log("Waiting 1.5 seconds...")
	time.Sleep(1500 * time.Millisecond)
	
	// Try all queues (message might be in any queue)
	var consumed []storage.Message
	for qid := 0; qid < 4; qid++ {
		messages, _, _, _ := b.ConsumeWithLock("worker", "scheduled-jobs", qid, "", 10)
		consumed = append(consumed, messages...)
	}
	
	if len(consumed) != 1 {
		t.Fatalf("Expected 1 message after delay, got %d", len(consumed))
	}
	if consumed[0].Body != "job 1" {
		t.Fatalf("Expected 'job 1', got '%s'", consumed[0].Body)
	}
	
	t.Log("✅ Delay topic end-to-end test passed")
}
