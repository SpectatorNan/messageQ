package broker_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"messageQ/mq/broker"
	"messageQ/mq/logger"
	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

func TestMessageProcessingTimeout(t *testing.T) {
	logger.Init(logger.Config{
		Level:      "info",
		OutputPath: "stdout",
		Format:     "console",
	})

	// Create a temporary data directory for testing
	tmpDir := filepath.Join(os.TempDir(), fmt.Sprintf("mq-test-timeout-%d", time.Now().Unix()))
	defer os.RemoveAll(tmpDir)

	// Initialize storage
	store := storage.NewWALStorage(tmpDir, 10*time.Millisecond)
	defer store.Close()

	// Initialize the broker with a short processing timeout
	b := broker.NewBrokerWithPersistence(store, 1, tmpDir)
	defer b.Close()

	// Create a normal topic
	topicName := "timeout-test"
	err := b.CreateTopic(topicName, broker.TopicTypeNormal, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// Produce a message
	msg := b.Enqueue(topicName, "test message", "test-tag")
	if msg.ID == "" {
		t.Fatal("Failed to produce message")
	}

	t.Run("MessageTimeout", func(t *testing.T) {
		// Consume the message
		group := "test-group"
		msgs, offset, nextOffset, err := b.ConsumeWithLock(group, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Failed to consume: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatal("No messages to consume")
		}

		consumedMsg := msgs[0]

		// Begin processing
		b.BeginProcessing(group, topicName, 0, offset, nextOffset, queue.Message{
			ID:        consumedMsg.ID,
			Body:      consumedMsg.Body,
			Tag:       consumedMsg.Tag,
			Retry:     consumedMsg.Retry,
			Timestamp: consumedMsg.Timestamp,
		})

		// Check stats - should show 1 processing
		stats := b.Stats()
		if stats[group].Processing != 1 {
			t.Errorf("Expected 1 processing, got %d", stats[group].Processing)
		}

		// Don't ack or nack - simulate timeout
		// Wait for timeout check to trigger (default timeout is 30s, check interval is 5s)
		// For testing, we need to wait longer than the timeout
		t.Log("Waiting for processing timeout (35 seconds)...")
		time.Sleep(35 * time.Second)

		// Check stats again - processing should be back to 0, retry should be 1
		stats = b.Stats()
		if stats[group].Processing != 0 {
			t.Errorf("Expected 0 processing after timeout, got %d", stats[group].Processing)
		}
		if stats[group].Retry != 1 {
			t.Errorf("Expected 1 retry after timeout, got %d", stats[group].Retry)
		}

		// Try to consume again - should get the message back
		msgs2, _, _, err := b.ConsumeWithLock(group, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Failed to consume after timeout: %v", err)
		}
		if len(msgs2) == 0 {
			t.Error("Expected to consume retried message, got none")
		} else {
			if msgs2[0].ID != consumedMsg.ID {
				t.Errorf("Expected message ID %s, got %s", consumedMsg.ID, msgs2[0].ID)
			}
			if msgs2[0].Retry != 1 {
				t.Errorf("Expected retry count 1, got %d", msgs2[0].Retry)
			}
		}
	})

	t.Run("MessageAckBeforeTimeout", func(t *testing.T) {
		// Produce another message
		msg := b.Enqueue(topicName, "test message 2", "test-tag")
		if msg.ID == "" {
			t.Fatal("Failed to produce message")
		}

		// Consume the message
		group := "test-group-2"
		msgs, offset, nextOffset, err := b.ConsumeWithLock(group, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Failed to consume: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatal("No messages to consume")
		}

		consumedMsg := msgs[0]

		// Begin processing
		b.BeginProcessing(group, topicName, 0, offset, nextOffset, queue.Message{
			ID:        consumedMsg.ID,
			Body:      consumedMsg.Body,
			Tag:       consumedMsg.Tag,
			Retry:     consumedMsg.Retry,
			Timestamp: consumedMsg.Timestamp,
		})

		// Ack the message immediately
		if !b.CompleteProcessing(consumedMsg.ID) {
			t.Error("Failed to ack message")
		}

		// Check stats - processing should be 0, completed should be 1
		stats := b.Stats()
		if stats[group].Processing != 0 {
			t.Errorf("Expected 0 processing after ack, got %d", stats[group].Processing)
		}
		if stats[group].Completed != 1 {
			t.Errorf("Expected 1 completed after ack, got %d", stats[group].Completed)
		}

		// Wait a bit to ensure no timeout processing happens
		time.Sleep(10 * time.Second)

		// Stats should remain the same
		stats = b.Stats()
		if stats[group].Processing != 0 {
			t.Errorf("Expected 0 processing after wait, got %d", stats[group].Processing)
		}
		if stats[group].Completed != 1 {
			t.Errorf("Expected 1 completed after wait, got %d", stats[group].Completed)
		}
		if stats[group].Retry != 0 {
			t.Errorf("Expected 0 retry after ack, got %d", stats[group].Retry)
		}
	})
}
