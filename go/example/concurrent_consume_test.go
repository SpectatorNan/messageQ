package example

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"messageQ/mq/broker"
	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

// TestConcurrentConsume tests that multiple consumers can consume different messages concurrently
func TestConcurrentConsume(t *testing.T) {
	baseDir := getTestDataDir(t, "concurrent")
	store := storage.NewWALStorage(baseDir, 10*time.Millisecond, 10*time.Second)
	defer store.Close()

	// Use only 1 queue to ensure all messages go to the same queue
	b := broker.NewBrokerWithStorage(store, 1)
	topic := "concurrent-test"
	group := "test-group"
	queueID := 0

	// Produce 10 messages (all will go to queue 0)
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		msg := b.Enqueue(topic, fmt.Sprintf("message-%d", i), "tagA")
		if msg.ID == "" {
			t.Fatalf("failed to enqueue message %d", i)
		}
	}

	// Give time for storage to flush
	time.Sleep(200 * time.Millisecond)

	// Consume messages concurrently with 3 consumers
	consumerCount := 3
	var wg sync.WaitGroup
	consumedMessages := make(map[string]bool)
	var mu sync.Mutex
	errors := []error{}

	for c := 0; c < consumerCount; c++ {
		wg.Add(1)
		consumerID := c
		go func() {
			defer wg.Done()
			for {
				// Try to consume a message
				msgs, offset, next, err := b.ConsumeWithLock(group, topic, queueID, "tagA", 1)
				if err != nil {
					mu.Lock()
					errors = append(errors, fmt.Errorf("consumer %d: %v", consumerID, err))
					mu.Unlock()
					return
				}

				if len(msgs) == 0 {
					// No more messages
					return
				}

				msg := msgs[0]
				t.Logf("Consumer %d consumed: %s (offset %d->%d)", consumerID, msg.Body, offset, next)

				// Begin processing (mark as in-flight)
				b.BeginProcessing(group, topic, queueID, offset, next, queue.Message{
					ID:        msg.ID,
					Body:      msg.Body,
					Tag:       msg.Tag,
					Retry:     msg.Retry,
					Timestamp: msg.Timestamp,
				})

				// Check for duplicate consumption
				mu.Lock()
				if consumedMessages[msg.ID] {
					errors = append(errors, fmt.Errorf("DUPLICATE: consumer %d consumed message %s that was already consumed", consumerID, msg.ID))
					mu.Unlock()
					return
				}
				consumedMessages[msg.ID] = true
				mu.Unlock()

				// Simulate processing
				time.Sleep(10 * time.Millisecond)

				// Ack the message
				t.Logf("Consumer %d acking message %s", consumerID, msg.ID)
				if !b.CompleteProcessing(msg.ID) {
					mu.Lock()
					errors = append(errors, fmt.Errorf("consumer %d: failed to ack message %s", consumerID, msg.ID))
					mu.Unlock()
					// Continue anyway to see if we can consume more
				}
			}
		}()
	}

	// Wait for all consumers to finish
	wg.Wait()

	// Check for errors
	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
		t.FailNow()
	}

	// Verify all messages were consumed exactly once
	if len(consumedMessages) != messageCount {
		t.Errorf("Expected %d messages to be consumed, but got %d", messageCount, len(consumedMessages))
	}

	t.Logf("✅ Successfully consumed %d messages concurrently without duplicates", len(consumedMessages))
}

// TestConcurrentConsumeMultipleQueues tests concurrent consumption across multiple queues
func TestConcurrentConsumeMultipleQueues(t *testing.T) {
	baseDir := getTestDataDir(t, "concurrent-multi")
	store := storage.NewWALStorage(baseDir, 10*time.Millisecond, 10*time.Second)
	defer store.Close()

	b := broker.NewBrokerWithStorage(store, 4)
	topic := "multi-queue-test"
	group := "test-group"

	// Produce 20 messages (will be distributed across 4 queues via round-robin)
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		msg := b.Enqueue(topic, fmt.Sprintf("message-%d", i), "tagA")
		if msg.ID == "" {
			t.Fatalf("failed to enqueue message %d", i)
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Consume from all 4 queues concurrently
	var wg sync.WaitGroup
	consumedMessages := make(map[string]bool)
	var mu sync.Mutex
	errors := []error{}

	// Start 2 consumers per queue (8 consumers total)
	for queueID := 0; queueID < 4; queueID++ {
		for c := 0; c < 2; c++ {
			wg.Add(1)
			qid := queueID
			cid := c
			go func() {
				defer wg.Done()
				for {
					msgs, offset, next, err := b.ConsumeWithLock(group, topic, qid, "tagA", 1)
					if err != nil {
						mu.Lock()
						errors = append(errors, fmt.Errorf("queue %d consumer %d: %v", qid, cid, err))
						mu.Unlock()
						return
					}

					if len(msgs) == 0 {
						return
					}

					msg := msgs[0]
					t.Logf("Queue %d Consumer %d: %s (offset %d->%d)", qid, cid, msg.Body, offset, next)

					// Begin processing
					b.BeginProcessing(group, topic, qid, offset, next, queue.Message{
						ID:        msg.ID,
						Body:      msg.Body,
						Tag:       msg.Tag,
						Retry:     msg.Retry,
						Timestamp: msg.Timestamp,
					})

					mu.Lock()
					if consumedMessages[msg.ID] {
						errors = append(errors, fmt.Errorf("DUPLICATE in queue %d: %s", qid, msg.ID))
						mu.Unlock()
						return
					}
					consumedMessages[msg.ID] = true
					mu.Unlock()

					time.Sleep(5 * time.Millisecond)
					b.CompleteProcessing(msg.ID)
				}
			}()
		}
	}

	wg.Wait()

	if len(errors) > 0 {
		for _, err := range errors {
			t.Error(err)
		}
		t.FailNow()
	}

	if len(consumedMessages) != messageCount {
		t.Errorf("Expected %d messages, got %d", messageCount, len(consumedMessages))
	}

	t.Logf("✅ Successfully consumed %d messages from 4 queues concurrently", len(consumedMessages))
}
