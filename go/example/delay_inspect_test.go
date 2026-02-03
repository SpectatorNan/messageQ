package example

import (
	"messageQ/mq/broker"
	"messageQ/mq/storage"
	"os"
	"testing"
	"time"
)

func TestDelayInspect(t *testing.T) {
	dataDir := "testdata/delay_inspect"
	defer os.RemoveAll(dataDir)

	// Create broker and schedule some delay messages
	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()
	
	b := broker.NewBrokerWithStorage(store, 4)
	defer b.Close()

	// Schedule messages with different delays
	msg1 := b.EnqueueWithDelay("orders", "Order #1001: iPhone 15 Pro", "electronics", 5*time.Second)
	msg2 := b.EnqueueWithDelay("orders", "Order #1002: MacBook Pro", "electronics", 10*time.Second)
	msg3 := b.EnqueueWithDelay("orders", "Order #1003: AirPods Pro", "electronics", 30*time.Second)
	msg4 := b.EnqueueWithDelay("orders", "Order #1004: iPad Air", "electronics", 1*time.Minute)
	msg5 := b.EnqueueWithDelay("orders", "Order #1005: Apple Watch", "electronics", 2*time.Minute)

	t.Logf("Scheduled messages: %s, %s, %s, %s, %s", msg1.ID, msg2.ID, msg3.ID, msg4.ID, msg5.ID)

	// Wait a bit to ensure persistence (scheduler ticks and persists)
	time.Sleep(1 * time.Second)
	
	// Close broker to trigger final persistence
	b.Close()
	store.Close()

	t.Logf("✅ Test data created. Run: go run ./cmd/mq-inspect -dir=%s -delay", dataDir)
}
