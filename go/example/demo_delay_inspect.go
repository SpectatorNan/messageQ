package main

import (
	"fmt"
	"messageQ/mq/broker"
	"messageQ/mq/storage"
	"time"
)

func main() {
	dataDir := "./example/testdata/demo_delay"
	
	// Create broker
	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()
	
	b := broker.NewBrokerWithStorage(store, 4)
	defer b.Close()

	fmt.Println("📝 Creating delayed messages...")
	
	// Schedule messages with different delays and topics
	msg1 := b.EnqueueWithDelay("orders", "Order #1001: iPhone 15 Pro - $999", "electronics", 5*time.Second)
	fmt.Printf("✅ Scheduled: %s (5 seconds)\n", msg1.ID)
	
	msg2 := b.EnqueueWithDelay("orders", "Order #1002: MacBook Pro M3 - $2499", "electronics", 15*time.Second)
	fmt.Printf("✅ Scheduled: %s (15 seconds)\n", msg2.ID)
	
	msg3 := b.EnqueueWithDelay("notifications", "Welcome email for user@example.com", "email", 30*time.Second)
	fmt.Printf("✅ Scheduled: %s (30 seconds)\n", msg3.ID)
	
	msg4 := b.EnqueueWithDelay("orders", "Order #1003: AirPods Pro - $249", "electronics", 1*time.Minute)
	fmt.Printf("✅ Scheduled: %s (1 minute)\n", msg4.ID)
	
	msg5 := b.EnqueueWithDelay("notifications", "Password reset link expires", "security", 5*time.Minute)
	fmt.Printf("✅ Scheduled: %s (5 minutes)\n", msg5.ID)
	
	msg6 := b.EnqueueWithDelay("orders", "Order #1004: iPad Air - $599", "electronics", 10*time.Minute)
	fmt.Printf("✅ Scheduled: %s (10 minutes)\n", msg6.ID)

	// Wait for persistence
	time.Sleep(500 * time.Millisecond)
	
	fmt.Println("\n🎯 Test data created successfully!")
	fmt.Printf("📂 Data directory: %s\n", dataDir)
	fmt.Printf("🔍 To inspect: go run ./cmd/mq-inspect -dir %s -delay\n", dataDir)
}
