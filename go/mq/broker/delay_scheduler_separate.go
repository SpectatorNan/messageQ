package broker

import (
	"container/heap"
	"fmt"
	"messageQ/mq/storage"
	"sync"
	"time"
)

// SeparateDelayScheduler schedules delayed messages to dedicated delay topics
// Example: messages for "orders" → "orders.delay"
type SeparateDelayScheduler struct {
	store      storage.Storage
	delayQueue DelayQueue
	mu         sync.Mutex
	stopCh     chan struct{}
	ticker     *time.Ticker
}

// NewSeparateDelayScheduler creates scheduler that uses separate delay topics
func NewSeparateDelayScheduler(store storage.Storage) *SeparateDelayScheduler {
	ds := &SeparateDelayScheduler{
		store:      store,
		delayQueue: make(DelayQueue, 0),
		stopCh:     make(chan struct{}),
		ticker:     time.NewTicker(100 * time.Millisecond),
	}
	heap.Init(&ds.delayQueue)
	go ds.run()
	return ds
}

// GetDelayTopicName returns the delay topic name for a given topic
// Example: "orders" → "orders.delay"
func GetDelayTopicName(topic string) string {
	return topic + ".delay"
}

// run processes delayed messages and writes to delay topics
func (ds *SeparateDelayScheduler) run() {
	for {
		select {
		case <-ds.stopCh:
			return
		case <-ds.ticker.C:
			ds.processDelayedMessages()
		}
	}
}

func (ds *SeparateDelayScheduler) processDelayedMessages() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	now := time.Now()
	for ds.delayQueue.Len() > 0 {
		next := ds.delayQueue[0]
		if next.ExecuteAt.After(now) {
			break
		}

		dm := heap.Pop(&ds.delayQueue).(*DelayedMessage)
		
		// Write to delay topic: orders → orders.delay
		delayTopic := GetDelayTopicName(dm.Topic)
		err := ds.store.Append(delayTopic, dm.QueueID, dm.Message)
		if err != nil {
			fmt.Printf("Failed to append delayed message to %s: %v\n", delayTopic, err)
		}
	}
}

// Schedule adds a message to delay queue
func (ds *SeparateDelayScheduler) Schedule(dm *DelayedMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	heap.Push(&ds.delayQueue, dm)
}

// ScheduleWithDelay schedules a message with relative delay from now
func (ds *SeparateDelayScheduler) ScheduleWithDelay(topic string, queueID int, msg storage.Message, delay time.Duration) {
	dm := &DelayedMessage{
		Message:   msg,
		Topic:     topic,
		QueueID:   queueID,
		ExecuteAt: time.Now().Add(delay),
	}
	ds.Schedule(dm)
}

// Stop gracefully shuts down the scheduler
func (ds *SeparateDelayScheduler) Stop() {
	close(ds.stopCh)
	ds.ticker.Stop()
}

// Stats returns current scheduler statistics
func (ds *SeparateDelayScheduler) Stats() map[string]interface{} {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stats := map[string]interface{}{
		"pending_messages": ds.delayQueue.Len(),
	}

	if ds.delayQueue.Len() > 0 {
		stats["next_execution"] = ds.delayQueue[0].ExecuteAt
	}

	return stats
}
