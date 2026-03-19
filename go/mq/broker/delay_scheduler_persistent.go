package broker

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"github.com/SpectatorNan/messageQ/go/mq/storage"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// PersistentDelayScheduler manages delayed messages with disk persistence
type PersistentDelayScheduler struct {
	store        storage.Storage
	delayQueue   DelayQueue
	mu           sync.Mutex
	stopCh       chan struct{}
	ticker       *time.Ticker
	persistPath  string
	topicManager *TopicManager
}

// NewPersistentDelayScheduler creates a scheduler with persistence support
func NewPersistentDelayScheduler(store storage.Storage, dataDir string, tm *TopicManager) (*PersistentDelayScheduler, error) {
	ds := &PersistentDelayScheduler{
		store:        store,
		delayQueue:   make(DelayQueue, 0),
		stopCh:       make(chan struct{}),
		ticker:       time.NewTicker(100 * time.Millisecond),
		persistPath:  filepath.Join(dataDir, "delay_queue.json"),
		topicManager: tm,
	}
	heap.Init(&ds.delayQueue)

	// Load persisted delay messages from disk
	if err := ds.load(); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to load delay queue: %w", err)
		}
	}

	go ds.run()
	return ds, nil
}

// run processes delayed messages and persists state
func (ds *PersistentDelayScheduler) run() {
	for {
		select {
		case <-ds.stopCh:
			return
		case <-ds.ticker.C:
			ds.processDelayedMessages()
		}
	}
}

func (ds *PersistentDelayScheduler) processDelayedMessages() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	now := time.Now()
	processed := false

	for ds.delayQueue.Len() > 0 {
		next := ds.delayQueue[0]
		if next.ExecuteAt.After(now) {
			break
		}

		dm := heap.Pop(&ds.delayQueue).(*DelayedMessage)

		// Determine target topic based on topic type
		targetTopic := dm.Topic
		if ds.topicManager != nil && ds.topicManager.IsDelayTopic(dm.Topic) {
			// Delay topic: write to same topic
			targetTopic = dm.Topic
		} else {
			// Normal topic: write to original topic (not .delay)
			targetTopic = dm.Topic
		}

		err := ds.store.Append(targetTopic, dm.QueueID, dm.Message)
		if err != nil {
			fmt.Printf("Failed to append delayed message to %s: %v\n", targetTopic, err)
		}
		processed = true
	}

	// Persist state after processing messages
	if processed {
		if err := ds.persist(); err != nil {
			fmt.Printf("Failed to persist delay queue: %v\n", err)
		}
	}
}

// Schedule adds a message to delay queue
func (ds *PersistentDelayScheduler) Schedule(dm *DelayedMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	heap.Push(&ds.delayQueue, dm)

	// Persist immediately when new message is scheduled
	if err := ds.persist(); err != nil {
		fmt.Printf("Failed to persist delay queue: %v\n", err)
	}
}

// ScheduleWithDelay schedules a message with relative delay from now
func (ds *PersistentDelayScheduler) ScheduleWithDelay(topic string, queueID int, msg storage.Message, delay time.Duration) {
	dm := &DelayedMessage{
		Message:   msg,
		Topic:     topic,
		QueueID:   queueID,
		ExecuteAt: time.Now().Add(delay),
	}
	ds.Schedule(dm)
}

// Stop gracefully shuts down the scheduler
func (ds *PersistentDelayScheduler) Stop() {
	close(ds.stopCh)
	ds.ticker.Stop()

	// Final persist before shutdown
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if err := ds.persist(); err != nil {
		fmt.Printf("Failed to persist delay queue on shutdown: %v\n", err)
	}
}

// Stats returns current scheduler statistics
func (ds *PersistentDelayScheduler) Stats() map[string]interface{} {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stats := map[string]interface{}{
		"pending_messages": ds.delayQueue.Len(),
		"persisted":        true,
	}

	if ds.delayQueue.Len() > 0 {
		stats["next_execution"] = ds.delayQueue[0].ExecuteAt
	}

	return stats
}

// persistedDelayMessage is the JSON-serializable version
type persistedDelayMessage struct {
	MessageID     string `json:"message_id"`
	Body          string `json:"body"`
	Tag           string `json:"tag"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Retry         int    `json:"retry"`
	Timestamp     int64  `json:"timestamp"`
	Topic         string `json:"topic"`
	QueueID       int    `json:"queue_id"`
	ExecuteAt     int64  `json:"execute_at"`
}

// persist saves the delay queue to disk
func (ds *PersistentDelayScheduler) persist() error {
	messages := make([]persistedDelayMessage, 0, ds.delayQueue.Len())

	for _, dm := range ds.delayQueue {
		messages = append(messages, persistedDelayMessage{
			MessageID:     dm.Message.ID,
			Body:          dm.Message.Body,
			Tag:           dm.Message.Tag,
			CorrelationID: dm.Message.CorrelationID,
			Retry:         dm.Message.Retry,
			Timestamp:     dm.Message.Timestamp.Unix(),
			Topic:         dm.Topic,
			QueueID:       dm.QueueID,
			ExecuteAt:     dm.ExecuteAt.Unix(),
		})
	}

	data, err := json.MarshalIndent(messages, "", "  ")
	if err != nil {
		return err
	}

	// Create directory if not exists
	dir := filepath.Dir(ds.persistPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write to temporary file first, then rename (atomic operation)
	tmpPath := ds.persistPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, ds.persistPath)
}

// load restores the delay queue from disk
func (ds *PersistentDelayScheduler) load() error {
	data, err := os.ReadFile(ds.persistPath)
	if err != nil {
		return err
	}

	var messages []persistedDelayMessage
	if err := json.Unmarshal(data, &messages); err != nil {
		return err
	}

	// Restore messages to delay queue
	for _, pm := range messages {
		dm := &DelayedMessage{
			Message: storage.Message{
				ID:            pm.MessageID,
				Body:          pm.Body,
				Tag:           pm.Tag,
				CorrelationID: pm.CorrelationID,
				Retry:         pm.Retry,
				Timestamp:     time.Unix(pm.Timestamp, 0),
			},
			Topic:     pm.Topic,
			QueueID:   pm.QueueID,
			ExecuteAt: time.Unix(pm.ExecuteAt, 0),
		}
		heap.Push(&ds.delayQueue, dm)
	}

	fmt.Printf("Loaded %d delayed messages from disk\n", len(messages))
	return nil
}
