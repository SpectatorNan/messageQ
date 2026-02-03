package broker

import (
	"container/heap"
	"encoding/binary"
	"fmt"
	"messageQ/mq/storage"
	"sync"
	"time"

	"github.com/google/uuid"
)

// System topic for storing delayed messages (like RocketMQ's SCHEDULE_TOPIC_XXXX)
const SystemDelayTopic = "__DELAY_TOPIC__"

// BinaryDelayScheduler uses CommitLog binary storage instead of JSON
type BinaryDelayScheduler struct {
	store        storage.Storage
	delayQueue   DelayQueue
	mu           sync.Mutex
	stopCh       chan struct{}
	ticker       *time.Ticker
	topicManager *TopicManager
	initialized  bool
}

// NewBinaryDelayScheduler creates a scheduler using binary CommitLog storage
func NewBinaryDelayScheduler(store storage.Storage, tm *TopicManager) (*BinaryDelayScheduler, error) {
	ds := &BinaryDelayScheduler{
		store:        store,
		delayQueue:   make(DelayQueue, 0),
		stopCh:       make(chan struct{}),
		ticker:       time.NewTicker(100 * time.Millisecond),
		topicManager: tm,
	}
	heap.Init(&ds.delayQueue)

	// Load delayed messages from system topic
	if err := ds.loadFromCommitLog(); err != nil {
		fmt.Printf("Warning: failed to load delay messages from commitlog: %v\n", err)
	}

	ds.initialized = true
	go ds.run()
	return ds, nil
}

// run processes delayed messages
func (ds *BinaryDelayScheduler) run() {
	for {
		select {
		case <-ds.stopCh:
			return
		case <-ds.ticker.C:
			ds.processDelayedMessages()
		}
	}
}

func (ds *BinaryDelayScheduler) processDelayedMessages() {
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

		// Write to target topic
		targetTopic := dm.Topic
		if ds.topicManager != nil && ds.topicManager.IsDelayTopic(dm.Topic) {
			targetTopic = dm.Topic
		}

		err := ds.store.Append(targetTopic, dm.QueueID, dm.Message)
		if err != nil {
			fmt.Printf("Failed to append delayed message to %s: %v\n", targetTopic, err)
		}
		processed = true
	}

	// Persist updated state back to CommitLog
	if processed {
		if err := ds.persistToCommitLog(); err != nil {
			fmt.Printf("Failed to persist delay queue: %v\n", err)
		}
	}
}

// Schedule adds a message to delay queue
func (ds *BinaryDelayScheduler) Schedule(dm *DelayedMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	heap.Push(&ds.delayQueue, dm)

	if ds.initialized {
		// Persist immediately to CommitLog
		if err := ds.persistToCommitLog(); err != nil {
			fmt.Printf("Failed to persist delay queue: %v\n", err)
		}
	}
}

// ScheduleWithDelay schedules a message with relative delay from now
func (ds *BinaryDelayScheduler) ScheduleWithDelay(topic string, queueID int, msg storage.Message, delay time.Duration) {
	dm := &DelayedMessage{
		Message:   msg,
		Topic:     topic,
		QueueID:   queueID,
		ExecuteAt: time.Now().Add(delay),
	}
	ds.Schedule(dm)
}

// Stop gracefully shuts down the scheduler
func (ds *BinaryDelayScheduler) Stop() {
	close(ds.stopCh)
	ds.ticker.Stop()

	// Final persist before shutdown
	ds.mu.Lock()
	defer ds.mu.Unlock()
	if err := ds.persistToCommitLog(); err != nil {
		fmt.Printf("Failed to persist delay queue on shutdown: %v\n", err)
	}
}

// Stats returns current scheduler statistics
func (ds *BinaryDelayScheduler) Stats() map[string]interface{} {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stats := map[string]interface{}{
		"pending_messages": ds.delayQueue.Len(),
		"storage_type":     "binary_commitlog",
		"system_topic":     SystemDelayTopic,
	}

	if ds.delayQueue.Len() > 0 {
		stats["next_execution"] = ds.delayQueue[0].ExecuteAt
	}

	return stats
}

// persistToCommitLog saves all pending delayed messages to system topic
// Format: [count:4][entry1][entry2]...
// Entry: [executeAt:8][topic_len:2][topic][queueID:4][msg_binary]
func (ds *BinaryDelayScheduler) persistToCommitLog() error {
	// Build binary representation
	var buf []byte
	
	// Write count
	count := uint32(ds.delayQueue.Len())
	countBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(countBytes, count)
	buf = append(buf, countBytes...)

	// Write each delayed message
	for _, dm := range ds.delayQueue {
		// ExecuteAt (8 bytes)
		executeAtBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(executeAtBytes, uint64(dm.ExecuteAt.Unix()))
		buf = append(buf, executeAtBytes...)

		// Topic length and topic (2 + N bytes)
		topicBytes := []byte(dm.Topic)
		topicLenBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(topicLenBytes, uint16(len(topicBytes)))
		buf = append(buf, topicLenBytes...)
		buf = append(buf, topicBytes...)

		// QueueID (4 bytes)
		queueIDBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(queueIDBytes, uint32(dm.QueueID))
		buf = append(buf, queueIDBytes...)

		// Message binary (use existing message encoding)
		msgBinary := encodeDelayMessage(dm.Message)
		buf = append(buf, msgBinary...)
	}

	// Write to system topic (queue 0)
	// Use special message with body containing the binary data
	// Generate a proper UUID for the message ID
	metaMsg := storage.Message{
		ID:        uuid.New().String(),
		Body:      string(buf), // Store binary as string
		Tag:       "__DELAY_META__",
		Retry:     0,
		Timestamp: time.Now(),
	}

	// Always append to queue 0 of system topic, overwrite by reading latest
	return ds.store.Append(SystemDelayTopic, 0, metaMsg)
}

// loadFromCommitLog restores delayed messages from system topic
func (ds *BinaryDelayScheduler) loadFromCommitLog() error {
	// Try to load from system topic
	messages, err := ds.store.Load(SystemDelayTopic, 0)
	if err != nil || len(messages) == 0 {
		// No saved state, that's ok
		return nil
	}

	// Get the last message (latest state)
	lastMsg := messages[len(messages)-1]
	if lastMsg.Tag != "__DELAY_META__" {
		return fmt.Errorf("invalid delay meta message")
	}

	buf := []byte(lastMsg.Body)
	if len(buf) < 4 {
		return nil // Empty state
	}

	// Read count
	count := binary.BigEndian.Uint32(buf[0:4])
	offset := 4

	// Read each entry
	for i := uint32(0); i < count; i++ {
		if offset+8 > len(buf) {
			break
		}

		// ExecuteAt
		executeAt := time.Unix(int64(binary.BigEndian.Uint64(buf[offset:offset+8])), 0)
		offset += 8

		// Topic
		if offset+2 > len(buf) {
			break
		}
		topicLen := binary.BigEndian.Uint16(buf[offset : offset+2])
		offset += 2
		if offset+int(topicLen) > len(buf) {
			break
		}
		topic := string(buf[offset : offset+int(topicLen)])
		offset += int(topicLen)

		// QueueID
		if offset+4 > len(buf) {
			break
		}
		queueID := int(binary.BigEndian.Uint32(buf[offset : offset+4]))
		offset += 4

		// Message
		msg, n := decodeDelayMessage(buf[offset:])
		if n <= 0 {
			break
		}
		offset += n

		// Add to delay queue
		dm := &DelayedMessage{
			Message:   msg,
			Topic:     topic,
			QueueID:   queueID,
			ExecuteAt: executeAt,
		}
		heap.Push(&ds.delayQueue, dm)
	}

	fmt.Printf("Loaded %d delayed messages from CommitLog (binary)\n", ds.delayQueue.Len())
	return nil
}

// encodeDelayMessage encodes a message to binary format
// Format: [id_len:2][id][retry:2][ts:8][tag_len:2][tag][body_len:4][body]
func encodeDelayMessage(msg storage.Message) []byte {
	var buf []byte

	// ID
	idBytes := []byte(msg.ID)
	idLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(idLenBytes, uint16(len(idBytes)))
	buf = append(buf, idLenBytes...)
	buf = append(buf, idBytes...)

	// Retry
	retryBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(retryBytes, uint16(msg.Retry))
	buf = append(buf, retryBytes...)

	// Timestamp
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(msg.Timestamp.Unix()))
	buf = append(buf, tsBytes...)

	// Tag
	tagBytes := []byte(msg.Tag)
	tagLenBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(tagLenBytes, uint16(len(tagBytes)))
	buf = append(buf, tagLenBytes...)
	buf = append(buf, tagBytes...)

	// Body
	bodyBytes := []byte(msg.Body)
	bodyLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bodyLenBytes, uint32(len(bodyBytes)))
	buf = append(buf, bodyLenBytes...)
	buf = append(buf, bodyBytes...)

	return buf
}

// decodeDelayMessage decodes a message from binary format
// Returns (message, bytes_consumed)
func decodeDelayMessage(buf []byte) (storage.Message, int) {
	var msg storage.Message
	offset := 0

	// ID
	if offset+2 > len(buf) {
		return msg, 0
	}
	idLen := binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	if offset+int(idLen) > len(buf) {
		return msg, 0
	}
	msg.ID = string(buf[offset : offset+int(idLen)])
	offset += int(idLen)

	// Retry
	if offset+2 > len(buf) {
		return msg, 0
	}
	msg.Retry = int(binary.BigEndian.Uint16(buf[offset : offset+2]))
	offset += 2

	// Timestamp
	if offset+8 > len(buf) {
		return msg, 0
	}
	msg.Timestamp = time.Unix(int64(binary.BigEndian.Uint64(buf[offset:offset+8])), 0)
	offset += 8

	// Tag
	if offset+2 > len(buf) {
		return msg, 0
	}
	tagLen := binary.BigEndian.Uint16(buf[offset : offset+2])
	offset += 2
	if offset+int(tagLen) > len(buf) {
		return msg, 0
	}
	msg.Tag = string(buf[offset : offset+int(tagLen)])
	offset += int(tagLen)

	// Body
	if offset+4 > len(buf) {
		return msg, 0
	}
	bodyLen := binary.BigEndian.Uint32(buf[offset : offset+4])
	offset += 4
	if offset+int(bodyLen) > len(buf) {
		return msg, 0
	}
	msg.Body = string(buf[offset : offset+int(bodyLen)])
	offset += int(bodyLen)

	return msg, offset
}
