package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

const defaultQueueCount = 4
const defaultMaxRetry = 3

type processingState string

const (
	stateProcessing processingState = "processing"
	stateCompleted  processingState = "completed"
	stateRetry      processingState = "retry"
)

type processingEntry struct {
	Group      string
	Topic      string
	QueueID    int
	Offset     int64
	NextOffset int64
	MsgID      string
	Body       string
	Tag        string
	Retry      int
	Timestamp  time.Time
	State      processingState
	UpdatedAt  time.Time
}

type Broker struct {
	queues            map[string][]*queue.Queue
	lock              sync.Mutex
	store             storage.Storage
	queueCount        int
	rrEnq             map[string]int
	rrDeq             map[string]int
	inflight          map[string]int
	processing        map[string]processingEntry // msgID -> entry
	stats             map[string]*groupStats     // group -> stats
	retryCounts       map[string]int             // msgID -> retry count (in-memory)
	maxRetry          int
	consumeOffsetLock map[string]*sync.Mutex          // "group:topic:queueID" -> lock for concurrent consume
	delayScheduler    *PersistentDelayScheduler       // persistent delay scheduler
	topicManager      *TopicManager                   // topic metadata manager
	dataDir           string                          // data directory for persistence
}

type groupStats struct {
	Processing int64 `json:"processing"`
	Completed  int64 `json:"completed"`
	Retry      int64 `json:"retry"`
}

func NewBroker() *Broker {
	return NewBrokerWithStorage(nil, defaultQueueCount)
}

func NewBrokerWithStorage(store storage.Storage, queueCount int) *Broker {
	if queueCount <= 0 {
		queueCount = defaultQueueCount
	}
	// Use default data directory
	return NewBrokerWithPersistence(store, queueCount, "data")
}

// NewBrokerWithPersistence creates a broker with persistent delay scheduler
func NewBrokerWithPersistence(store storage.Storage, queueCount int, dataDir string) *Broker {
	if queueCount <= 0 {
		queueCount = defaultQueueCount
	}
	
	// Initialize topic manager
	tm, err := NewTopicManager(dataDir)
	if err != nil {
		fmt.Printf("Warning: failed to load topic manager: %v\n", err)
		tm, _ = NewTopicManager(dataDir) // Create empty one
	}
	
	// Initialize persistent delay scheduler
	scheduler, err := NewPersistentDelayScheduler(store, dataDir, tm)
	if err != nil {
		fmt.Printf("Warning: failed to load delay scheduler: %v\n", err)
		scheduler, _ = NewPersistentDelayScheduler(store, dataDir, tm) // Create empty one
	}
	
	return &Broker{
		queues:            make(map[string][]*queue.Queue),
		store:             store,
		queueCount:        queueCount,
		rrEnq:             make(map[string]int),
		rrDeq:             make(map[string]int),
		inflight:          make(map[string]int),
		processing:        make(map[string]processingEntry),
		stats:             make(map[string]*groupStats),
		retryCounts:       make(map[string]int),
		maxRetry:          defaultMaxRetry,
		consumeOffsetLock: make(map[string]*sync.Mutex),
		delayScheduler:    scheduler,
		topicManager:      tm,
		dataDir:           dataDir,
	}
}

// now returns current time (can be mocked in tests)
var now = time.Now

// getQueues ensures queues exist for a topic.
func (b *Broker) getQueues(topic string) []*queue.Queue {
	if qs, ok := b.queues[topic]; ok {
		return qs
	}
	qs := make([]*queue.Queue, b.queueCount)
	for i := 0; i < b.queueCount; i++ {
		if b.store != nil {
			qs[i] = queue.NewQueueWithStorage(b.store, topic, i)
		} else {
			qs[i] = queue.NewQueue()
		}
	}
	b.queues[topic] = qs
	return qs
}

// Enqueue routes to a queue using round-robin and returns the message.
func (b *Broker) Enqueue(topic string, body string, tag string) queue.Message {
	if tag == "" {
		return queue.Message{}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	qs := b.getQueues(topic)
	idx := b.rrEnq[topic] % len(qs)
	b.rrEnq[topic] = (idx + 1) % len(qs)
	return qs[idx].Enqueue(body, tag)
}

// EnqueueBody keeps backward compatibility for callers without tags.
func (b *Broker) EnqueueBody(topic string, body string) queue.Message {
	return b.Enqueue(topic, body, "")
}

// EnqueueWithDelay schedules a message for delayed delivery
func (b *Broker) EnqueueWithDelay(topic string, body string, tag string, delay time.Duration) queue.Message {
	if tag == "" {
		return queue.Message{}
	}
	
	b.lock.Lock()
	qs := b.getQueues(topic)
	idx := b.rrEnq[topic] % len(qs)
	b.rrEnq[topic] = (idx + 1) % len(qs)
	b.lock.Unlock()
	
	// Generate message ID and metadata
	uid, _ := uuid.NewV7()
	msg := queue.Message{
		ID:        uid.String(),
		Body:      body,
		Tag:       tag,
		Timestamp: time.Now(),
	}
	
	// Schedule for delayed delivery
	b.delayScheduler.ScheduleWithDelay(topic, idx, storage.Message{
		ID:        msg.ID,
		Body:      msg.Body,
		Tag:       msg.Tag,
		Retry:     0,
		Timestamp: msg.Timestamp,
	}, delay)
	
	return msg
}

// Dequeue attempts a non-blocking scan across queues; if empty, blocks on a queue in round-robin.
func (b *Broker) Dequeue(topic string) queue.Message {
	return b.DequeueTag(topic, "")
}

// DequeueTag attempts a non-blocking scan across queues for tag; if none, blocks on a queue in round-robin.
func (b *Broker) DequeueTag(topic string, tag string) queue.Message {
	b.lock.Lock()
	qs := b.getQueues(topic)
	start := b.rrDeq[topic] % len(qs)
	b.lock.Unlock()

	// try each queue once without blocking
	for i := 0; i < len(qs); i++ {
		idx := (start + i) % len(qs)
		if msg, ok := qs[idx].TryDequeueTag(tag); ok {
			b.lock.Lock()
			b.rrDeq[topic] = (idx + 1) % len(qs)
			b.inflight[msg.ID] = idx
			b.lock.Unlock()
			return msg
		}
	}

	// if none available, block on the round-robin queue
	idx := start
	msg := qs[idx].DequeueTag(tag)
	b.lock.Lock()
	b.rrDeq[topic] = (idx + 1) % len(qs)
	b.inflight[msg.ID] = idx
	b.lock.Unlock()
	return msg
}

// Ack routes to the inflight queue based on msgID.
func (b *Broker) Ack(topic string, id string) bool {
	b.lock.Lock()
	idx, ok := b.inflight[id]
	qs := b.getQueues(topic)
	if ok {
		delete(b.inflight, id)
	}
	b.lock.Unlock()
	if !ok || idx < 0 || idx >= len(qs) {
		return false
	}
	return qs[idx].Ack(id)
}

// Nack routes to the inflight queue based on msgID.
func (b *Broker) Nack(topic string, id string) bool {
	b.lock.Lock()
	idx, ok := b.inflight[id]
	qs := b.getQueues(topic)
	if ok {
		delete(b.inflight, id)
	}
	b.lock.Unlock()
	if !ok || idx < 0 || idx >= len(qs) {
		return false
	}
	return qs[idx].Nack(id)
}

// GetQueue returns the first queue for backward compatibility.
func (b *Broker) GetQueue(topic string) *queue.Queue {
	b.lock.Lock()
	defer b.lock.Unlock()
	qs := b.getQueues(topic)
	return qs[0]
}

// OffsetStore provides consumer group offset persistence.
type OffsetStore interface {
	CommitOffset(group, topic string, queueID int, offset int64) error
	GetOffset(group, topic string, queueID int) (int64, bool, error)
}

var ErrOffsetUnsupported = errors.New("offset store not supported")

// CommitOffset persists a consumer group offset if supported by storage.
func (b *Broker) CommitOffset(group, topic string, queueID int, offset int64) error {
	if os, ok := b.store.(OffsetStore); ok {
		return os.CommitOffset(group, topic, queueID, offset)
	}
	return ErrOffsetUnsupported
}

// GetOffset loads a consumer group offset if supported by storage.
func (b *Broker) GetOffset(group, topic string, queueID int) (int64, bool, error) {
	if os, ok := b.store.(OffsetStore); ok {
		return os.GetOffset(group, topic, queueID)
	}
	return 0, false, ErrOffsetUnsupported
}

// ConsumeQueueReader provides access to consumequeue index reads.
type ConsumeQueueReader interface {
	ReadFromConsumeQueue(topic string, queueID int, offset int64, max int, tag string) ([]storage.Message, int64, error)
}

// ReadFromConsumeQueue proxies to storage consumequeue reader when available.
func (b *Broker) ReadFromConsumeQueue(topic string, queueID int, offset int64, max int, tag string) ([]storage.Message, int64, error) {
	if r, ok := b.store.(ConsumeQueueReader); ok {
		return r.ReadFromConsumeQueue(topic, queueID, offset, max, tag)
	}
	return nil, offset, ErrOffsetUnsupported
}

// getConsumeLock returns the lock for a specific group/topic/queue combination.
func (b *Broker) getConsumeLock(group, topic string, queueID int) *sync.Mutex {
	key := fmt.Sprintf("%s:%s:%d", group, topic, queueID)
	b.lock.Lock()
	defer b.lock.Unlock()
	if _, ok := b.consumeOffsetLock[key]; !ok {
		b.consumeOffsetLock[key] = &sync.Mutex{}
	}
	return b.consumeOffsetLock[key]
}

// ConsumeWithLock 提供线程安全的消费操作，防止多consumer重复消费
func (b *Broker) ConsumeWithLock(group, topic string, queueID int, tag string, maxMessages int) ([]storage.Message, int64, int64, error) {
	if maxMessages <= 0 {
		maxMessages = 1
	}
	
	// 获取该group/topic/queue的专属锁
	consumeLock := b.getConsumeLock(group, topic, queueID)
	consumeLock.Lock()
	defer consumeLock.Unlock()
	
	// 获取当前offset
	offset, ok, err := b.GetOffset(group, topic, queueID)
	if err != nil {
		return nil, 0, 0, err
	}
	if !ok {
		offset = 0
	}
	
	// 从consumequeue读取消息
	msgs, nextOffset, err := b.ReadFromConsumeQueue(topic, queueID, offset, maxMessages, tag)
	if err != nil {
		return nil, offset, offset, err
	}
	
	if len(msgs) == 0 {
		return nil, offset, offset, nil
	}
	
	// 立即提交nextOffset，防止其他consumer读取相同消息
	// 如果消费失败，通过重试机制处理，不回退offset
	if err := b.CommitOffset(group, topic, queueID, nextOffset); err != nil {
		return nil, offset, offset, err
	}
	
	return msgs, offset, nextOffset, nil
}

// BeginProcessing records a message as processing for a group/queue/offset.
func (b *Broker) BeginProcessing(group, topic string, queueID int, offset, nextOffset int64, msg queue.Message) {
	b.lock.Lock()
	defer b.lock.Unlock()
	// apply retry count if tracked
	if rc := b.retryCounts[msg.ID]; rc > msg.Retry {
		msg.Retry = rc
	}
	entry := processingEntry{
		Group:      group,
		Topic:      topic,
		QueueID:    queueID,
		Offset:     offset,
		NextOffset: nextOffset,
		MsgID:      msg.ID,
		Body:       msg.Body,
		Tag:        msg.Tag,
		Retry:      msg.Retry,
		Timestamp:  msg.Timestamp,
		State:      stateProcessing,
		UpdatedAt:  time.Now(),
	}
	b.processing[msg.ID] = entry
	// fmt.Printf("[DEBUG] BeginProcessing: added %s to processing map (total: %d)\n", msg.ID, len(b.processing))
	gs := b.stats[group]
	if gs == nil {
		gs = &groupStats{}
		b.stats[group] = gs
	}
	gs.Processing++
}

// RetryProcessing marks a message retry and reappends to queue end or DLQ.
func (b *Broker) RetryProcessing(msgID string) bool {
	b.lock.Lock()
	entry, ok := b.processing[msgID]
	if !ok || entry.State != stateProcessing {
		b.lock.Unlock()
		return false
	}
	retryCount := b.retryCounts[msgID] + 1
	b.retryCounts[msgID] = retryCount
	entry.State = stateRetry
	entry.UpdatedAt = time.Now()
	entry.Retry = retryCount
	b.processing[msgID] = entry
	gs := b.stats[entry.Group]
	if gs != nil {
		gs.Processing--
		gs.Retry++
	}
	delete(b.processing, msgID)
	// exceed max retry -> send to DLQ
	if retryCount > b.maxRetry {
		b.clearRetryCount(msgID)
		b.lock.Unlock()
		dlqTopic := entry.Topic + ".dlq"
		_ = b.store.Append(dlqTopic, 0, storage.Message{
			ID:        entry.MsgID,
			Body:      entry.Body,
			Tag:       entry.Tag,
			Retry:     retryCount,
			Timestamp: time.Now(),
		})
		return true
	}
	b.lock.Unlock()
	// Schedule retry with exponential backoff
	delay := CalculateRetryBackoff(retryCount)
	b.delayScheduler.ScheduleWithDelay(entry.Topic, entry.QueueID, storage.Message{
		ID:        entry.MsgID,
		Body:      entry.Body,
		Tag:       entry.Tag,
		Retry:     retryCount,
		Timestamp: time.Now(),
	}, delay)
	return true
}

// CompleteProcessing marks a message completed (offset already committed in ConsumeWithLock).
func (b *Broker) CompleteProcessing(msgID string) bool {
	b.lock.Lock()
	entry, ok := b.processing[msgID]
	if !ok {
		// fmt.Printf("[DEBUG] CompleteProcessing: msgID %s not found in processing map (total: %d)\n", msgID, len(b.processing))
		b.lock.Unlock()
		return false
	}
	if entry.State != stateProcessing {
		// fmt.Printf("[DEBUG] CompleteProcessing: msgID %s has wrong state: %s\n", msgID, entry.State)
		b.lock.Unlock()
		return false
	}
	entry.State = stateCompleted
	entry.UpdatedAt = time.Now()
	b.processing[msgID] = entry
	gs := b.stats[entry.Group]
	if gs != nil {
		gs.Processing--
		gs.Completed++
	}
	delete(b.processing, msgID)
	b.clearRetryCount(msgID)
	b.lock.Unlock()
	return true
}

// Stats returns current processing/completed/retry counts per group.
func (b *Broker) Stats() map[string]groupStats {
	b.lock.Lock()
	defer b.lock.Unlock()
	out := make(map[string]groupStats, len(b.stats))
	for g, s := range b.stats {
		out[g] = *s
	}
	return out
}

// Topic Management Methods

// CreateTopic creates a new topic with specified type
func (b *Broker) CreateTopic(name string, topicType TopicType, queueCount int) error {
	return b.topicManager.CreateTopic(name, topicType, queueCount)
}

// GetTopicConfig returns the configuration of a topic
func (b *Broker) GetTopicConfig(name string) (*TopicConfig, error) {
	return b.topicManager.GetTopicConfig(name)
}

// ListTopics returns all topics
func (b *Broker) ListTopics() []*TopicConfig {
	return b.topicManager.ListTopics()
}

// DeleteTopic deletes a topic
func (b *Broker) DeleteTopic(name string) error {
	return b.topicManager.DeleteTopic(name)
}

// IsDelayTopic checks if a topic is configured as a delay topic
func (b *Broker) IsDelayTopic(name string) bool {
	return b.topicManager.IsDelayTopic(name)
}

// GetRetryCount returns current retry count for a message id.
func (b *Broker) GetRetryCount(msgID string) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.retryCounts[msgID]
}

// clearRetryCount removes retry count tracking for a message id.
func (b *Broker) clearRetryCount(msgID string) {
	delete(b.retryCounts, msgID)
}

// Close gracefully stops the broker and its components
func (b *Broker) Close() error {
	if b.delayScheduler != nil {
		b.delayScheduler.Stop()
	}
	return nil
}

// GetDelayScheduler returns the delay scheduler for direct access
func (b *Broker) GetDelayScheduler() *PersistentDelayScheduler {
	return b.delayScheduler
}
