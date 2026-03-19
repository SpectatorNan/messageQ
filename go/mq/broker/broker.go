package broker

import (
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"

	"github.com/SpectatorNan/messageQ/go/mq/logger"
	"github.com/SpectatorNan/messageQ/go/mq/queue"
	"github.com/SpectatorNan/messageQ/go/mq/storage"
)

const defaultQueueCount = 4
const defaultMaxRetry = 3
const defaultProcessingTimeout = 30 * time.Second // 消息处理超时时间
const defaultCompletedHistorySize = 1000
const defaultMessageRetention = 7 * 24 * time.Hour
const defaultMessageExpiryFactor = 2
const defaultCancelledCacheLimit = 10000
const defaultNewGroupStartPosition = "latest"

type processingState string

const (
	stateProcessing processingState = "processing"
	stateCompleted  processingState = "completed"
	stateCancelled  processingState = "cancelled"
	stateExpired    processingState = "expired"
	stateRetry      processingState = "retry"
)

type processingEntry struct {
	Group         string
	Topic         string
	QueueID       int
	Offset        int64
	NextOffset    int64
	MsgID         string
	Body          string
	Tag           string
	CorrelationID string
	Retry         int
	Timestamp     time.Time
	ConsumedAt    time.Time
	State         processingState
	UpdatedAt     time.Time
}

type completedEntry struct {
	Group         string
	Topic         string
	QueueID       int
	Offset        int64
	NextOffset    int64
	MsgID         string
	Body          string
	Tag           string
	CorrelationID string
	Retry         int
	Timestamp     time.Time
	ConsumedAt    time.Time
	AckedAt       time.Time
}

type cancelledEntry struct {
	Group         string
	Topic         string
	State         string
	QueueID       *int
	Offset        *int64
	NextOffset    *int64
	MsgID         string
	Body          string
	Tag           string
	CorrelationID string
	Retry         int
	Timestamp     time.Time
	ScheduledAt   *time.Time
	ConsumedAt    *time.Time
	CancelledAt   time.Time
}

type subscriptionEntry struct {
	Group     string
	Topic     string
	Tag       string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Broker struct {
	queues                 map[string][]*queue.Queue
	lock                   sync.Mutex
	store                  storage.Storage
	queueCount             int
	rrEnq                  map[string]int
	rrDeq                  map[string]int
	inflight               map[string]int
	processing             map[string]processingEntry // group:topic:msgID -> entry
	completed              map[string][]completedEntry
	cancelled              map[string]cancelledEntry
	cancelledOrder         []string
	cancelledCacheLimit    int
	subscriptions          map[string]subscriptionEntry
	stats                  map[string]*groupStats // group -> stats
	retryCounts            map[string]int         // msgID -> retry count (in-memory)
	maxRetry               int
	consumeOffsetLock      map[string]*sync.Mutex // "group:topic:queueID" -> lock for concurrent consume
	delayScheduler         *BinaryDelayScheduler  // binary delay scheduler (CommitLog based)
	topicManager           *TopicManager          // topic metadata manager
	dataDir                string                 // data directory for persistence
	processingTimeout      time.Duration          // message processing timeout
	messageRetention       time.Duration
	messageExpiryFactor    int
	newGroupStartPosition  string
	completedLimit         int
	retryBackoffBase       time.Duration // retry backoff base delay
	retryBackoffMultiplier float64       // retry backoff multiplier
	retryBackoffMax        time.Duration // retry backoff max delay
	stopChan               chan struct{} // channel to stop background goroutines
	akStore                *storage.AccessKeyStore
}

type groupStats struct {
	Processing int64 `json:"processing"`
	Completed  int64 `json:"completed"`
	Retry      int64 `json:"retry"`
}

// DelayEnqueueItem represents a delayed enqueue input.
type DelayEnqueueItem struct {
	Body          string
	Tag           string
	CorrelationID string
	Delay         time.Duration
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
		logger.Warn("Failed to load topic manager, creating new one", zap.Error(err))
		tm, _ = NewTopicManager(dataDir) // Create empty one
	}

	// Initialize binary delay scheduler (CommitLog based)
	scheduler, err := NewBinaryDelayScheduler(store, tm)
	if err != nil {
		logger.Warn("Failed to load delay scheduler, creating new one", zap.Error(err))
		scheduler, _ = NewBinaryDelayScheduler(store, tm) // Create empty one
	}

	logger.Info("Broker with persistence initialized",
		zap.Int("queue_count", queueCount),
		zap.String("data_dir", dataDir))

	b := &Broker{
		queues:                 make(map[string][]*queue.Queue),
		store:                  store,
		queueCount:             queueCount,
		rrEnq:                  make(map[string]int),
		rrDeq:                  make(map[string]int),
		inflight:               make(map[string]int),
		processing:             make(map[string]processingEntry),
		stats:                  make(map[string]*groupStats),
		retryCounts:            make(map[string]int),
		maxRetry:               defaultMaxRetry,
		consumeOffsetLock:      make(map[string]*sync.Mutex),
		delayScheduler:         scheduler,
		topicManager:           tm,
		dataDir:                dataDir,
		processingTimeout:      defaultProcessingTimeout,
		messageRetention:       defaultMessageRetention,
		messageExpiryFactor:    defaultMessageExpiryFactor,
		newGroupStartPosition:  defaultNewGroupStartPosition,
		retryBackoffBase:       1 * time.Second,
		retryBackoffMultiplier: 2.0,
		retryBackoffMax:        60 * time.Second,
		stopChan:               make(chan struct{}),
		completed:              make(map[string][]completedEntry),
		cancelled:              make(map[string]cancelledEntry),
		cancelledCacheLimit:    defaultCancelledCacheLimit,
		subscriptions:          make(map[string]subscriptionEntry),
		completedLimit:         defaultCompletedHistorySize,
	}

	akStore, err := storage.NewAccessKeyStore(filepath.Join(dataDir, "aks.json"))
	if err != nil {
		logger.Warn("Failed to load ak store", zap.Error(err))
	} else {
		b.akStore = akStore
	}
	if rs, ok := b.store.(SegmentRetentionStore); ok {
		rs.SetPruneObserver(b.onPrunedMessage)
	}
	b.applyRetentionSettings(b.messageRetention, b.messageExpiryFactor)

	// 初始化已有 topics 的 queues，确保 reclaimLoop 运行
	b.initializeExistingTopics()
	// 恢复持久化的 processing 记录，避免 crash 丢失
	b.recoverProcessingRecords()
	b.recoverSubscriptions()
	b.recoverCancelledRecords()

	// 启动处理超时检查
	go b.checkProcessingTimeouts()

	return b
}

// initializeExistingTopics 初始化所有已存在的 topics 的 queues
// 这样可以确保 reclaimLoop 在 broker 启动时就开始运行
func (b *Broker) initializeExistingTopics() {
	topics := b.topicManager.ListTopics()
	if len(topics) == 0 {
		return
	}

	logger.Info("Initializing queues for existing topics",
		zap.Int("topic_count", len(topics)))

	b.lock.Lock()
	defer b.lock.Unlock()

	for _, topicConfig := range topics {
		// 根据 topic 配置的 queue_count 创建 queues
		queueCount := topicConfig.QueueCount
		if queueCount <= 0 {
			queueCount = b.queueCount
		}

		qs := make([]*queue.Queue, queueCount)
		for i := 0; i < queueCount; i++ {
			if b.store != nil {
				qs[i] = queue.NewQueueWithStorage(b.store, topicConfig.Name, i)
			} else {
				qs[i] = queue.NewQueue()
			}
		}
		b.queues[topicConfig.Name] = qs

		logger.Debug("Initialized queues for topic",
			zap.String("topic", topicConfig.Name),
			zap.Int("queue_count", queueCount))
	}
}

// recoverProcessingRecords loads persisted processing records and schedules retries.
func (b *Broker) recoverProcessingRecords() {
	ps, ok := b.store.(ProcessingStore)
	if !ok {
		return
	}
	records, err := ps.LoadProcessing()
	if err != nil {
		logger.Warn("Failed to load processing records", zap.Error(err))
		return
	}
	if len(records) == 0 {
		return
	}

	for _, rec := range records {
		// restore retry count baseline
		b.retryCounts[rec.MsgID] = rec.Retry

		retryTopic, err := b.ensureRetryTopic(rec.Group, rec.Topic)
		if err != nil {
			logger.Error("Failed to ensure retry topic on recovery",
				zap.String("group", rec.Group),
				zap.String("topic", rec.Topic),
				zap.Error(err))
			continue
		}

		// schedule retry with remaining timeout window
		deadline := rec.UpdatedAt.Add(b.processingTimeout)
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		msg := storage.Message{
			ID:            rec.MsgID,
			Body:          rec.Body,
			Tag:           rec.Tag,
			CorrelationID: rec.CorrelationID,
			Retry:         rec.Retry + 1,
			Timestamp:     rec.Timestamp,
		}
		b.delayScheduler.ScheduleWithDelay(retryTopic, rec.QueueID, msg, delay)

		// remove persisted processing record after scheduling
		_ = ps.RemoveProcessing(rec.Group, rec.Topic, rec.QueueID, rec.MsgID)
	}
}

func (b *Broker) recoverCancelledRecords() {
	// Tombstones are no longer eagerly loaded into memory on startup.
	// They are resolved from storage on cache miss to avoid linear memory growth.
}

func (b *Broker) recoverSubscriptions() {
	ss, ok := b.store.(SubscriptionStore)
	if !ok {
		return
	}
	records, err := ss.LoadSubscriptions()
	if err != nil {
		logger.Warn("Failed to load subscriptions", zap.Error(err))
		return
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	for _, rec := range records {
		b.subscriptions[groupTopicKey(rec.Group, rec.Topic)] = subscriptionEntry{
			Group:     rec.Group,
			Topic:     rec.Topic,
			Tag:       rec.Tag,
			CreatedAt: rec.CreatedAt,
			UpdatedAt: rec.UpdatedAt,
		}
	}
}

// now returns current time (can be mocked in tests)
var now = time.Now

func processingKey(group, topic, msgID string) string {
	return group + ":" + topic + ":" + msgID
}

func groupTopicKey(group, topic string) string {
	return group + ":" + topic
}

func cancelledKey(topic, msgID string) string {
	return topic + ":" + msgID
}

func normalizeSubscriptionTag(tag string) string {
	normalized := strings.TrimSpace(tag)
	if normalized == "" || normalized == "*" {
		return "*"
	}
	return normalized
}

func cancelledEntryFromRecord(rec storage.CancelledRecord) cancelledEntry {
	state := rec.State
	if state == "" {
		state = string(stateCancelled)
	}
	return cancelledEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		State:         state,
		QueueID:       rec.QueueID,
		Offset:        rec.Offset,
		NextOffset:    rec.NextOffset,
		MsgID:         rec.MsgID,
		Body:          rec.Body,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		ScheduledAt:   rec.ScheduledAt,
		ConsumedAt:    rec.ConsumedAt,
		CancelledAt:   rec.CancelledAt,
	}
}

// getQueues ensures queues exist for a topic.
func (b *Broker) getQueues(topic string) []*queue.Queue {
	if qs, ok := b.queues[topic]; ok {
		return qs
	}

	// Get queue count from topic config, fallback to broker default
	queueCount := b.queueCount
	if topicConfig, err := b.topicManager.GetTopicConfig(topic); err == nil {
		if topicConfig.QueueCount > 0 {
			queueCount = topicConfig.QueueCount
		}
	}

	qs := make([]*queue.Queue, queueCount)
	for i := 0; i < queueCount; i++ {
		if b.store != nil {
			qs[i] = queue.NewQueueWithStorage(b.store, topic, i)
		} else {
			qs[i] = queue.NewQueue()
		}
	}
	b.queues[topic] = qs
	return qs
}

// GetQueueCount returns the number of queues for a topic
func (b *Broker) GetQueueCount(topic string) int {
	b.lock.Lock()
	defer b.lock.Unlock()
	qs := b.getQueues(topic)
	return len(qs)
}

// Enqueue routes to a queue using round-robin and returns the message.
func (b *Broker) Enqueue(topic string, body string, tag string, correlationID string) queue.Message {
	return b.enqueueSingle(topic, body, tag, correlationID)
}

// EnqueueBody keeps backward compatibility for callers without tags.
func (b *Broker) EnqueueBody(topic string, body string) queue.Message {
	return b.Enqueue(topic, body, "", "")
}

// EnqueueWithDelay schedules a message for delayed delivery
func (b *Broker) EnqueueWithDelay(topic string, body string, tag string, correlationID string, delay time.Duration) queue.Message {
	msgs := b.EnqueueWithDelayBatch(topic, []DelayEnqueueItem{{Body: body, Tag: tag, CorrelationID: correlationID, Delay: delay}})
	if len(msgs) == 0 {
		return queue.Message{}
	}
	return msgs[0]
}

// enqueueSingle enqueues a single message without calling batch APIs.
func (b *Broker) enqueueSingle(topic string, body string, tag string, correlationID string) queue.Message {
	if tag == "" {
		return queue.Message{}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	qs := b.getQueues(topic)
	idx := b.rrEnq[topic] % len(qs)
	b.rrEnq[topic] = (idx + 1) % len(qs)
	return qs[idx].Enqueue(body, tag, correlationID)
}

// EnqueueBatch enqueues multiple messages with a single broker lock.
func (b *Broker) EnqueueBatch(topic string, items []queue.Message) []queue.Message {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		msg := b.enqueueSingle(topic, items[0].Body, items[0].Tag, items[0].CorrelationID)
		return []queue.Message{msg}
	}
	b.lock.Lock()
	qs := b.getQueues(topic)
	start := b.rrEnq[topic] % len(qs)
	b.rrEnq[topic] = (start + len(items)) % len(qs)
	b.lock.Unlock()

	msgs := make([]queue.Message, 0, len(items))
	for i, item := range items {
		idx := (start + i) % len(qs)
		msg := qs[idx].Enqueue(item.Body, item.Tag, item.CorrelationID)
		if msg.ID != "" {
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

// EnqueueWithDelayBatch schedules multiple delayed messages with a single broker lock.
func (b *Broker) EnqueueWithDelayBatch(topic string, items []DelayEnqueueItem) []queue.Message {
	if len(items) == 0 {
		return nil
	}
	if len(items) == 1 {
		item := items[0]
		if item.Tag == "" {
			return nil
		}
		uid, err := uuid.NewV7()
		if err != nil {
			uid = uuid.New()
		}
		msg := queue.Message{
			ID:            uid.String(),
			Body:          item.Body,
			Tag:           item.Tag,
			CorrelationID: item.CorrelationID,
			Timestamp:     time.Now(),
		}
		b.lock.Lock()
		qs := b.getQueues(topic)
		idx := b.rrEnq[topic] % len(qs)
		b.rrEnq[topic] = (idx + 1) % len(qs)
		b.lock.Unlock()
		b.delayScheduler.ScheduleWithDelay(topic, idx, storage.Message{
			ID:            msg.ID,
			Body:          msg.Body,
			Tag:           msg.Tag,
			CorrelationID: msg.CorrelationID,
			Retry:         0,
			Timestamp:     msg.Timestamp,
		}, item.Delay)
		return []queue.Message{msg}
	}
	b.lock.Lock()
	qs := b.getQueues(topic)
	start := b.rrEnq[topic] % len(qs)
	b.rrEnq[topic] = (start + len(items)) % len(qs)
	b.lock.Unlock()

	msgs := make([]queue.Message, 0, len(items))
	for i, item := range items {
		if item.Tag == "" {
			continue
		}
		idx := (start + i) % len(qs)
		uid, err := uuid.NewV7()
		if err != nil {
			uid = uuid.New()
		}
		msg := queue.Message{
			ID:            uid.String(),
			Body:          item.Body,
			Tag:           item.Tag,
			CorrelationID: item.CorrelationID,
			Timestamp:     time.Now(),
		}
		b.delayScheduler.ScheduleWithDelay(topic, idx, storage.Message{
			ID:            msg.ID,
			Body:          msg.Body,
			Tag:           msg.Tag,
			CorrelationID: msg.CorrelationID,
			Retry:         0,
			Timestamp:     msg.Timestamp,
		}, item.Delay)
		msgs = append(msgs, msg)
	}
	return msgs
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

// ProcessingStore provides persistence for in-flight processing records.
type ProcessingStore interface {
	SaveProcessing(rec storage.ProcessingRecord) error
	RemoveProcessing(group, topic string, queueID int, msgID string) error
	LoadProcessing() ([]storage.ProcessingRecord, error)
}

// CancelledStore provides persistence for terminated message records.
type CancelledStore interface {
	SaveCancelled(rec storage.CancelledRecord) error
	LoadCancelled() ([]storage.CancelledRecord, error)
}

// CancelledLookupStore provides point/topic lookups for persisted terminal records.
type CancelledLookupStore interface {
	LoadCancelledByID(topic, msgID string) (storage.CancelledRecord, bool, error)
	ListCancelledByTopic(topic, state string, limit int) ([]storage.CancelledRecord, error)
}

// SubscriptionStore provides persistence for normalized group/topic subscriptions.
type SubscriptionStore interface {
	SaveSubscription(rec storage.SubscriptionRecord) error
	LoadSubscriptions() ([]storage.SubscriptionRecord, error)
}

// ConsumeQueueDepthStore reports the current consumequeue depth for a topic/queue.
type ConsumeQueueDepthStore interface {
	ConsumeQueueDepth(topic string, queueID int) int64
}

// ConsumeQueueBaseOffsetStore reports the current logical base offset for a topic/queue.
type ConsumeQueueBaseOffsetStore interface {
	ConsumeQueueBaseOffset(topic string, queueID int) int64
}

// SegmentRetentionStore configures sealed-segment retention pruning in storage.
type SegmentRetentionStore interface {
	SetRetentionWindow(d time.Duration)
	SetPruneObserver(observer func(topic string, queueID int, msg storage.Message) error)
}

var ErrOffsetUnsupported = errors.New("offset store not supported")
var ErrSubscriptionConflict = errors.New("subscription conflict")

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

// ConsumeQueueReaderWithOffset provides consumequeue reads with offsets.
type ConsumeQueueReaderWithOffset interface {
	ReadFromConsumeQueueWithOffsets(topic string, queueID int, offset int64, max int, tag string) ([]storage.MessageWithOffset, int64, error)
}

// ReadFromConsumeQueue proxies to storage consumequeue reader when available.
func (b *Broker) ReadFromConsumeQueue(topic string, queueID int, offset int64, max int, tag string) ([]storage.Message, int64, error) {
	if r, ok := b.store.(ConsumeQueueReader); ok {
		return r.ReadFromConsumeQueue(topic, queueID, offset, max, tag)
	}
	return nil, offset, ErrOffsetUnsupported
}

// ReadFromConsumeQueueWithOffsets proxies to storage consumequeue reader when available.
func (b *Broker) ReadFromConsumeQueueWithOffsets(topic string, queueID int, offset int64, max int, tag string) ([]storage.MessageWithOffset, int64, error) {
	if r, ok := b.store.(ConsumeQueueReaderWithOffset); ok {
		return r.ReadFromConsumeQueueWithOffsets(topic, queueID, offset, max, tag)
	}
	return nil, offset, ErrOffsetUnsupported
}

// ConsumeQueueDepth returns the current consumequeue depth for a topic/queue.
func (b *Broker) ConsumeQueueDepth(topic string, queueID int) int64 {
	if r, ok := b.store.(ConsumeQueueDepthStore); ok {
		return r.ConsumeQueueDepth(topic, queueID)
	}
	return 0
}

// ConsumeQueueBaseOffset returns the current logical base offset for a topic/queue.
func (b *Broker) ConsumeQueueBaseOffset(topic string, queueID int) int64 {
	if r, ok := b.store.(ConsumeQueueBaseOffsetStore); ok {
		return r.ConsumeQueueBaseOffset(topic, queueID)
	}
	return 0
}

// GetRetryTopicName returns the retry topic name for a group and topic.
// Example: "orders" + "g1" -> "orders.retry.g1"
func GetRetryTopicName(group, topic string) string {
	return topic + ".retry." + group
}

func isRetryTopicName(group, topic string) bool {
	return strings.HasSuffix(topic, ".retry."+group)
}

func splitRetryTopicName(topic string) (string, string, bool) {
	const marker = ".retry."
	idx := strings.LastIndex(topic, marker)
	if idx <= 0 || idx+len(marker) >= len(topic) {
		return "", "", false
	}
	return topic[:idx], topic[idx+len(marker):], true
}

func (b *Broker) resolveInitialOffset(group, topic string, queueID int) (int64, error) {
	if strings.EqualFold(b.newGroupStartPosition, "earliest") || isRetryTopicName(group, topic) {
		return b.ConsumeQueueBaseOffset(topic, queueID), nil
	}
	offset := b.ConsumeQueueDepth(topic, queueID)
	if err := b.CommitOffset(group, topic, queueID, offset); err != nil {
		return 0, err
	}
	return offset, nil
}

func (b *Broker) EnsureSubscription(group, topic, tag string) error {
	if group == "" || topic == "" {
		return fmt.Errorf("invalid subscription")
	}
	normalizedTag := normalizeSubscriptionTag(tag)
	key := groupTopicKey(group, topic)
	currentTime := now()

	b.lock.Lock()
	existing, ok := b.subscriptions[key]
	if ok {
		b.lock.Unlock()
		if existing.Tag != normalizedTag {
			return ErrSubscriptionConflict
		}
		return nil
	}
	entry := subscriptionEntry{
		Group:     group,
		Topic:     topic,
		Tag:       normalizedTag,
		CreatedAt: currentTime,
		UpdatedAt: currentTime,
	}
	b.subscriptions[key] = entry
	b.lock.Unlock()

	if ss, ok := b.store.(SubscriptionStore); ok {
		if err := ss.SaveSubscription(storage.SubscriptionRecord{
			Group:     entry.Group,
			Topic:     entry.Topic,
			Tag:       entry.Tag,
			CreatedAt: entry.CreatedAt,
			UpdatedAt: entry.UpdatedAt,
		}); err != nil {
			b.lock.Lock()
			delete(b.subscriptions, key)
			b.lock.Unlock()
			return err
		}
	}
	return nil
}

// getRetryTopicIfExists checks if retry topic exists for group/topic.
func (b *Broker) getRetryTopicIfExists(group, topic string) (string, bool) {
	retryTopic := GetRetryTopicName(group, topic)
	if _, err := b.topicManager.GetTopicConfig(retryTopic); err == nil {
		return retryTopic, true
	}
	return "", false
}

// ensureRetryTopic creates retry topic for group/topic if missing.
func (b *Broker) ensureRetryTopic(group, topic string) (string, error) {
	retryTopic := GetRetryTopicName(group, topic)
	if _, err := b.topicManager.GetTopicConfig(retryTopic); err == nil {
		return retryTopic, nil
	}

	// Use original topic queue count when creating retry topic
	queueCount := b.queueCount
	if cfg, err := b.topicManager.GetTopicConfig(topic); err == nil {
		if cfg.QueueCount > 0 {
			queueCount = cfg.QueueCount
		}
	}

	if err := b.topicManager.CreateInternalTopic(retryTopic, TopicTypeNormal, queueCount); err != nil {
		// If it was created concurrently, treat as success
		if _, getErr := b.topicManager.GetTopicConfig(retryTopic); getErr == nil {
			return retryTopic, nil
		}
		return "", err
	}
	return retryTopic, nil
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
		offset, err = b.resolveInitialOffset(group, topic, queueID)
		if err != nil {
			return nil, 0, 0, err
		}
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

// ConsumeWithRetry first tries the group retry topic, then falls back to original topic.
func (b *Broker) ConsumeWithRetry(group, topic string, queueID int, tag string, maxMessages int) ([]storage.Message, int64, int64, error) {
	if retryTopic, ok := b.getRetryTopicIfExists(group, topic); ok {
		msgs, offset, next, err := b.ConsumeWithLock(group, retryTopic, queueID, tag, maxMessages)
		if err == nil && len(msgs) > 0 {
			return msgs, offset, next, nil
		}
	}
	return b.ConsumeWithLock(group, topic, queueID, tag, maxMessages)
}

func (b *Broker) consumeVisibleFromTopic(group, logicalTopic, storageTopic string, queueID int, tag string, maxMessages int) ([]storage.MessageWithOffset, error) {
	if maxMessages <= 0 {
		maxMessages = 1
	}

	consumeLock := b.getConsumeLock(group, storageTopic, queueID)
	consumeLock.Lock()
	defer consumeLock.Unlock()

	offset, ok, err := b.GetOffset(group, storageTopic, queueID)
	if err != nil {
		return nil, err
	}
	if !ok {
		offset, err = b.resolveInitialOffset(group, storageTopic, queueID)
		if err != nil {
			return nil, err
		}
	}

	cursor := offset
	out := make([]storage.MessageWithOffset, 0, maxMessages)
	for len(out) < maxMessages {
		msgs, nextCursor, err := b.ReadFromConsumeQueueWithOffsets(storageTopic, queueID, cursor, 1, tag)
		if err != nil {
			return nil, err
		}
		if len(msgs) == 0 {
			cursor = nextCursor
			break
		}

		cursor = nextCursor
		msg := msgs[0]
		if b.IsTerminal(logicalTopic, msg.ID) {
			continue
		}
		if b.shouldExpire(msg.Timestamp) {
			qid := queueID
			off := msg.Offset
			next := msg.Offset + 1
			b.recordExpired(cancelledEntry{
				Topic:         logicalTopic,
				QueueID:       &qid,
				Offset:        &off,
				NextOffset:    &next,
				MsgID:         msg.ID,
				Body:          msg.Body,
				Tag:           msg.Tag,
				CorrelationID: msg.CorrelationID,
				Retry:         msg.Retry,
				Timestamp:     msg.Timestamp,
				CancelledAt:   now(),
			})
			continue
		}
		out = append(out, msg)
	}

	if cursor > offset {
		if err := b.CommitOffset(group, storageTopic, queueID, cursor); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (b *Broker) ConsumeVisibleWithRetry(group, topic string, queueID int, tag string, maxMessages int) ([]storage.MessageWithOffset, error) {
	if retryTopic, ok := b.getRetryTopicIfExists(group, topic); ok {
		msgs, err := b.consumeVisibleFromTopic(group, topic, retryTopic, queueID, tag, maxMessages)
		if err == nil && len(msgs) > 0 {
			return msgs, nil
		}
	}
	return b.consumeVisibleFromTopic(group, topic, topic, queueID, tag, maxMessages)
}

// BeginProcessing records a message as processing for a group/queue/offset.
func (b *Broker) BeginProcessing(group, topic string, queueID int, offset, nextOffset int64, msg queue.Message) {
	_ = nextOffset
	b.BeginProcessingBatch(group, topic, queueID, offset, []queue.Message{msg})
}

// BeginProcessingBatch records multiple messages as processing in a single lock.
func (b *Broker) BeginProcessingBatch(group, topic string, queueID int, startOffset int64, msgs []queue.Message) {
	if len(msgs) == 0 {
		return
	}
	consumedAt := now()
	b.lock.Lock()
	gs := b.stats[group]
	if gs == nil {
		gs = &groupStats{}
		b.stats[group] = gs
	}
	for i, msg := range msgs {
		// apply retry count if tracked
		if rc := b.retryCounts[msg.ID]; rc > msg.Retry {
			msg.Retry = rc
		}
		offset := startOffset + int64(i)
		nextOffset := offset + 1
		entry := processingEntry{
			Group:         group,
			Topic:         topic,
			QueueID:       queueID,
			Offset:        offset,
			NextOffset:    nextOffset,
			MsgID:         msg.ID,
			Body:          msg.Body,
			Tag:           msg.Tag,
			CorrelationID: msg.CorrelationID,
			Retry:         msg.Retry,
			Timestamp:     msg.Timestamp,
			ConsumedAt:    consumedAt,
			State:         stateProcessing,
			UpdatedAt:     consumedAt,
		}
		b.processing[processingKey(group, topic, msg.ID)] = entry
	}
	gs.Processing += int64(len(msgs))
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		for _, msg := range msgs {
			_ = ps.SaveProcessing(storage.ProcessingRecord{
				Group:         group,
				Topic:         topic,
				QueueID:       queueID,
				MsgID:         msg.ID,
				Body:          msg.Body,
				Tag:           msg.Tag,
				CorrelationID: msg.CorrelationID,
				Retry:         msg.Retry,
				Timestamp:     msg.Timestamp,
				UpdatedAt:     consumedAt,
			})
		}
	}
}

// RetryProcessing schedules a retry to the group retry topic or DLQ.
// RetryProcessing schedules a retry to the group retry topic or DLQ.
func (b *Broker) RetryProcessing(msgID, group, topic string) bool {
	if group == "" || topic == "" {
		return false
	}
	key := processingKey(group, topic, msgID)
	b.lock.Lock()
	entry, ok := b.processing[key]
	if !ok || entry.State != stateProcessing {
		b.lock.Unlock()
		return false
	}
	retryCount := b.retryCounts[msgID] + 1
	b.retryCounts[msgID] = retryCount
	entry.State = stateRetry
	entry.UpdatedAt = time.Now()
	entry.Retry = retryCount
	gs := b.stats[entry.Group]
	if gs != nil {
		gs.Processing--
		gs.Retry++
	}
	delete(b.processing, key)

	// exceed max retry -> send to DLQ
	if retryCount > b.maxRetry {
		b.clearRetryCount(msgID)
		b.lock.Unlock()
		if ps, ok := b.store.(ProcessingStore); ok {
			_ = ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, msgID)
		}
		dlqTopic := entry.Topic + ".dlq"
		_ = b.store.Append(dlqTopic, 0, storage.Message{
			ID:            entry.MsgID,
			Body:          entry.Body,
			Tag:           entry.Tag,
			CorrelationID: entry.CorrelationID,
			Retry:         retryCount,
			Timestamp:     entry.Timestamp,
		})
		return true
	}

	group = entry.Group
	topic = entry.Topic
	queueID := entry.QueueID
	msg := storage.Message{
		ID:            entry.MsgID,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         retryCount,
		Timestamp:     entry.Timestamp,
	}
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		_ = ps.RemoveProcessing(group, topic, queueID, msgID)
	}

	retryTopic, err := b.ensureRetryTopic(group, topic)
	if err != nil {
		logger.Error("Failed to ensure retry topic",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Error(err))
		return false
	}

	// Schedule retry to group retry topic (RocketMQ-style), no offset rollback
	delay := b.calculateRetryBackoff(retryCount)
	b.delayScheduler.ScheduleWithDelay(retryTopic, queueID, msg, delay)
	return true
}

// CompleteProcessing marks a message completed (offset already committed in ConsumeWithLock).
func (b *Broker) CompleteProcessing(msgID, group, topic string) bool {
	if group == "" || topic == "" {
		return false
	}
	key := processingKey(group, topic, msgID)
	b.lock.Lock()
	entry, ok := b.processing[key]
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
	ackedAt := now()
	entry.UpdatedAt = ackedAt
	gs := b.stats[entry.Group]
	if gs != nil {
		gs.Processing--
		gs.Completed++
	}
	delete(b.processing, key)

	ctKey := groupTopicKey(entry.Group, entry.Topic)
	completed := b.completed[ctKey]
	completed = append(completed, completedEntry{
		Group:         entry.Group,
		Topic:         entry.Topic,
		QueueID:       entry.QueueID,
		Offset:        entry.Offset,
		NextOffset:    entry.NextOffset,
		MsgID:         entry.MsgID,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		ConsumedAt:    entry.ConsumedAt,
		AckedAt:       ackedAt,
	})
	if b.completedLimit > 0 && len(completed) > b.completedLimit {
		completed = completed[len(completed)-b.completedLimit:]
	}
	b.completed[ctKey] = completed
	b.clearRetryCount(msgID)
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		_ = ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, msgID)
	}
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

// ListProcessing returns in-flight processing entries for a group/topic.
func (b *Broker) ListProcessing(group, topic string, limit int) []processingEntry {
	if limit <= 0 {
		limit = 50
	}
	b.lock.Lock()
	entries := make([]processingEntry, 0)
	for _, entry := range b.processing {
		if entry.Group == group && entry.Topic == topic && entry.State == stateProcessing {
			entries = append(entries, entry)
		}
	}
	b.lock.Unlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].ConsumedAt.After(entries[j].ConsumedAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}

// ListCompleted returns recent acked entries for a group/topic.
func (b *Broker) ListCompleted(group, topic string, limit int) []completedEntry {
	if limit <= 0 {
		limit = 50
	}
	b.lock.Lock()
	entries := append([]completedEntry(nil), b.completed[groupTopicKey(group, topic)]...)
	b.lock.Unlock()
	if len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}
	return entries
}

func (b *Broker) topicQueueCount(topic string) int {
	if count, ok := b.PeekQueueCount(topic); ok && count > 0 {
		return count
	}
	if cfg, err := b.topicManager.GetTopicConfig(topic); err == nil && cfg.QueueCount > 0 {
		return cfg.QueueCount
	}
	return b.queueCount
}

func (b *Broker) cacheCancelled(entry cancelledEntry) {
	if entry.State == "" {
		entry.State = string(stateCancelled)
	}
	key := cancelledKey(entry.Topic, entry.MsgID)
	b.lock.Lock()
	if _, ok := b.cancelled[key]; !ok {
		b.cancelledOrder = append(b.cancelledOrder, key)
	}
	b.cancelled[key] = entry
	for b.cancelledCacheLimit > 0 && len(b.cancelled) > b.cancelledCacheLimit && len(b.cancelledOrder) > 0 {
		oldest := b.cancelledOrder[0]
		b.cancelledOrder = b.cancelledOrder[1:]
		delete(b.cancelled, oldest)
	}
	b.lock.Unlock()
}

func (b *Broker) loadCancelled(topic, msgID string) (cancelledEntry, bool) {
	key := cancelledKey(topic, msgID)
	b.lock.Lock()
	if entry, ok := b.cancelled[key]; ok {
		b.lock.Unlock()
		return entry, true
	}
	b.lock.Unlock()

	cs, ok := b.store.(CancelledLookupStore)
	if !ok {
		return cancelledEntry{}, false
	}
	rec, found, err := cs.LoadCancelledByID(topic, msgID)
	if err != nil || !found {
		return cancelledEntry{}, false
	}
	entry := cancelledEntryFromRecord(rec)
	b.cacheCancelled(entry)
	return entry, true
}

func (b *Broker) IsCancelled(topic, msgID string) bool {
	entry, ok := b.loadCancelled(topic, msgID)
	return ok && entry.State == string(stateCancelled)
}

func (b *Broker) IsExpired(topic, msgID string) bool {
	entry, ok := b.loadCancelled(topic, msgID)
	return ok && entry.State == string(stateExpired)
}

func (b *Broker) IsTerminal(topic, msgID string) bool {
	entry, ok := b.loadCancelled(topic, msgID)
	return ok && (entry.State == string(stateCancelled) || entry.State == string(stateExpired))
}

func (b *Broker) shouldExpire(ts time.Time) bool {
	if ts.IsZero() || b.messageRetention <= 0 || b.messageExpiryFactor <= 0 {
		return false
	}
	return now().Sub(ts) > time.Duration(b.messageExpiryFactor)*b.messageRetention
}

func (b *Broker) persistCancelled(entry cancelledEntry) {
	cs, ok := b.store.(CancelledStore)
	if !ok {
		return
	}
	_ = cs.SaveCancelled(storage.CancelledRecord{
		Group:         entry.Group,
		Topic:         entry.Topic,
		MsgID:         entry.MsgID,
		State:         entry.State,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		QueueID:       entry.QueueID,
		Offset:        entry.Offset,
		NextOffset:    entry.NextOffset,
		ScheduledAt:   entry.ScheduledAt,
		ConsumedAt:    entry.ConsumedAt,
		CancelledAt:   entry.CancelledAt,
	})
}

func (b *Broker) recordCancelled(entry cancelledEntry) {
	if entry.CancelledAt.IsZero() {
		entry.CancelledAt = now()
	}
	entry.Group = ""
	if entry.State == "" {
		entry.State = string(stateCancelled)
	}
	b.cacheCancelled(entry)
	b.lock.Lock()
	b.clearRetryCount(entry.MsgID)
	b.lock.Unlock()
	b.persistCancelled(entry)
}

func (b *Broker) recordExpired(entry cancelledEntry) {
	entry.State = string(stateExpired)
	b.recordCancelled(entry)
}

func (b *Broker) cancelProcessing(msgID, topic string) bool {
	cancelledAt := now()

	b.lock.Lock()
	keys := make([]string, 0)
	entries := make([]processingEntry, 0)
	for key, entry := range b.processing {
		if entry.Topic != topic || entry.MsgID != msgID || entry.State != stateProcessing {
			continue
		}
		keys = append(keys, key)
		entries = append(entries, entry)
	}
	if len(entries) == 0 {
		b.lock.Unlock()
		return false
	}
	first := entries[0]
	consumedAt := first.ConsumedAt
	queueID := first.QueueID
	offset := first.Offset
	nextOffset := first.NextOffset
	cancelled := cancelledEntry{
		Topic:         first.Topic,
		State:         string(stateCancelled),
		QueueID:       &queueID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         first.MsgID,
		Body:          first.Body,
		Tag:           first.Tag,
		CorrelationID: first.CorrelationID,
		Retry:         first.Retry,
		Timestamp:     first.Timestamp,
		ConsumedAt:    &consumedAt,
		CancelledAt:   cancelledAt,
	}
	for i, key := range keys {
		entry := entries[i]
		delete(b.processing, key)
		if gs := b.stats[entry.Group]; gs != nil {
			gs.Processing--
		}
	}
	b.clearRetryCount(msgID)
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		for _, entry := range entries {
			_ = ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, msgID)
		}
	}
	b.cacheCancelled(cancelled)
	b.persistCancelled(cancelled)
	return true
}

func (b *Broker) findScheduledMessage(topic, msgID string) (cancelledEntry, bool) {
	ds := b.GetDelayScheduler()
	if ds == nil {
		return cancelledEntry{}, false
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()
	for _, dm := range ds.delayQueue {
		if dm.Topic != topic || dm.Message.ID != msgID {
			continue
		}
		queueID := dm.QueueID
		scheduledAt := dm.ExecuteAt
		return cancelledEntry{
			Topic:         topic,
			QueueID:       &queueID,
			MsgID:         dm.Message.ID,
			Body:          dm.Message.Body,
			Tag:           dm.Message.Tag,
			CorrelationID: dm.Message.CorrelationID,
			Retry:         dm.Message.Retry,
			Timestamp:     dm.Message.Timestamp,
			ScheduledAt:   &scheduledAt,
			CancelledAt:   now(),
		}, true
	}
	return cancelledEntry{}, false
}

func (b *Broker) findPendingMessage(topic, msgID string) (cancelledEntry, bool) {
	queueCount := b.topicQueueCount(topic)
	for queueID := 0; queueID < queueCount; queueID++ {
		cursor := int64(0)
		for {
			msgs, nextCursor, err := b.ReadFromConsumeQueueWithOffsets(topic, queueID, cursor, 128, "")
			if err != nil {
				break
			}
			if len(msgs) == 0 {
				break
			}
			for _, msg := range msgs {
				if msg.ID != msgID {
					continue
				}
				offset := msg.Offset
				nextOffset := msg.Offset + 1
				queueIDCopy := queueID
				return cancelledEntry{
					Topic:         topic,
					QueueID:       &queueIDCopy,
					Offset:        &offset,
					NextOffset:    &nextOffset,
					MsgID:         msg.ID,
					Body:          msg.Body,
					Tag:           msg.Tag,
					CorrelationID: msg.CorrelationID,
					Retry:         msg.Retry,
					Timestamp:     msg.Timestamp,
					CancelledAt:   now(),
				}, true
			}
			cursor = nextCursor
		}
	}
	return cancelledEntry{}, false
}

func (b *Broker) listTerminalByState(topic, state string, limit int) []cancelledEntry {
	if limit <= 0 {
		limit = 50
	}
	if cs, ok := b.store.(CancelledLookupStore); ok {
		records, err := cs.ListCancelledByTopic(topic, state, limit)
		if err == nil {
			entries := make([]cancelledEntry, 0, len(records))
			for _, rec := range records {
				entry := cancelledEntryFromRecord(rec)
				b.cacheCancelled(entry)
				entries = append(entries, entry)
			}
			return entries
		}
	}
	b.lock.Lock()
	defer b.lock.Unlock()
	entries := make([]cancelledEntry, 0)
	for _, entry := range b.cancelled {
		if entry.Topic == topic && entry.State == state {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CancelledAt.After(entries[j].CancelledAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}

// ListCancelled returns recent cancelled entries for a topic.
func (b *Broker) ListCancelled(topic string, limit int) []cancelledEntry {
	return b.listTerminalByState(topic, string(stateCancelled), limit)
}

// ListExpired returns recent expired entries for a topic.
func (b *Broker) ListExpired(topic string, limit int) []cancelledEntry {
	return b.listTerminalByState(topic, string(stateExpired), limit)
}

func (b *Broker) logicalTopicForStorageTopic(topic string) string {
	if topic == SystemDelayTopic {
		return topic
	}
	if logicalTopic, _, ok := splitRetryTopicName(topic); ok {
		return logicalTopic
	}
	return topic
}

func (b *Broker) onPrunedMessage(storageTopic string, queueID int, msg storage.Message) error {
	if storageTopic == SystemDelayTopic {
		return nil
	}
	logicalTopic := b.logicalTopicForStorageTopic(storageTopic)
	if b.IsTerminal(logicalTopic, msg.ID) {
		return nil
	}
	queueIDCopy := queueID
	b.recordExpired(cancelledEntry{
		Topic:         logicalTopic,
		QueueID:       &queueIDCopy,
		MsgID:         msg.ID,
		Body:          msg.Body,
		Tag:           msg.Tag,
		CorrelationID: msg.CorrelationID,
		Retry:         msg.Retry,
		Timestamp:     msg.Timestamp,
		CancelledAt:   now(),
	})
	return nil
}

func (b *Broker) applyRetentionSettings(retention time.Duration, factor int) {
	if retention <= 0 || factor <= 0 {
		return
	}
	if rs, ok := b.store.(SegmentRetentionStore); ok {
		rs.SetRetentionWindow(time.Duration(factor) * retention)
	}
}

// PeekQueueCount returns the number of queues for a topic if already initialized.
// It does not create queues.
func (b *Broker) PeekQueueCount(topic string) (int, bool) {
	b.lock.Lock()
	defer b.lock.Unlock()
	qs, ok := b.queues[topic]
	if !ok {
		return 0, false
	}
	return len(qs), true
}

// PeekTopicQueueTotal returns the number of topics with initialized queues.
// It does not create queues.
func (b *Broker) PeekTopicQueueTotal() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.queues)
}

// TopicStats holds per-topic message statistics.
type TopicStats struct {
	Name       string    `json:"name"`
	Type       TopicType `json:"type"`
	QueueCount int       `json:"queueCount"`
	Total      int64     `json:"total"` // total messages produced
	CreatedAt  int64     `json:"createdAt"`
}

// ConsumerGroupTopicStats holds per-topic stats within a consumer group.
type ConsumerGroupTopicStats struct {
	Topic      string `json:"topic"`
	Total      int64  `json:"total"`      // total messages in the topic
	Completed  int64  `json:"completed"`  // acked messages
	Processing int64  `json:"processing"` // in-flight messages
	Pending    int64  `json:"pending"`    // waiting to be consumed
}

// ConsumerGroupStats holds per-consumer-group statistics.
type ConsumerGroupStats struct {
	Group      string                    `json:"group"`
	Total      int64                     `json:"total"`
	Completed  int64                     `json:"completed"`
	Processing int64                     `json:"processing"`
	Pending    int64                     `json:"pending"`
	Topics     []ConsumerGroupTopicStats `json:"topics"`
}

// BrokerStats holds the full broker statistics.
type BrokerStats struct {
	Topics         []TopicStats           `json:"topics"`
	ConsumerGroups []ConsumerGroupStats   `json:"consumerGroups"`
	DelayScheduler map[string]interface{} `json:"delayScheduler,omitempty"`
	Total          int64                  `json:"total"`
	Completed      int64                  `json:"completed"`
	Processing     int64                  `json:"processing"`
	Pending        int64                  `json:"pending"`
}

// consumeQueueDepthReader is an optional interface for storage to report queue depth.
type consumeQueueDepthReader interface {
	ConsumeQueueDepth(topic string, queueID int) int64
}

// topicDepth returns the total message count (consumequeue depth) for a topic.
func (b *Broker) topicDepth(topic string, queueCount int) int64 {
	dr, ok := b.store.(consumeQueueDepthReader)
	if !ok {
		return 0
	}
	var total int64
	for i := 0; i < queueCount; i++ {
		total += dr.ConsumeQueueDepth(topic, i)
	}
	return total
}

// topicConsumed returns the total consumed offset (sum across all queues) for a group/topic.
func (b *Broker) topicConsumed(group, topic string, queueCount int) int64 {
	var total int64
	for i := 0; i < queueCount; i++ {
		off, ok, err := b.GetOffset(group, topic, i)
		if err == nil && ok {
			total += off
		}
	}
	return total
}

// countProcessing returns the number of in-flight processing entries for a group/topic.
// Must be called with b.lock held.
func (b *Broker) countProcessingLocked(group, topic string) int64 {
	var count int64
	for _, entry := range b.processing {
		if entry.Group == group && entry.Topic == topic && entry.State == stateProcessing {
			count++
		}
	}
	return count
}

// FullStats returns comprehensive broker statistics.
func (b *Broker) FullStats() BrokerStats {
	topicConfigs := b.topicManager.ListUserTopics()

	// Build topic depth map
	topicDepthMap := make(map[string]int64, len(topicConfigs))
	topicQueueCountMap := make(map[string]int, len(topicConfigs))
	topicStatsList := make([]TopicStats, 0, len(topicConfigs))
	var grandTotal int64

	for _, tc := range topicConfigs {
		qc := tc.QueueCount
		if qc <= 0 {
			qc = b.queueCount
		}
		depth := b.topicDepth(tc.Name, qc)
		topicDepthMap[tc.Name] = depth
		topicQueueCountMap[tc.Name] = qc
		grandTotal += depth

		topicStatsList = append(topicStatsList, TopicStats{
			Name:       tc.Name,
			Type:       tc.Type,
			QueueCount: qc,
			Total:      depth,
			CreatedAt:  tc.CreatedAt,
		})
	}

	// Discover consumer groups from b.stats
	b.lock.Lock()
	groupSet := make(map[string]bool, len(b.stats))
	for g := range b.stats {
		groupSet[g] = true
	}

	// Also discover groups from processing entries
	for _, entry := range b.processing {
		groupSet[entry.Group] = true
	}
	b.lock.Unlock()

	// Build consumer group stats
	var totalCompleted, totalProcessing, totalPending int64
	groupStatsList := make([]ConsumerGroupStats, 0, len(groupSet))

	for group := range groupSet {
		var gCompleted, gProcessing, gPending, gTotal int64
		topicStats := make([]ConsumerGroupTopicStats, 0)

		for _, tc := range topicConfigs {
			qc := topicQueueCountMap[tc.Name]
			depth := topicDepthMap[tc.Name]
			consumed := b.topicConsumed(group, tc.Name, qc)

			b.lock.Lock()
			processing := b.countProcessingLocked(group, tc.Name)
			b.lock.Unlock()

			// completed = consumed - processing (consumed offset includes processing messages)
			completed := consumed - processing
			if completed < 0 {
				completed = 0
			}

			// pending = total depth - consumed
			pending := depth - consumed
			if pending < 0 {
				pending = 0
			}

			// Only include topics that this group has interacted with
			if consumed > 0 || processing > 0 {
				topicStats = append(topicStats, ConsumerGroupTopicStats{
					Topic:      tc.Name,
					Total:      depth,
					Completed:  completed,
					Processing: processing,
					Pending:    pending,
				})
				gTotal += depth
				gCompleted += completed
				gProcessing += processing
				gPending += pending
			}
		}

		if len(topicStats) > 0 {
			groupStatsList = append(groupStatsList, ConsumerGroupStats{
				Group:      group,
				Total:      gTotal,
				Completed:  gCompleted,
				Processing: gProcessing,
				Pending:    gPending,
				Topics:     topicStats,
			})
			totalCompleted += gCompleted
			totalProcessing += gProcessing
			totalPending += gPending
		}
	}

	// Delay scheduler stats
	var delayStats map[string]interface{}
	if ds := b.GetDelayScheduler(); ds != nil {
		delayStats = ds.Stats()
	}

	return BrokerStats{
		Topics:         topicStatsList,
		ConsumerGroups: groupStatsList,
		DelayScheduler: delayStats,
		Total:          grandTotal,
		Completed:      totalCompleted,
		Processing:     totalProcessing,
		Pending:        totalPending,
	}
}

// TopicFullStats returns detailed stats for a specific topic across all consumer groups.
func (b *Broker) TopicFullStats(topicName string) (*TopicStats, []ConsumerGroupTopicStats, error) {
	tc, err := b.topicManager.GetTopicConfig(topicName)
	if err != nil {
		return nil, nil, err
	}
	qc := tc.QueueCount
	if qc <= 0 {
		qc = b.queueCount
	}
	depth := b.topicDepth(tc.Name, qc)

	ts := &TopicStats{
		Name:       tc.Name,
		Type:       tc.Type,
		QueueCount: qc,
		Total:      depth,
		CreatedAt:  tc.CreatedAt,
	}

	// Collect per-consumer-group stats for this topic
	b.lock.Lock()
	groupSet := make(map[string]bool, len(b.stats))
	for g := range b.stats {
		groupSet[g] = true
	}
	for _, entry := range b.processing {
		if entry.Topic == topicName {
			groupSet[entry.Group] = true
		}
	}
	b.lock.Unlock()

	consumerStats := make([]ConsumerGroupTopicStats, 0)
	for group := range groupSet {
		consumed := b.topicConsumed(group, topicName, qc)

		b.lock.Lock()
		processing := b.countProcessingLocked(group, topicName)
		b.lock.Unlock()

		if consumed == 0 && processing == 0 {
			continue
		}

		completed := consumed - processing
		if completed < 0 {
			completed = 0
		}
		pending := depth - consumed
		if pending < 0 {
			pending = 0
		}

		consumerStats = append(consumerStats, ConsumerGroupTopicStats{
			Topic:      group, // repurpose Topic field as group name in per-topic view
			Total:      depth,
			Completed:  completed,
			Processing: processing,
			Pending:    pending,
		})
	}

	return ts, consumerStats, nil
}

// ListPending reads pending messages from consumequeue starting at cursor or group offset.
func (b *Broker) ListPending(group, topic string, queueID int, cursor *int64, limit int, tag string) ([]storage.MessageWithOffset, int64, error) {
	if limit <= 0 {
		limit = 50
	}
	start := int64(0)
	if cursor != nil {
		start = *cursor
	} else {
		off, ok, err := b.GetOffset(group, topic, queueID)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			start = off
		} else {
			start, err = b.resolveInitialOffset(group, topic, queueID)
			if err != nil {
				return nil, 0, err
			}
		}
	}

	scanCursor := start
	out := make([]storage.MessageWithOffset, 0, limit)
	for len(out) < limit {
		batchSize := limit - len(out)
		if batchSize < 50 {
			batchSize = 50
		}
		msgs, nextCursor, err := b.ReadFromConsumeQueueWithOffsets(topic, queueID, scanCursor, batchSize, tag)
		if err != nil {
			return nil, 0, err
		}
		if len(msgs) == 0 {
			return out, nextCursor, nil
		}
		scanCursor = nextCursor
		for _, msg := range msgs {
			if b.IsTerminal(topic, msg.ID) {
				continue
			}
			if b.shouldExpire(msg.Timestamp) {
				qid := queueID
				off := msg.Offset
				next := msg.Offset + 1
				b.recordExpired(cancelledEntry{
					Topic:         topic,
					QueueID:       &qid,
					Offset:        &off,
					NextOffset:    &next,
					MsgID:         msg.ID,
					Body:          msg.Body,
					Tag:           msg.Tag,
					CorrelationID: msg.CorrelationID,
					Retry:         msg.Retry,
					Timestamp:     msg.Timestamp,
					CancelledAt:   now(),
				})
				continue
			}
			out = append(out, msg)
			if len(out) == limit {
				break
			}
		}
	}
	return out, scanCursor, nil
}

// ListScheduledVisible returns scheduled delayed messages after filtering cancelled ones.
func (b *Broker) ListScheduledVisible(topic string, queueID *int, cursor int64, limit int) ([]DelayedMessage, *int64) {
	ds := b.GetDelayScheduler()
	if ds == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}

	scanCursor := cursor
	out := make([]DelayedMessage, 0, limit)
	for len(out) < limit {
		batchSize := limit - len(out)
		if batchSize < 50 {
			batchSize = 50
		}
		items, next := ds.ListScheduled(topic, queueID, scanCursor, batchSize)
		if len(items) == 0 {
			return out, next
		}
		for _, item := range items {
			if b.IsTerminal(topic, item.Message.ID) {
				continue
			}
			if b.shouldExpire(item.Message.Timestamp) {
				qid := item.QueueID
				scheduledAt := item.ExecuteAt
				b.recordExpired(cancelledEntry{
					Topic:         topic,
					QueueID:       &qid,
					MsgID:         item.Message.ID,
					Body:          item.Message.Body,
					Tag:           item.Message.Tag,
					CorrelationID: item.Message.CorrelationID,
					Retry:         item.Message.Retry,
					Timestamp:     item.Message.Timestamp,
					ScheduledAt:   &scheduledAt,
					CancelledAt:   now(),
				})
				continue
			}
			out = append(out, item)
			if len(out) == limit {
				break
			}
		}
		if next == nil {
			return out, nil
		}
		scanCursor = *next
	}

	n := scanCursor
	return out, &n
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
	return b.topicManager.ListUserTopics()
}

// ValidateProcessing checks whether a message is currently processing and matches optional group/topic.
func (b *Broker) ValidateProcessing(msgID, group, topic string) bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	if group == "" || topic == "" {
		return false
	}
	entry, ok := b.processing[processingKey(group, topic, msgID)]
	if !ok || entry.State != stateProcessing {
		return false
	}
	return true
}

// TerminateMessage marks a message cancelled for a topic.
// It is intentionally idempotent: unknown or already-terminated messages still succeed.
func (b *Broker) TerminateMessage(msgID, topic string) bool {
	if topic == "" || msgID == "" {
		return false
	}
	if b.IsTerminal(topic, msgID) {
		return true
	}
	if b.cancelProcessing(msgID, topic) {
		return true
	}
	if entry, ok := b.findScheduledMessage(topic, msgID); ok {
		b.recordCancelled(entry)
		return true
	}
	if entry, ok := b.findPendingMessage(topic, msgID); ok {
		b.recordCancelled(entry)
		return true
	}
	return true
}

// TerminateMessages marks multiple messages cancelled for a topic.
// Unknown or already-terminated message IDs are treated as successful.
func (b *Broker) TerminateMessages(topic string, msgIDs []string) int {
	terminated := 0
	for _, msgID := range msgIDs {
		if b.TerminateMessage(msgID, topic) {
			terminated++
		}
	}
	return terminated
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
	close(b.stopChan) // 停止超时检查
	if b.delayScheduler != nil {
		b.delayScheduler.Stop()
	}
	return nil
}

// GetDelayScheduler returns the delay scheduler for direct access
func (b *Broker) GetDelayScheduler() *BinaryDelayScheduler {
	return b.delayScheduler
}

// AddAK adds an access key to the store and returns its ID.
func (b *Broker) AddAK(name string, ak string) (*storage.AccessKeyRecord, error) {
	if b.akStore == nil {
		return nil, fmt.Errorf("ak store not initialized")
	}
	record, err := b.akStore.Add(name, ak)
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// RemoveAK removes an access key by ID from the store.
func (b *Broker) RemoveAK(id string) error {
	if b.akStore == nil {
		return fmt.Errorf("ak store not initialized")
	}
	return b.akStore.Remove(id)
}

// ListAKs returns all access key info.
func (b *Broker) ListAKs() []storage.AccessKeyInfo {
	if b.akStore == nil {
		return nil
	}
	return b.akStore.List()
}

// IsAKValid checks if an access key exists.
func (b *Broker) IsAKValid(ak string) bool {
	if b.akStore == nil {
		return false
	}
	return b.akStore.Has(ak)
}

// checkProcessingTimeouts 定期检查超时的处理中消息并自动重试
func (b *Broker) checkProcessingTimeouts() {
	ticker := time.NewTicker(5 * time.Second) // 每 5 秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			return
		case <-ticker.C:
			b.lock.Lock()
			now := time.Now()
			var timeoutEntries []processingEntry

			// 查找超时的消息
			for _, entry := range b.processing {
				if entry.State == stateProcessing && now.Sub(entry.UpdatedAt) > b.processingTimeout {
					timeoutEntries = append(timeoutEntries, entry)
					logger.Warn("Message processing timeout",
						zap.String("message_id", entry.MsgID),
						zap.String("group", entry.Group),
						zap.String("topic", entry.Topic),
						zap.Duration("elapsed", now.Sub(entry.UpdatedAt)))
				}
			}
			b.lock.Unlock()

			// 对超时的消息执行重试
			for _, entry := range timeoutEntries {
				b.RetryProcessing(entry.MsgID, entry.Group, entry.Topic)
			}
		}
	}
}

// SetMaxRetry sets the maximum retry count for messages.
func (b *Broker) SetMaxRetry(maxRetry int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.maxRetry = maxRetry
}

// SetProcessingTimeout sets the message processing timeout.
func (b *Broker) SetProcessingTimeout(timeout time.Duration) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.processingTimeout = timeout
}

// SetRetryBackoff sets the retry backoff parameters.
func (b *Broker) SetRetryBackoff(base time.Duration, multiplier float64, max time.Duration) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.retryBackoffBase = base
	b.retryBackoffMultiplier = multiplier
	b.retryBackoffMax = max
}

// SetMessageRetention sets the retention window used by terminal-expiry checks.
func (b *Broker) SetMessageRetention(retention time.Duration) {
	b.lock.Lock()
	if retention > 0 {
		b.messageRetention = retention
	}
	currentRetention := b.messageRetention
	currentFactor := b.messageExpiryFactor
	b.lock.Unlock()
	b.applyRetentionSettings(currentRetention, currentFactor)
}

// SetMessageExpiryFactor sets the multiplier applied to retention before auto-expiring a message.
func (b *Broker) SetMessageExpiryFactor(factor int) {
	b.lock.Lock()
	if factor > 0 {
		b.messageExpiryFactor = factor
	}
	currentRetention := b.messageRetention
	currentFactor := b.messageExpiryFactor
	b.lock.Unlock()
	b.applyRetentionSettings(currentRetention, currentFactor)
}

// SetNewGroupStartPosition sets the initial offset policy for groups without committed offsets.
func (b *Broker) SetNewGroupStartPosition(position string) {
	b.lock.Lock()
	defer b.lock.Unlock()
	switch strings.ToLower(strings.TrimSpace(position)) {
	case "earliest":
		b.newGroupStartPosition = "earliest"
	default:
		b.newGroupStartPosition = "latest"
	}
}

// SetCancelledCacheLimit sets the in-memory terminal cache size.
func (b *Broker) SetCancelledCacheLimit(limit int) {
	b.lock.Lock()
	defer b.lock.Unlock()
	if limit <= 0 {
		b.cancelledCacheLimit = defaultCancelledCacheLimit
		return
	}
	b.cancelledCacheLimit = limit
	for len(b.cancelled) > b.cancelledCacheLimit && len(b.cancelledOrder) > 0 {
		oldest := b.cancelledOrder[0]
		b.cancelledOrder = b.cancelledOrder[1:]
		delete(b.cancelled, oldest)
	}
}

// CalculateRetryBackoff returns the retry backoff delay for the given retry count.
func (b *Broker) CalculateRetryBackoff(retryCount int) time.Duration {
	return b.calculateRetryBackoff(retryCount)
}

// calculateRetryBackoff calculates retry backoff delay using configured parameters.
func (b *Broker) calculateRetryBackoff(retryCount int) time.Duration {
	b.lock.Lock()
	base := b.retryBackoffBase
	multiplier := b.retryBackoffMultiplier
	max := b.retryBackoffMax
	b.lock.Unlock()

	if retryCount <= 0 {
		return base
	}

	// Calculate: base * (multiplier ^ retryCount)
	delay := float64(base) * math.Pow(multiplier, float64(retryCount))
	duration := time.Duration(delay)

	// Cap at max
	if duration > max {
		duration = max
	}

	return duration
}
