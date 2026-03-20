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
const defaultNewGroupStartPosition = "topic_progress"
const defaultTombstoneSweepInterval = 30 * time.Second
const defaultConsumedPruneInterval = 30 * time.Second

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
	StorageTopic  string
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
	StorageTopic  string
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

type deliveryEventEntry struct {
	Group         string
	Topic         string
	StorageTopic  string
	Event         string
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
	EventAt       time.Time
}

type cancelledEntry struct {
	Group         string
	Topic         string
	StorageTopic  string
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
	go b.sweepTerminalTombstonesLoop()
	go b.pruneConsumedSegmentsLoop()

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
	var ps ProcessingStore
	if store, ok := b.store.(ProcessingStore); ok {
		ps = store
	}

	var records []storage.ProcessingRecord
	if ps != nil {
		var err error
		records, err = ps.LoadProcessing()
		if err != nil {
			logger.Warn("Failed to load processing records", zap.Error(err))
		}
	}

	latestByGroupTopic := make(map[string]map[string]storage.DeliveryEventRecord)
	loadLatest := func(group, topic string) map[string]storage.DeliveryEventRecord {
		key := groupTopicKey(group, topic)
		if latest, ok := latestByGroupTopic[key]; ok {
			return latest
		}
		latest, supported, err := b.latestGroupTopicDeliveryEvents(group, topic)
		if err != nil {
			logger.Warn("Failed to load delivery events for recovery",
				zap.String("group", group),
				zap.String("topic", topic),
				zap.Error(err))
			latestByGroupTopic[key] = nil
			return nil
		}
		if !supported {
			latestByGroupTopic[key] = nil
			return nil
		}
		latestByGroupTopic[key] = latest
		return latest
	}

	recoverRecord := func(persisted *storage.ProcessingRecord, effective storage.ProcessingRecord) {
		removePersisted := func() {
			if ps == nil || persisted == nil {
				return
			}
			_ = ps.RemoveProcessing(persisted.Group, persisted.Topic, persisted.QueueID, persisted.MsgID)
		}
		if b.shouldExpire(effective.Timestamp) {
			if err := b.recordExpired(terminalEntryFromProcessingRecord(effective, now())); err != nil {
				logger.Error("Failed to record expired processing during recovery",
					zap.String("group", effective.Group),
					zap.String("topic", effective.Topic),
					zap.String("message_id", effective.MsgID),
					zap.Error(err))
				return
			}
			removePersisted()
			return
		}

		retryTopic, err := b.ensureRetryTopic(effective.Group, effective.Topic)
		if err != nil {
			logger.Error("Failed to ensure retry topic on recovery",
				zap.String("group", effective.Group),
				zap.String("topic", effective.Topic),
				zap.Error(err))
			return
		}

		// schedule retry with remaining timeout window
		deadline := effective.UpdatedAt.Add(b.processingTimeout)
		delay := time.Until(deadline)
		if delay < 0 {
			delay = 0
		}
		retryCount := effective.Retry + 1
		msg := storage.Message{
			ID:            effective.MsgID,
			Body:          effective.Body,
			Tag:           effective.Tag,
			CorrelationID: effective.CorrelationID,
			Retry:         retryCount,
			Timestamp:     effective.Timestamp,
		}
		if err := b.delayScheduler.ScheduleWithDelayStrict(retryTopic, effective.QueueID, msg, delay); err != nil {
			logger.Error("Failed to schedule retry during recovery",
				zap.String("group", effective.Group),
				zap.String("topic", effective.Topic),
				zap.String("message_id", effective.MsgID),
				zap.Error(err))
			return
		}
		b.retryCounts[effective.MsgID] = retryCount

		eventAt := now()
		retryEvent := deliveryEventFromProcessingRecord(storage.DeliveryEventRetry, effective, eventAt)
		retryEvent.Retry = retryCount
		scheduledAt := eventAt.Add(delay)
		retryEvent.ScheduledAt = &scheduledAt
		if err := b.appendDeliveryEvent(retryEvent); err != nil {
			if rollbackErr := b.delayScheduler.RemoveScheduled(retryTopic, effective.QueueID, effective.MsgID); rollbackErr != nil {
				logger.Error("Failed to rollback scheduled retry during recovery",
					zap.String("group", effective.Group),
					zap.String("topic", effective.Topic),
					zap.String("message_id", effective.MsgID),
					zap.Error(rollbackErr))
			}
			logger.Error("Failed to append retry delivery event during recovery",
				zap.String("group", effective.Group),
				zap.String("topic", effective.Topic),
				zap.String("message_id", effective.MsgID),
				zap.Error(err))
			return
		}

		removePersisted()
	}

	seen := make(map[string]struct{}, len(records))
	for _, rec := range records {
		key := processingKey(rec.Group, rec.Topic, rec.MsgID)
		seen[key] = struct{}{}

		effective := rec
		if latestEvents := loadLatest(rec.Group, rec.Topic); latestEvents != nil {
			if latest, ok := latestEvents[rec.MsgID]; ok {
				if isSettlingDeliveryEvent(latest.Event) {
					if ps != nil {
						_ = ps.RemoveProcessing(rec.Group, rec.Topic, rec.QueueID, rec.MsgID)
					}
					continue
				}
				if replayRec, ok := processingRecordFromDeliveryEvent(latest); ok {
					effective = replayRec
				}
			}
		}
		recoverRecord(&rec, effective)
	}

	gls, ok := b.store.(GroupDeliveryTopicStore)
	if !ok {
		return
	}
	namespaces, err := gls.ListGroupDeliveryTopics()
	if err != nil {
		logger.Warn("Failed to enumerate delivery event namespaces for recovery", zap.Error(err))
		return
	}
	for group, topics := range namespaces {
		for _, topic := range topics {
			latestEvents := loadLatest(group, topic)
			if latestEvents == nil {
				continue
			}
			for msgID, latest := range latestEvents {
				if latest.Event != storage.DeliveryEventProcessing {
					continue
				}
				key := processingKey(group, topic, msgID)
				if _, ok := seen[key]; ok {
					continue
				}
				replayRec, ok := processingRecordFromDeliveryEvent(latest)
				if !ok {
					continue
				}
				seen[key] = struct{}{}
				recoverRecord(nil, replayRec)
			}
		}
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
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	return cancelledEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
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

func deliveryEventFromProcessingEntry(event string, entry processingEntry, eventAt time.Time) storage.DeliveryEventRecord {
	storageTopic := entry.StorageTopic
	if storageTopic == "" {
		storageTopic = entry.Topic
	}
	queueID := entry.QueueID
	offset := entry.Offset
	nextOffset := entry.NextOffset
	consumedAt := entry.ConsumedAt
	return storage.DeliveryEventRecord{
		Event:         event,
		Group:         entry.Group,
		Topic:         entry.Topic,
		StorageTopic:  storageTopic,
		QueueID:       &queueID,
		MsgID:         entry.MsgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		ConsumedAt:    &consumedAt,
		EventAt:       eventAt,
	}
}

func deliveryEventFromProcessingRecord(event string, rec storage.ProcessingRecord, eventAt time.Time) storage.DeliveryEventRecord {
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	var consumedAt *time.Time
	if !rec.UpdatedAt.IsZero() {
		consumed := rec.UpdatedAt
		consumedAt = &consumed
	}
	return storage.DeliveryEventRecord{
		Event:         event,
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
		QueueID:       &rec.QueueID,
		MsgID:         rec.MsgID,
		Offset:        rec.Offset,
		NextOffset:    rec.NextOffset,
		Body:          rec.Body,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		ConsumedAt:    consumedAt,
		EventAt:       eventAt,
	}
}

func deliveryEventFromCancelledEntry(entry cancelledEntry) storage.DeliveryEventRecord {
	storageTopic := entry.StorageTopic
	if storageTopic == "" {
		storageTopic = entry.Topic
	}
	return storage.DeliveryEventRecord{
		Event:         entry.State,
		Group:         entry.Group,
		Topic:         entry.Topic,
		StorageTopic:  storageTopic,
		QueueID:       entry.QueueID,
		MsgID:         entry.MsgID,
		Offset:        entry.Offset,
		NextOffset:    entry.NextOffset,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		ScheduledAt:   entry.ScheduledAt,
		ConsumedAt:    entry.ConsumedAt,
		EventAt:       entry.CancelledAt,
	}
}

func cancelledEntryFromDeliveryEvent(rec storage.DeliveryEventRecord) cancelledEntry {
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	state := rec.Event
	if state == "" {
		state = string(stateCancelled)
	}
	return cancelledEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
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
		CancelledAt:   rec.EventAt,
	}
}

func completedEntryFromDeliveryEvent(rec storage.DeliveryEventRecord) (completedEntry, bool) {
	if rec.Event != storage.DeliveryEventAck || rec.QueueID == nil || rec.Offset == nil || rec.NextOffset == nil {
		return completedEntry{}, false
	}
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	consumedAt := rec.EventAt
	if rec.ConsumedAt != nil && !rec.ConsumedAt.IsZero() {
		consumedAt = *rec.ConsumedAt
	}
	return completedEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
		QueueID:       *rec.QueueID,
		Offset:        *rec.Offset,
		NextOffset:    *rec.NextOffset,
		MsgID:         rec.MsgID,
		Body:          rec.Body,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		ConsumedAt:    consumedAt,
		AckedAt:       rec.EventAt,
	}, true
}

func processingEntryFromDeliveryEvent(rec storage.DeliveryEventRecord) (processingEntry, bool) {
	if rec.Event != storage.DeliveryEventProcessing || strings.TrimSpace(rec.Group) == "" || rec.QueueID == nil || rec.Offset == nil || rec.NextOffset == nil {
		return processingEntry{}, false
	}
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	consumedAt := rec.EventAt
	if rec.ConsumedAt != nil && !rec.ConsumedAt.IsZero() {
		consumedAt = *rec.ConsumedAt
	}
	return processingEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
		QueueID:       *rec.QueueID,
		Offset:        *rec.Offset,
		NextOffset:    *rec.NextOffset,
		MsgID:         rec.MsgID,
		Body:          rec.Body,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		ConsumedAt:    consumedAt,
		State:         stateProcessing,
		UpdatedAt:     rec.EventAt,
	}, true
}

func deliveryEventLatestSortRank(rec storage.DeliveryEventRecord) int {
	if isSettlingDeliveryEvent(rec.Event) {
		return 2
	}
	if rec.Event == storage.DeliveryEventProcessing {
		return 1
	}
	return 0
}

func sortLatestDeliveryEvents(records []storage.DeliveryEventRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].EventAt.Equal(records[j].EventAt) {
			return deliveryEventLatestSortRank(records[i]) > deliveryEventLatestSortRank(records[j])
		}
		return records[i].EventAt.After(records[j].EventAt)
	})
}

func latestDeliveryEventsByMsgID(records []storage.DeliveryEventRecord) map[string]storage.DeliveryEventRecord {
	sortLatestDeliveryEvents(records)
	latest := make(map[string]storage.DeliveryEventRecord, len(records))
	for _, rec := range records {
		if rec.MsgID == "" {
			continue
		}
		if _, ok := latest[rec.MsgID]; ok {
			continue
		}
		latest[rec.MsgID] = rec
	}
	return latest
}

func processingRecordFromDeliveryEvent(rec storage.DeliveryEventRecord) (storage.ProcessingRecord, bool) {
	entry, ok := processingEntryFromDeliveryEvent(rec)
	if !ok {
		return storage.ProcessingRecord{}, false
	}
	offset := entry.Offset
	nextOffset := entry.NextOffset
	return storage.ProcessingRecord{
		Group:         entry.Group,
		Topic:         entry.Topic,
		StorageTopic:  entry.StorageTopic,
		QueueID:       entry.QueueID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         entry.MsgID,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		UpdatedAt:     entry.ConsumedAt,
	}, true
}

func (b *Broker) latestGroupTopicDeliveryEvents(group, topic string) (map[string]storage.DeliveryEventRecord, bool, error) {
	if strings.TrimSpace(group) == "" || topic == "" {
		return nil, false, nil
	}
	ds, ok := b.store.(GroupDeliveryEventStore)
	if !ok {
		return nil, false, nil
	}
	allEventLimit := int(^uint(0) >> 1)
	records, err := ds.ListGroupDeliveryEvents(group, topic, "", allEventLimit)
	if err != nil {
		return nil, true, err
	}
	if ts, ok := b.store.(TopicDeliveryEventStore); ok {
		topicRecords, err := ts.ListTopicDeliveryEvents(topic, "", allEventLimit)
		if err != nil {
			return nil, true, err
		}
		records = append(records, topicRecords...)
	}
	return latestDeliveryEventsByMsgID(records), true, nil
}

func deliveryEventEntryFromRecord(rec storage.DeliveryEventRecord) (deliveryEventEntry, bool) {
	if strings.TrimSpace(rec.Group) == "" {
		return deliveryEventEntry{}, false
	}
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	return deliveryEventEntry{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
		Event:         rec.Event,
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
		EventAt:       rec.EventAt,
	}, true
}

func terminalEntryFromProcessingEntry(entry processingEntry, terminalAt time.Time) cancelledEntry {
	storageTopic := entry.StorageTopic
	if storageTopic == "" {
		storageTopic = entry.Topic
	}
	queueID := entry.QueueID
	offset := entry.Offset
	nextOffset := entry.NextOffset
	consumedAt := entry.ConsumedAt
	return cancelledEntry{
		Topic:         entry.Topic,
		StorageTopic:  storageTopic,
		QueueID:       &queueID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         entry.MsgID,
		Body:          entry.Body,
		Tag:           entry.Tag,
		CorrelationID: entry.CorrelationID,
		Retry:         entry.Retry,
		Timestamp:     entry.Timestamp,
		ConsumedAt:    &consumedAt,
		CancelledAt:   terminalAt,
	}
}

func terminalEntryFromProcessingRecord(rec storage.ProcessingRecord, terminalAt time.Time) cancelledEntry {
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	queueID := rec.QueueID
	consumedAt := rec.UpdatedAt
	return cancelledEntry{
		Topic:         rec.Topic,
		StorageTopic:  storageTopic,
		QueueID:       &queueID,
		Offset:        rec.Offset,
		NextOffset:    rec.NextOffset,
		MsgID:         rec.MsgID,
		Body:          rec.Body,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		ConsumedAt:    &consumedAt,
		CancelledAt:   terminalAt,
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

// OffsetListStore lists persisted offsets for a topic/queue across groups.
type OffsetListStore interface {
	ListOffsetsByTopicQueue(topic string, queueID int) (map[string]int64, error)
}

// ConsumedSegmentPruner removes sealed commitlog segments that are fully behind a safe offset.
type ConsumedSegmentPruner interface {
	PruneConsumedSegments(topic string, queueID int, cutoffOffset int64) (int, error)
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

// CancelledDeleteStore deletes persisted terminal records after sweep.
type CancelledDeleteStore interface {
	DeleteCancelled(topic, msgID string) error
}

// CancelledLookupStore provides point/topic lookups for persisted terminal records.
type CancelledLookupStore interface {
	LoadCancelledByID(topic, msgID string) (storage.CancelledRecord, bool, error)
	ListCancelledByTopic(topic, state string, limit int) ([]storage.CancelledRecord, error)
}

// AckLogStore persists completed delivery events for future replay/watermark work.
type AckLogStore interface {
	AppendAck(rec storage.AckRecord) error
}

// DeliveryEventStore persists generalized consumer lifecycle events.
type DeliveryEventStore interface {
	AppendDeliveryEvent(rec storage.DeliveryEventRecord) error
}

// TopicDeliveryEventStore lists topic-scoped delivery events for terminal history.
type TopicDeliveryEventStore interface {
	ListTopicDeliveryEvents(topic, event string, limit int) ([]storage.DeliveryEventRecord, error)
}

// GroupDeliveryEventStore lists group-scoped delivery events for replay/history queries.
type GroupDeliveryEventStore interface {
	ListGroupDeliveryEvents(group, topic, event string, limit int) ([]storage.DeliveryEventRecord, error)
}

// GroupDeliveryTopicStore enumerates group/topic delivery-event namespaces for replay recovery.
type GroupDeliveryTopicStore interface {
	ListGroupDeliveryTopics() (map[string][]string, error)
}

// QueueDeliveryEventStore loads raw delivery events for a specific queue file.
type QueueDeliveryEventStore interface {
	LoadDeliveryEvents(group, topic string, queueID *int) ([]storage.DeliveryEventRecord, error)
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

func subscriptionTagCovers(existingTag, targetTag string) bool {
	existing := normalizeSubscriptionTag(existingTag)
	target := normalizeSubscriptionTag(targetTag)
	if target == "*" {
		return existing == "*"
	}
	return existing == "*" || existing == target
}

func (b *Broker) topicProgressInitialOffset(group, topic string, queueID int) (int64, bool, error) {
	ols, ok := b.store.(OffsetListStore)
	if !ok {
		return 0, false, nil
	}
	offsets, err := ols.ListOffsetsByTopicQueue(topic, queueID)
	if err != nil {
		return 0, false, err
	}
	if len(offsets) == 0 {
		return 0, false, nil
	}

	b.lock.Lock()
	targetSub, ok := b.subscriptions[groupTopicKey(group, topic)]
	if !ok {
		b.lock.Unlock()
		return 0, false, nil
	}
	referenceTags := make(map[string]string, len(offsets))
	for refGroup := range offsets {
		if refGroup == group {
			continue
		}
		if refSub, ok := b.subscriptions[groupTopicKey(refGroup, topic)]; ok {
			referenceTags[refGroup] = refSub.Tag
		}
	}
	b.lock.Unlock()

	minOffset := int64(math.MaxInt64)
	found := false
	for refGroup, offset := range offsets {
		if refGroup == group {
			continue
		}
		refTag, ok := referenceTags[refGroup]
		if !ok || !subscriptionTagCovers(refTag, targetSub.Tag) {
			continue
		}
		if offset < minOffset {
			minOffset = offset
			found = true
		}
	}
	if !found {
		return 0, false, nil
	}
	return minOffset, true, nil
}

func (b *Broker) resolveInitialOffset(group, topic string, queueID int) (int64, error) {
	baseOffset := b.ConsumeQueueBaseOffset(topic, queueID)
	if isRetryTopicName(group, topic) || strings.EqualFold(b.newGroupStartPosition, "earliest") {
		return baseOffset, nil
	}
	if strings.EqualFold(b.newGroupStartPosition, "latest") {
		offset := b.ConsumeQueueDepth(topic, queueID)
		if err := b.CommitOffset(group, topic, queueID, offset); err != nil {
			return 0, err
		}
		return offset, nil
	}

	offset := baseOffset
	if inferredOffset, ok, err := b.topicProgressInitialOffset(group, topic, queueID); err != nil {
		return 0, err
	} else if ok {
		offset = inferredOffset
	}
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
			if err := b.recordExpired(cancelledEntry{
				Topic:         logicalTopic,
				StorageTopic:  storageTopic,
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
			}); err != nil {
				return nil, err
			}
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

func (b *Broker) ConsumeVisibleWithRetry(group, topic string, queueID int, tag string, maxMessages int) ([]storage.MessageWithOffset, string, error) {
	if retryTopic, ok := b.getRetryTopicIfExists(group, topic); ok {
		msgs, err := b.consumeVisibleFromTopic(group, topic, retryTopic, queueID, tag, maxMessages)
		if err == nil && len(msgs) > 0 {
			return msgs, retryTopic, nil
		}
	}
	msgs, err := b.consumeVisibleFromTopic(group, topic, topic, queueID, tag, maxMessages)
	return msgs, topic, err
}

// BeginProcessing records a message as processing for a group/queue/offset.
func (b *Broker) BeginProcessing(group, topic, storageTopic string, queueID int, offset, nextOffset int64, msg queue.Message) error {
	_ = nextOffset
	return b.BeginProcessingBatch(group, topic, storageTopic, queueID, offset, []queue.Message{msg})
}

// BeginProcessingBatch records multiple messages as processing in a single lock.
func (b *Broker) BeginProcessingBatch(group, topic, storageTopic string, queueID int, startOffset int64, msgs []queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if storageTopic == "" {
		storageTopic = topic
	}
	consumedAt := now()
	normalized := append([]queue.Message(nil), msgs...)
	entries := make([]processingEntry, 0, len(normalized))
	b.lock.Lock()
	gs := b.stats[group]
	if gs == nil {
		gs = &groupStats{}
		b.stats[group] = gs
	}
	for i, msg := range normalized {
		// apply retry count if tracked
		if rc := b.retryCounts[msg.ID]; rc > msg.Retry {
			msg.Retry = rc
			normalized[i] = msg
		}
		offset := startOffset + int64(i)
		nextOffset := offset + 1
		entry := processingEntry{
			Group:         group,
			Topic:         topic,
			StorageTopic:  storageTopic,
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
		entries = append(entries, entry)
		b.processing[processingKey(group, topic, msg.ID)] = entry
	}
	gs.Processing += int64(len(msgs))
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		for i, msg := range normalized {
			offset := startOffset + int64(i)
			nextOffset := offset + 1
			if err := ps.SaveProcessing(storage.ProcessingRecord{
				Group:         group,
				Topic:         topic,
				StorageTopic:  storageTopic,
				QueueID:       queueID,
				Offset:        &offset,
				NextOffset:    &nextOffset,
				MsgID:         msg.ID,
				Body:          msg.Body,
				Tag:           msg.Tag,
				CorrelationID: msg.CorrelationID,
				Retry:         msg.Retry,
				Timestamp:     msg.Timestamp,
				UpdatedAt:     consumedAt,
			}); err != nil {
				return fmt.Errorf("save processing record for group=%s topic=%s message=%s: %w", group, topic, msg.ID, err)
			}
		}
	}
	for _, entry := range entries {
		if err := b.appendDeliveryEvent(deliveryEventFromProcessingEntry(storage.DeliveryEventProcessing, entry, consumedAt)); err != nil {
			return fmt.Errorf("append processing delivery event for group=%s topic=%s message=%s: %w", group, topic, entry.MsgID, err)
		}
		if ps, ok := b.store.(ProcessingStore); ok {
			if err := ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, entry.MsgID); err != nil {
				logger.Warn("Failed to remove crash-window processing fallback after processing event",
					zap.String("group", entry.Group),
					zap.String("topic", entry.Topic),
					zap.String("message_id", entry.MsgID),
					zap.Error(err))
			}
		}
	}
	return nil
}

// RetryProcessing schedules a retry to the group retry topic or DLQ.
func (b *Broker) RetryProcessing(msgID, group, topic string) (bool, error) {
	if group == "" || topic == "" {
		return false, nil
	}
	key := processingKey(group, topic, msgID)
	eventAt := now()

	b.lock.Lock()
	entry, ok := b.processing[key]
	if !ok || entry.State != stateProcessing {
		b.lock.Unlock()
		return false, nil
	}
	retryCount := b.retryCounts[msgID] + 1
	original := entry
	entry.State = stateRetry
	entry.UpdatedAt = eventAt
	entry.Retry = retryCount
	b.processing[key] = entry
	b.lock.Unlock()

	revertRetryReservation := func() {
		b.lock.Lock()
		current, ok := b.processing[key]
		if ok && current.State == stateRetry {
			b.processing[key] = original
		}
		b.lock.Unlock()
	}
	removeCrashWindowFallback := func() {
		if ps, ok := b.store.(ProcessingStore); ok {
			if err := ps.RemoveProcessing(original.Group, original.Topic, original.QueueID, msgID); err != nil {
				logger.Warn("Failed to remove crash-window processing fallback after retry",
					zap.String("group", original.Group),
					zap.String("topic", original.Topic),
					zap.String("message_id", original.MsgID),
					zap.Error(err))
			}
		}
	}
	finalizeRetrySuccess := func(clearRetry bool) {
		b.lock.Lock()
		current, ok := b.processing[key]
		if ok && current.State == stateRetry {
			delete(b.processing, key)
			if gs := b.stats[original.Group]; gs != nil {
				gs.Processing--
				gs.Retry++
			}
		}
		if clearRetry {
			b.clearRetryCount(msgID)
		} else {
			b.retryCounts[msgID] = retryCount
		}
		b.lock.Unlock()
	}

	if retryCount > b.maxRetry {
		dlqTopic := original.Topic + ".dlq"
		if err := b.store.Append(dlqTopic, 0, storage.Message{
			ID:            original.MsgID,
			Body:          original.Body,
			Tag:           original.Tag,
			CorrelationID: original.CorrelationID,
			Retry:         retryCount,
			Timestamp:     original.Timestamp,
		}); err != nil {
			revertRetryReservation()
			logger.Error("Failed to append message to DLQ",
				zap.String("group", original.Group),
				zap.String("topic", original.Topic),
				zap.String("message_id", original.MsgID),
				zap.Error(err))
			return false, fmt.Errorf("append message to DLQ for group=%s topic=%s message=%s: %w", original.Group, original.Topic, original.MsgID, err)
		}
		if err := b.appendDeliveryEvent(deliveryEventFromProcessingEntry(storage.DeliveryEventDLQ, entry, eventAt)); err != nil {
			logger.Error("Failed to append DLQ delivery event",
				zap.String("group", original.Group),
				zap.String("topic", original.Topic),
				zap.String("message_id", original.MsgID),
				zap.Error(err))
		}
		removeCrashWindowFallback()
		finalizeRetrySuccess(true)
		return true, nil
	}

	group = original.Group
	topic = original.Topic
	queueID := original.QueueID
	msg := storage.Message{
		ID:            original.MsgID,
		Body:          original.Body,
		Tag:           original.Tag,
		CorrelationID: original.CorrelationID,
		Retry:         retryCount,
		Timestamp:     original.Timestamp,
	}
	retryTopic, err := b.ensureRetryTopic(group, topic)
	if err != nil {
		revertRetryReservation()
		logger.Error("Failed to ensure retry topic",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.Error(err))
		return false, fmt.Errorf("ensure retry topic for group=%s topic=%s: %w", group, topic, err)
	}

	// Schedule retry to group retry topic (RocketMQ-style), no offset rollback
	delay := b.calculateRetryBackoff(retryCount)
	if err := b.delayScheduler.ScheduleWithDelayStrict(retryTopic, queueID, msg, delay); err != nil {
		revertRetryReservation()
		logger.Error("Failed to schedule retry",
			zap.String("group", group),
			zap.String("topic", topic),
			zap.String("message_id", original.MsgID),
			zap.Error(err))
		return false, fmt.Errorf("schedule retry for group=%s topic=%s message=%s: %w", group, topic, original.MsgID, err)
	}
	retryEvent := deliveryEventFromProcessingEntry(storage.DeliveryEventRetry, entry, eventAt)
	scheduledAt := eventAt.Add(delay)
	retryEvent.ScheduledAt = &scheduledAt
	if err := b.appendDeliveryEvent(retryEvent); err != nil {
		if rollbackErr := b.delayScheduler.RemoveScheduled(retryTopic, queueID, msgID); rollbackErr != nil {
			logger.Error("Failed to rollback scheduled retry after event append failure",
				zap.String("group", original.Group),
				zap.String("topic", original.Topic),
				zap.String("message_id", original.MsgID),
				zap.Error(rollbackErr))
		}
		revertRetryReservation()
		return false, fmt.Errorf("append retry delivery event for group=%s topic=%s message=%s: %w", original.Group, original.Topic, original.MsgID, err)
	}
	removeCrashWindowFallback()
	finalizeRetrySuccess(false)
	return true, nil
}

// CompleteProcessing marks a message completed (offset already committed in ConsumeWithLock).
func (b *Broker) CompleteProcessing(msgID, group, topic string) (bool, error) {
	if group == "" || topic == "" {
		return false, nil
	}
	key := processingKey(group, topic, msgID)
	b.lock.Lock()
	entry, ok := b.processing[key]
	if !ok {
		// fmt.Printf("[DEBUG] CompleteProcessing: msgID %s not found in processing map (total: %d)\n", msgID, len(b.processing))
		b.lock.Unlock()
		return false, nil
	}
	if entry.State != stateProcessing {
		// fmt.Printf("[DEBUG] CompleteProcessing: msgID %s has wrong state: %s\n", msgID, entry.State)
		b.lock.Unlock()
		return false, nil
	}
	ackedAt := now()
	if err := b.appendAckEvent(entry, ackedAt); err != nil {
		b.lock.Unlock()
		return false, err
	}

	entry.State = stateCompleted
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
		StorageTopic:  entry.StorageTopic,
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
		if err := ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, msgID); err != nil {
			logger.Error("Failed to remove processing record after ack",
				zap.String("group", entry.Group),
				zap.String("topic", entry.Topic),
				zap.String("message_id", entry.MsgID),
				zap.Error(err))
		}
	}
	return true, nil
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
	byMsgID := make(map[string]processingEntry)
	b.lock.Lock()
	for _, entry := range b.processing {
		if entry.Group == group && entry.Topic == topic && entry.State == stateProcessing {
			byMsgID[entry.MsgID] = entry
		}
	}
	b.lock.Unlock()

	if latestEvents, ok, err := b.latestGroupTopicDeliveryEvents(group, topic); ok && err == nil {
		for msgID, rec := range latestEvents {
			if entry, ok := processingEntryFromDeliveryEvent(rec); ok {
				existing, exists := byMsgID[msgID]
				if !exists || existing.ConsumedAt.Before(entry.ConsumedAt) {
					byMsgID[msgID] = entry
				}
				continue
			}
			if isSettlingDeliveryEvent(rec.Event) {
				delete(byMsgID, msgID)
			}
		}
	}

	entries := make([]processingEntry, 0, len(byMsgID))
	for _, entry := range byMsgID {
		entries = append(entries, entry)
	}
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
	memoryEntries := append([]completedEntry(nil), b.completed[groupTopicKey(group, topic)]...)
	b.lock.Unlock()

	byMsgID := make(map[string]completedEntry, len(memoryEntries))
	baseByQueue := make(map[string]int64)
	for _, entry := range memoryEntries {
		storageTopic := entry.StorageTopic
		if storageTopic == "" {
			storageTopic = entry.Topic
		}
		baseKey := fmt.Sprintf("%s:%d", storageTopic, entry.QueueID)
		base, ok := baseByQueue[baseKey]
		if !ok {
			base = b.ConsumeQueueBaseOffset(storageTopic, entry.QueueID)
			baseByQueue[baseKey] = base
		}
		if entry.NextOffset <= base {
			continue
		}
		byMsgID[entry.MsgID] = entry
	}
	if latestEvents, ok, err := b.latestGroupTopicDeliveryEvents(group, topic); ok && err == nil {
		for msgID, rec := range latestEvents {
			if rec.Event != storage.DeliveryEventAck {
				delete(byMsgID, msgID)
				continue
			}
			entry, ok := completedEntryFromDeliveryEvent(rec)
			if !ok {
				continue
			}
			existing, exists := byMsgID[entry.MsgID]
			if !exists || existing.AckedAt.Before(entry.AckedAt) {
				byMsgID[entry.MsgID] = entry
			}
		}
	} else if ds, ok := b.store.(GroupDeliveryEventStore); ok {
		fetchLimit := limit + len(memoryEntries)
		if fetchLimit < limit*2 {
			fetchLimit = limit * 2
		}
		records, err := ds.ListGroupDeliveryEvents(group, topic, storage.DeliveryEventAck, fetchLimit)
		if err == nil {
			for _, rec := range records {
				entry, ok := completedEntryFromDeliveryEvent(rec)
				if !ok {
					continue
				}
				existing, exists := byMsgID[entry.MsgID]
				if !exists || existing.AckedAt.Before(entry.AckedAt) {
					byMsgID[entry.MsgID] = entry
				}
			}
		}
	}
	entries := make([]completedEntry, 0, len(byMsgID))
	for _, entry := range byMsgID {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].AckedAt.After(entries[j].AckedAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries
}

// ListDeliveryEvents returns messages whose latest delivery event matches the requested state.
func (b *Broker) ListDeliveryEvents(group, topic, event string, limit int) []deliveryEventEntry {
	if strings.TrimSpace(group) == "" || topic == "" || event == "" {
		return nil
	}
	if limit <= 0 {
		limit = 50
	}
	if latestEvents, ok, err := b.latestGroupTopicDeliveryEvents(group, topic); ok && err == nil {
		entries := make([]deliveryEventEntry, 0, len(latestEvents))
		for _, rec := range latestEvents {
			if rec.Event != event {
				continue
			}
			entry, ok := deliveryEventEntryFromRecord(rec)
			if !ok {
				continue
			}
			entries = append(entries, entry)
		}
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].EventAt.After(entries[j].EventAt)
		})
		if len(entries) > limit {
			entries = entries[:limit]
		}
		return entries
	}
	ds, ok := b.store.(GroupDeliveryEventStore)
	if !ok {
		return nil
	}
	fetchLimit := limit
	if fetchLimit < 50 {
		fetchLimit = 50
	}
	records, err := ds.ListGroupDeliveryEvents(group, topic, event, fetchLimit)
	if err != nil {
		return nil
	}
	entries := make([]deliveryEventEntry, 0, len(records))
	for _, rec := range records {
		entry, ok := deliveryEventEntryFromRecord(rec)
		if !ok {
			continue
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].EventAt.After(entries[j].EventAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
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

func (b *Broker) removeCancelledFromCache(topic, msgID string) {
	key := cancelledKey(topic, msgID)
	b.lock.Lock()
	delete(b.cancelled, key)
	for i, existing := range b.cancelledOrder {
		if existing != key {
			continue
		}
		b.cancelledOrder = append(b.cancelledOrder[:i], b.cancelledOrder[i+1:]...)
		break
	}
	b.lock.Unlock()
}

func deliveryEventStorageTopic(rec storage.DeliveryEventRecord) string {
	if rec.StorageTopic == "" {
		return rec.Topic
	}
	return rec.StorageTopic
}

func deliveryEventOffsetRange(rec storage.DeliveryEventRecord) (int64, int64, bool) {
	if rec.Offset != nil {
		if rec.NextOffset != nil {
			return *rec.Offset, *rec.NextOffset, true
		}
		return *rec.Offset, *rec.Offset + 1, true
	}
	if rec.NextOffset != nil {
		return *rec.NextOffset - 1, *rec.NextOffset, true
	}
	return 0, 0, false
}

func isSettlingDeliveryEvent(event string) bool {
	switch event {
	case storage.DeliveryEventAck, storage.DeliveryEventRetry, storage.DeliveryEventDLQ, storage.DeliveryEventCancelled, storage.DeliveryEventExpired:
		return true
	default:
		return false
	}
}

func terminalNextOffset(rec storage.CancelledRecord) (int64, bool) {
	if rec.NextOffset != nil {
		return *rec.NextOffset, true
	}
	if rec.Offset == nil {
		return 0, false
	}
	return *rec.Offset + 1, true
}

func (b *Broker) canSweepCancelledRecord(rec storage.CancelledRecord) (bool, error) {
	if rec.QueueID == nil {
		return false, nil
	}
	nextOffset, ok := terminalNextOffset(rec)
	if !ok {
		return false, nil
	}
	storageTopic := rec.StorageTopic
	if storageTopic == "" {
		storageTopic = rec.Topic
	}
	queueID := *rec.QueueID
	if b.ConsumeQueueBaseOffset(storageTopic, queueID) >= nextOffset {
		return true, nil
	}
	if _, ownerGroup, isRetry := splitRetryTopicName(storageTopic); isRetry {
		offset, found, err := b.GetOffset(ownerGroup, storageTopic, queueID)
		if err != nil {
			return false, err
		}
		if !found || offset < nextOffset {
			return false, nil
		}
		if settled, checked, err := b.deliveryEventSettledBefore(ownerGroup, rec.Topic, storageTopic, queueID, nextOffset, offset); err != nil {
			return false, err
		} else if checked && !settled {
			return false, nil
		}
		return true, nil
	}
	if strings.EqualFold(b.newGroupStartPosition, "earliest") {
		return false, nil
	}
	ols, ok := b.store.(OffsetListStore)
	if !ok {
		return false, nil
	}
	offsets, err := ols.ListOffsetsByTopicQueue(storageTopic, queueID)
	if err != nil {
		return false, err
	}
	for group, offset := range offsets {
		if offset < nextOffset {
			return false, nil
		}
		if settled, checked, err := b.deliveryEventSettledBefore(group, rec.Topic, storageTopic, queueID, nextOffset, offset); err != nil {
			return false, err
		} else if checked && !settled {
			return false, nil
		}
	}
	return true, nil
}

func (b *Broker) deliveryEventSettledCutoff(group, logicalTopic, storageTopic string, queueID int, upperBound int64) (int64, bool, error) {
	if strings.TrimSpace(group) == "" || logicalTopic == "" || storageTopic == "" {
		return 0, false, nil
	}
	ds, ok := b.store.(QueueDeliveryEventStore)
	if !ok {
		return 0, false, nil
	}
	base := b.ConsumeQueueBaseOffset(storageTopic, queueID)
	if upperBound <= base {
		return upperBound, true, nil
	}
	qid := queueID
	groupRecords, err := ds.LoadDeliveryEvents(group, logicalTopic, &qid)
	if err != nil {
		return 0, false, err
	}
	topicRecords, err := ds.LoadDeliveryEvents("", logicalTopic, &qid)
	if err != nil {
		return 0, false, err
	}

	settled := make(map[int64]int64)
	addRecords := func(records []storage.DeliveryEventRecord) {
		for _, rec := range records {
			if !isSettlingDeliveryEvent(rec.Event) {
				continue
			}
			if deliveryEventStorageTopic(rec) != storageTopic {
				continue
			}
			if rec.QueueID == nil || *rec.QueueID != queueID {
				continue
			}
			offset, nextOffset, ok := deliveryEventOffsetRange(rec)
			if !ok || nextOffset <= offset || offset < base || offset >= upperBound {
				continue
			}
			if existing, ok := settled[offset]; !ok || existing < nextOffset {
				settled[offset] = nextOffset
			}
		}
	}
	addRecords(groupRecords)
	addRecords(topicRecords)

	cutoff := base
	for cutoff < upperBound {
		nextOffset, ok := settled[cutoff]
		if !ok || nextOffset <= cutoff {
			break
		}
		cutoff = nextOffset
	}
	return cutoff, true, nil
}

func (b *Broker) deliveryEventSettledBefore(group, logicalTopic, storageTopic string, queueID int, nextOffset, upperBound int64) (bool, bool, error) {
	if nextOffset <= 0 || upperBound < nextOffset {
		return false, false, nil
	}
	cutoff, ok, err := b.deliveryEventSettledCutoff(group, logicalTopic, storageTopic, queueID, upperBound)
	if err != nil {
		return false, false, err
	}
	if !ok {
		return false, false, nil
	}
	return cutoff >= nextOffset, true, nil
}

// SweepTerminalTombstones removes persisted terminal tombstones that are safely behind queue watermarks.
func (b *Broker) SweepTerminalTombstones() (int, error) {
	cs, ok := b.store.(CancelledStore)
	if !ok {
		return 0, nil
	}
	ds, ok := b.store.(CancelledDeleteStore)
	if !ok {
		return 0, nil
	}
	records, err := cs.LoadCancelled()
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, rec := range records {
		safe, err := b.canSweepCancelledRecord(rec)
		if err != nil {
			return deleted, err
		}
		if !safe {
			continue
		}
		if err := ds.DeleteCancelled(rec.Topic, rec.MsgID); err != nil {
			return deleted, err
		}
		b.removeCancelledFromCache(rec.Topic, rec.MsgID)
		deleted++
	}
	return deleted, nil
}

func (b *Broker) consumedPruneCutoff(topic string, queueID int) (int64, bool, error) {
	if topic == SystemDelayTopic {
		return 0, false, nil
	}
	logicalTopic := topic
	if originalTopic, ownerGroup, isRetry := splitRetryTopicName(topic); isRetry {
		logicalTopic = originalTopic
		offset, ok, err := b.GetOffset(ownerGroup, topic, queueID)
		if err != nil {
			return 0, false, err
		}
		if !ok {
			return 0, false, nil
		}
		if cutoff, cutoffOK, err := b.deliveryEventSettledCutoff(ownerGroup, logicalTopic, topic, queueID, offset); err != nil {
			return 0, false, err
		} else if cutoffOK && cutoff < offset {
			offset = cutoff
		}
		return offset, true, nil
	}
	if strings.EqualFold(b.newGroupStartPosition, "earliest") {
		return 0, false, nil
	}
	ols, ok := b.store.(OffsetListStore)
	if !ok {
		return 0, false, nil
	}
	offsets, err := ols.ListOffsetsByTopicQueue(topic, queueID)
	if err != nil {
		return 0, false, err
	}
	if len(offsets) == 0 {
		return 0, false, nil
	}
	minOffset := int64(^uint64(0) >> 1)
	for group, offset := range offsets {
		if cutoff, cutoffOK, err := b.deliveryEventSettledCutoff(group, logicalTopic, topic, queueID, offset); err != nil {
			return 0, false, err
		} else if cutoffOK && cutoff < offset {
			offset = cutoff
		}
		if offset < minOffset {
			minOffset = offset
		}
	}
	if minOffset == int64(^uint64(0)>>1) {
		return 0, false, nil
	}
	return minOffset, true, nil
}

// SweepConsumedSegments prunes sealed commitlog segments that are fully behind safe consumer offsets.
func (b *Broker) SweepConsumedSegments() (int, error) {
	ps, ok := b.store.(ConsumedSegmentPruner)
	if !ok {
		return 0, nil
	}
	topics := b.topicManager.ListTopics()
	totalDeleted := 0
	for _, cfg := range topics {
		if cfg == nil || cfg.Name == SystemDelayTopic {
			continue
		}
		queueCount := cfg.QueueCount
		if queueCount <= 0 {
			queueCount = b.queueCount
		}
		for queueID := 0; queueID < queueCount; queueID++ {
			cutoff, ok, err := b.consumedPruneCutoff(cfg.Name, queueID)
			if err != nil {
				return totalDeleted, err
			}
			if !ok {
				continue
			}
			deleted, err := ps.PruneConsumedSegments(cfg.Name, queueID, cutoff)
			if err != nil {
				return totalDeleted, err
			}
			totalDeleted += deleted
		}
	}
	return totalDeleted, nil
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

func (b *Broker) persistCancelled(entry cancelledEntry) error {
	cs, ok := b.store.(CancelledStore)
	if !ok {
		return nil
	}
	return cs.SaveCancelled(storage.CancelledRecord{
		Group:         entry.Group,
		Topic:         entry.Topic,
		StorageTopic:  entry.StorageTopic,
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

func (b *Broker) appendDeliveryEvent(rec storage.DeliveryEventRecord) error {
	if ds, ok := b.store.(DeliveryEventStore); ok {
		return ds.AppendDeliveryEvent(rec)
	}
	return nil
}

func (b *Broker) appendAckEvent(entry processingEntry, ackedAt time.Time) error {
	if ds, ok := b.store.(DeliveryEventStore); ok {
		return ds.AppendDeliveryEvent(deliveryEventFromProcessingEntry(storage.DeliveryEventAck, entry, ackedAt))
	}
	storageTopic := entry.StorageTopic
	if storageTopic == "" {
		storageTopic = entry.Topic
	}
	if as, ok := b.store.(AckLogStore); ok {
		return as.AppendAck(storage.AckRecord{
			Group:         entry.Group,
			Topic:         entry.Topic,
			StorageTopic:  storageTopic,
			QueueID:       entry.QueueID,
			MsgID:         entry.MsgID,
			Offset:        entry.Offset,
			NextOffset:    entry.NextOffset,
			Tag:           entry.Tag,
			CorrelationID: entry.CorrelationID,
			Retry:         entry.Retry,
			Timestamp:     entry.Timestamp,
			AckedAt:       ackedAt,
		})
	}
	return nil
}

func (b *Broker) recordCancelled(entry cancelledEntry) error {
	if entry.CancelledAt.IsZero() {
		entry.CancelledAt = now()
	}
	entry.Group = ""
	if entry.StorageTopic == "" {
		entry.StorageTopic = entry.Topic
	}
	if entry.State == "" {
		entry.State = string(stateCancelled)
	}
	if err := b.persistCancelled(entry); err != nil {
		return fmt.Errorf("save terminal record for topic=%s message=%s: %w", entry.Topic, entry.MsgID, err)
	}
	if err := b.appendDeliveryEvent(deliveryEventFromCancelledEntry(entry)); err != nil {
		if ds, ok := b.store.(CancelledDeleteStore); ok {
			if deleteErr := ds.DeleteCancelled(entry.Topic, entry.MsgID); deleteErr != nil {
				logger.Error("Failed to rollback terminal record after delivery event error",
					zap.String("event", entry.State),
					zap.String("topic", entry.Topic),
					zap.String("message_id", entry.MsgID),
					zap.Error(deleteErr))
			}
		}
		return fmt.Errorf("append terminal delivery event for state=%s topic=%s message=%s: %w", entry.State, entry.Topic, entry.MsgID, err)
	}
	b.cacheCancelled(entry)
	b.lock.Lock()
	b.clearRetryCount(entry.MsgID)
	b.lock.Unlock()
	return nil
}

func (b *Broker) recordExpired(entry cancelledEntry) error {
	entry.State = string(stateExpired)
	return b.recordCancelled(entry)
}

func (b *Broker) expireProcessing(msgID, group, topic string) (bool, error) {
	if group == "" || topic == "" {
		return false, nil
	}
	key := processingKey(group, topic, msgID)
	expiredAt := now()

	b.lock.Lock()
	entry, ok := b.processing[key]
	if !ok || entry.State != stateProcessing {
		b.lock.Unlock()
		return false, nil
	}
	original := entry
	entry.State = stateExpired
	entry.UpdatedAt = expiredAt
	b.processing[key] = entry
	b.lock.Unlock()

	revertReservation := func() {
		b.lock.Lock()
		current, ok := b.processing[key]
		if ok && current.State == stateExpired {
			b.processing[key] = original
		}
		b.lock.Unlock()
	}
	if err := b.recordExpired(terminalEntryFromProcessingEntry(original, expiredAt)); err != nil {
		revertReservation()
		return false, err
	}

	b.lock.Lock()
	current, ok := b.processing[key]
	if ok && current.State == stateExpired {
		delete(b.processing, key)
		if gs := b.stats[original.Group]; gs != nil {
			gs.Processing--
		}
	}
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		if err := ps.RemoveProcessing(original.Group, original.Topic, original.QueueID, msgID); err != nil {
			logger.Warn("Failed to remove crash-window processing fallback after expiry",
				zap.String("group", original.Group),
				zap.String("topic", original.Topic),
				zap.String("message_id", original.MsgID),
				zap.Error(err))
		}
	}
	return true, nil
}

func (b *Broker) cancelProcessing(msgID, topic string) (bool, error) {
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
		return false, nil
	}
	cancelled := terminalEntryFromProcessingEntry(entries[0], cancelledAt)
	cancelled.State = string(stateCancelled)
	for i, key := range keys {
		entry := entries[i]
		entry.State = stateCancelled
		entry.UpdatedAt = cancelledAt
		b.processing[key] = entry
	}
	b.lock.Unlock()

	revertReservation := func() {
		b.lock.Lock()
		for i, key := range keys {
			current, ok := b.processing[key]
			if ok && current.State == stateCancelled {
				b.processing[key] = entries[i]
			}
		}
		b.lock.Unlock()
	}
	if err := b.recordCancelled(cancelled); err != nil {
		revertReservation()
		return false, err
	}

	b.lock.Lock()
	for i, key := range keys {
		entry := entries[i]
		current, ok := b.processing[key]
		if ok && current.State == stateCancelled {
			delete(b.processing, key)
			if gs := b.stats[entry.Group]; gs != nil {
				gs.Processing--
			}
		}
	}
	b.lock.Unlock()

	if ps, ok := b.store.(ProcessingStore); ok {
		for _, entry := range entries {
			if err := ps.RemoveProcessing(entry.Group, entry.Topic, entry.QueueID, msgID); err != nil {
				logger.Warn("Failed to remove crash-window processing fallback after cancel",
					zap.String("group", entry.Group),
					zap.String("topic", entry.Topic),
					zap.String("message_id", entry.MsgID),
					zap.Error(err))
			}
		}
	}
	return true, nil
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
			StorageTopic:  topic,
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
					StorageTopic:  topic,
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
	entries := make([]cancelledEntry, 0, limit)
	seen := make(map[string]struct{})
	if cs, ok := b.store.(CancelledLookupStore); ok {
		records, err := cs.ListCancelledByTopic(topic, state, limit)
		if err == nil {
			for _, rec := range records {
				entry := cancelledEntryFromRecord(rec)
				b.cacheCancelled(entry)
				seen[entry.MsgID] = struct{}{}
				entries = append(entries, entry)
			}
		}
	}
	if ds, ok := b.store.(TopicDeliveryEventStore); ok {
		records, err := ds.ListTopicDeliveryEvents(topic, state, limit*2)
		if err == nil {
			for _, rec := range records {
				entry := cancelledEntryFromDeliveryEvent(rec)
				if _, ok := seen[entry.MsgID]; ok {
					continue
				}
				seen[entry.MsgID] = struct{}{}
				entries = append(entries, entry)
			}
		}
	}
	if len(entries) == 0 {
		b.lock.Lock()
		for _, entry := range b.cancelled {
			if entry.Topic == topic && entry.State == state {
				if _, ok := seen[entry.MsgID]; ok {
					continue
				}
				seen[entry.MsgID] = struct{}{}
				entries = append(entries, entry)
			}
		}
		b.lock.Unlock()
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
	return b.recordExpired(cancelledEntry{
		Topic:         logicalTopic,
		StorageTopic:  storageTopic,
		QueueID:       &queueIDCopy,
		MsgID:         msg.ID,
		Body:          msg.Body,
		Tag:           msg.Tag,
		CorrelationID: msg.CorrelationID,
		Retry:         msg.Retry,
		Timestamp:     msg.Timestamp,
		CancelledAt:   now(),
	})
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
				if err := b.recordExpired(cancelledEntry{
					Topic:         topic,
					StorageTopic:  topic,
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
				}); err != nil {
					return nil, 0, err
				}
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
func (b *Broker) ListScheduledVisible(topic string, queueID *int, cursor int64, limit int) ([]DelayedMessage, *int64, error) {
	ds := b.GetDelayScheduler()
	if ds == nil {
		return nil, nil, nil
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
			return out, next, nil
		}
		for _, item := range items {
			if b.IsTerminal(topic, item.Message.ID) {
				continue
			}
			if b.shouldExpire(item.Message.Timestamp) {
				qid := item.QueueID
				scheduledAt := item.ExecuteAt
				if err := b.recordExpired(cancelledEntry{
					Topic:         topic,
					StorageTopic:  topic,
					QueueID:       &qid,
					MsgID:         item.Message.ID,
					Body:          item.Message.Body,
					Tag:           item.Message.Tag,
					CorrelationID: item.Message.CorrelationID,
					Retry:         item.Message.Retry,
					Timestamp:     item.Message.Timestamp,
					ScheduledAt:   &scheduledAt,
					CancelledAt:   now(),
				}); err != nil {
					return nil, nil, err
				}
				continue
			}
			out = append(out, item)
			if len(out) == limit {
				break
			}
		}
		if next == nil {
			return out, nil, nil
		}
		scanCursor = *next
	}

	n := scanCursor
	return out, &n, nil
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
func (b *Broker) TerminateMessage(msgID, topic string) (bool, error) {
	if topic == "" || msgID == "" {
		return false, nil
	}
	if b.IsTerminal(topic, msgID) {
		return true, nil
	}
	cancelled, err := b.cancelProcessing(msgID, topic)
	if err != nil {
		return false, err
	}
	if cancelled {
		return true, nil
	}
	if entry, ok := b.findScheduledMessage(topic, msgID); ok {
		if err := b.recordCancelled(entry); err != nil {
			return false, err
		}
		return true, nil
	}
	if entry, ok := b.findPendingMessage(topic, msgID); ok {
		if err := b.recordCancelled(entry); err != nil {
			return false, err
		}
		return true, nil
	}
	return true, nil
}

// TerminateMessages marks multiple messages cancelled for a topic.
// Unknown or already-terminated message IDs are treated as successful.
func (b *Broker) TerminateMessages(topic string, msgIDs []string) (int, error) {
	terminated := 0
	for _, msgID := range msgIDs {
		ok, err := b.TerminateMessage(msgID, topic)
		if err != nil {
			return terminated, err
		}
		if ok {
			terminated++
		}
	}
	return terminated, nil
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

func (b *Broker) sweepProcessingTimeoutsAt(ts time.Time) (expiredCount, retriedCount int) {
	b.lock.Lock()
	var timeoutEntries []processingEntry
	for _, entry := range b.processing {
		if entry.State == stateProcessing && ts.Sub(entry.UpdatedAt) > b.processingTimeout {
			timeoutEntries = append(timeoutEntries, entry)
			logger.Warn("Message processing timeout",
				zap.String("message_id", entry.MsgID),
				zap.String("group", entry.Group),
				zap.String("topic", entry.Topic),
				zap.Duration("elapsed", ts.Sub(entry.UpdatedAt)))
		}
	}
	b.lock.Unlock()

	for _, entry := range timeoutEntries {
		if b.shouldExpire(entry.Timestamp) {
			if ok, err := b.expireProcessing(entry.MsgID, entry.Group, entry.Topic); err != nil {
				logger.Error("Failed to expire timed-out processing entry",
					zap.String("message_id", entry.MsgID),
					zap.String("group", entry.Group),
					zap.String("topic", entry.Topic),
					zap.Error(err))
			} else if ok {
				expiredCount++
			}
			continue
		}
		if ok, err := b.RetryProcessing(entry.MsgID, entry.Group, entry.Topic); err != nil {
			logger.Error("Failed to retry timed-out processing entry",
				zap.String("message_id", entry.MsgID),
				zap.String("group", entry.Group),
				zap.String("topic", entry.Topic),
				zap.Error(err))
		} else if ok {
			retriedCount++
		}
	}
	return expiredCount, retriedCount
}

// checkProcessingTimeouts 定期检查超时的处理中消息并自动重试/过期。
func (b *Broker) checkProcessingTimeouts() {
	ticker := time.NewTicker(5 * time.Second) // 每 5 秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			return
		case <-ticker.C:
			b.sweepProcessingTimeoutsAt(now())
		}
	}
}

func (b *Broker) sweepTerminalTombstonesLoop() {
	ticker := time.NewTicker(defaultTombstoneSweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			return
		case <-ticker.C:
			deleted, err := b.SweepTerminalTombstones()
			if err != nil {
				logger.Warn("Failed to sweep terminal tombstones", zap.Error(err))
				continue
			}
			if deleted > 0 {
				logger.Debug("Swept terminal tombstones", zap.Int("deleted", deleted))
			}
		}
	}
}

func (b *Broker) pruneConsumedSegmentsLoop() {
	ticker := time.NewTicker(defaultConsumedPruneInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			return
		case <-ticker.C:
			deleted, err := b.SweepConsumedSegments()
			if err != nil {
				logger.Warn("Failed to prune consumed segments", zap.Error(err))
				continue
			}
			if deleted > 0 {
				logger.Debug("Pruned consumed segments", zap.Int("deleted", deleted))
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
	case "latest":
		b.newGroupStartPosition = "latest"
	case "topic-progress", "topic_progress", "progress":
		b.newGroupStartPosition = "topic_progress"
	default:
		b.newGroupStartPosition = defaultNewGroupStartPosition
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
