package broker

import (
	"errors"
	"sync"
	"time"

	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

const defaultQueueCount = 4

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
	queues     map[string][]*queue.Queue
	lock       sync.Mutex
	store      storage.Storage
	queueCount int

	// round-robin pointers per topic
	rrEnq map[string]int
	rrDeq map[string]int

	// inflight routing: msgID -> queueID
	inflight map[string]int

	processing map[string]processingEntry // msgID -> entry
	stats      map[string]*groupStats     // group -> stats
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
	return &Broker{
		queues:     make(map[string][]*queue.Queue),
		store:      store,
		queueCount: queueCount,
		rrEnq:      make(map[string]int),
		rrDeq:      make(map[string]int),
		inflight:   make(map[string]int),
		processing: make(map[string]processingEntry),
		stats:      make(map[string]*groupStats),
	}
}

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

// BeginProcessing records a message as processing for a group/queue/offset.
func (b *Broker) BeginProcessing(group, topic string, queueID int, offset, nextOffset int64, msg queue.Message) {
	b.lock.Lock()
	defer b.lock.Unlock()
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
	gs := b.stats[group]
	if gs == nil {
		gs = &groupStats{}
		b.stats[group] = gs
	}
	gs.Processing++
}

// RetryProcessing marks a message retry without committing offset.
func (b *Broker) RetryProcessing(msgID string) bool {
	b.lock.Lock()
	entry, ok := b.processing[msgID]
	if !ok || entry.State != stateProcessing {
		b.lock.Unlock()
		return false
	}
	entry.State = stateRetry
	entry.UpdatedAt = time.Now()
	b.processing[msgID] = entry
	gs := b.stats[entry.Group]
	if gs != nil {
		gs.Processing--
		gs.Retry++
	}
	delete(b.processing, msgID)
	b.lock.Unlock()
	// no offset commit on retry; message will be re-consumed
	return true
}

// CompleteProcessing marks a message completed and commits offset.
func (b *Broker) CompleteProcessing(msgID string) bool {
	b.lock.Lock()
	entry, ok := b.processing[msgID]
	if ok && entry.State == stateProcessing {
		entry.State = stateCompleted
		entry.UpdatedAt = time.Now()
		b.processing[msgID] = entry
		gs := b.stats[entry.Group]
		if gs != nil {
			gs.Processing--
			gs.Completed++
		}
		delete(b.processing, msgID)
	}
	b.lock.Unlock()
	if !ok {
		return false
	}
	cur, _, err := b.GetOffset(entry.Group, entry.Topic, entry.QueueID)
	if err == nil && entry.NextOffset > cur {
		_ = b.CommitOffset(entry.Group, entry.Topic, entry.QueueID, entry.NextOffset)
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
