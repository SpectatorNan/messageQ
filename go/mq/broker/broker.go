package broker

import (
	"errors"
	"sync"

	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

const defaultQueueCount = 4

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
	b.lock.Lock()
	qs := b.getQueues(topic)
	start := b.rrDeq[topic] % len(qs)
	b.lock.Unlock()

	// try each queue once without blocking
	for i := 0; i < len(qs); i++ {
		idx := (start + i) % len(qs)
		if msg, ok := qs[idx].TryDequeue(); ok {
			b.lock.Lock()
			b.rrDeq[topic] = (idx + 1) % len(qs)
			b.inflight[msg.ID] = idx
			b.lock.Unlock()
			return msg
		}
	}

	// if none available, block on the round-robin queue
	idx := start
	msg := qs[idx].Dequeue()
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
