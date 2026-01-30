package broker

import (
	"messageQ/mq/queue"
	"messageQ/mq/storage"
	"sync"
)

type Broker struct {
	queues map[string]*queue.Queue
	lock   sync.Mutex
	store  storage.Storage
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*queue.Queue),
	}
}

func NewBrokerWithStorage(store storage.Storage) *Broker {
	return &Broker{
		queues: make(map[string]*queue.Queue),
		store:  store,
	}
}

func (b *Broker) GetQueue(topic string) *queue.Queue {
	b.lock.Lock()
	defer b.lock.Unlock()
	if q, exists := b.queues[topic]; exists {
		return q
	}
	var q *queue.Queue
	if b.store != nil {
		q = queue.NewQueueWithStorage(b.store, topic)
	} else {
		q = queue.NewQueue()
	}
	b.queues[topic] = q
	return q
}
