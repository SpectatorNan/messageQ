package broker

import (
	"messageQ/mq/queue"
	"sync"
)

type Broker struct {
	queues map[string]*queue.Queue
	lock   sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		queues: make(map[string]*queue.Queue),
	}
}

func (b *Broker) GetQueue(topic string) *queue.Queue {
	b.lock.Lock()
	defer b.lock.Unlock()
	if q, exists := b.queues[topic]; exists {
		return q
	}
	q := queue.NewQueue()
	b.queues[topic] = q
	return q
}
