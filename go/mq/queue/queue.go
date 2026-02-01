package queue

import (
	"log"
	"sync"
	"time"

	"messageQ/mq/storage"
)

type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	data     []Message
	inflight map[int64]InflightMsg
	nextID   int64

	ackTimeout time.Duration
	maxRetry   int

	store storage.Storage
	// topic name used when persisting
	topic string
}

func NewQueue() *Queue {
	q := &Queue{
		data:       make([]Message, 0),
		inflight:   make(map[int64]InflightMsg),
		ackTimeout: 10 * time.Second,
		maxRetry:   3,
	}
	q.cond = sync.NewCond(&q.mu)
	go q.reclaimLoop()
	return q
}

func NewQueueWithStorage(store storage.Storage, topic string) *Queue {
	q := &Queue{
		data:       make([]Message, 0),
		inflight:   make(map[int64]InflightMsg),
		ackTimeout: 10 * time.Second,
		maxRetry:   3,
		store:      store,
		topic:      topic,
	}
	q.cond = sync.NewCond(&q.mu)
	// load persisted messages into data
	if store != nil {
		msgs, err := store.Load(topic)
		if err != nil {
			log.Println("storage load error:", err)
		} else {
			// convert storage.Message to queue.Message
			for _, sm := range msgs {
				qm := Message{
					ID:        sm.ID,
					Body:      sm.Body,
					Retry:     sm.Retry,
					Timestamp: sm.Timestamp,
				}
				q.data = append(q.data, qm)
				if sm.ID > q.nextID {
					q.nextID = sm.ID
				}
			}
		}
	}
	go q.reclaimLoop()
	return q
}

func (q *Queue) Enqueue(body string) Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.nextID++
	msg := Message{
		ID:        q.nextID,
		Body:      body,
		Timestamp: time.Now(),
	}

	q.data = append(q.data, msg)
	if q.store != nil {
		// convert to storage.Message
		sm := storage.Message{
			ID:        msg.ID,
			Body:      msg.Body,
			Retry:     msg.Retry,
			Timestamp: msg.Timestamp,
		}
		_ = q.store.Append(q.topic, sm)
	}
	q.cond.Signal()
	return msg
}

func (q *Queue) Dequeue() Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.data) == 0 {
		q.cond.Wait()
	}

	msg := q.data[0]
	q.data = q.data[1:]

	q.inflight[msg.ID] = InflightMsg{
		Msg:      msg,
		Deadline: time.Now().Add(q.ackTimeout),
	}

	return msg
}

func (q *Queue) Ack(id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.inflight[id]; ok {
		delete(q.inflight, id)
		if q.store != nil {
			_ = q.store.Ack(q.topic, id)
		}
		return true
	}
	return false
}

func (q *Queue) Nack(id int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	im, ok := q.inflight[id]
	if !ok {
		return false
	}

	delete(q.inflight, id)

	im.Msg.Retry++
	if im.Msg.Retry > q.maxRetry {
		q.persistDLQ(im.Msg)
		log.Println("DLQ:", im.Msg.ID)
		return true
	}

	q.persistRetry(im.Msg)
	q.data = append(q.data, im.Msg)
	q.cond.Signal()
	return true
}

func (q *Queue) reclaimLoop() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		now := time.Now()
		q.mu.Lock()
		for id, im := range q.inflight {
			if now.After(im.Deadline) {
				delete(q.inflight, id)
				im.Msg.Retry++
				if im.Msg.Retry <= q.maxRetry {
					q.persistRetry(im.Msg)
					q.data = append(q.data, im.Msg)
					q.cond.Signal()
				} else {
					q.persistDLQ(im.Msg)
					log.Println("DLQ timeout:", id)
				}
			}
		}
		q.mu.Unlock()
	}
}

func (q *Queue) persistRetry(msg Message) {
	if q.store == nil {
		return
	}
	sm := storage.Message{
		ID:        msg.ID,
		Body:      msg.Body,
		Retry:     msg.Retry,
		Timestamp: msg.Timestamp,
	}
	if err := q.store.Append(q.topic, sm); err != nil {
		log.Println("storage retry append error:", err)
	}
}

func (q *Queue) persistDLQ(msg Message) {
	if q.store == nil {
		return
	}
	topicDLQ := q.topic + ".dlq"
	sm := storage.Message{
		ID:        msg.ID,
		Body:      msg.Body,
		Retry:     msg.Retry,
		Timestamp: msg.Timestamp,
	}
	if err := q.store.Append(topicDLQ, sm); err != nil {
		log.Println("storage dlq append error:", err)
	}
	if f, ok := q.store.(interface{ FlushTopic(string) error }); ok {
		_ = f.FlushTopic(topicDLQ)
	}
	// mark as acked in active topic so replay won't restore it
	if err := q.store.Ack(q.topic, msg.ID); err != nil {
		log.Println("storage dlq ack error:", err)
	}
	if f, ok := q.store.(interface{ FlushTopic(string) error }); ok {
		_ = f.FlushTopic(q.topic)
	}
}
