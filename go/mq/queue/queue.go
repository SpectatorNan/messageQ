package queue

import (
	"log"
	"sync"
	"time"
)

type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	data     []Message
	inflight map[int64]InflightMsg
	nextID   int64

	ackTimeout time.Duration
	maxRetry   int
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
		log.Println("DLQ:", im.Msg.ID)
		return true
	}

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
					q.data = append(q.data, im.Msg)
					q.cond.Signal()
				} else {
					log.Println("DLQ timeout:", id)
				}
			}
		}
		q.mu.Unlock()
	}
}
