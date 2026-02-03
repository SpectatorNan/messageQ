package queue

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"

	"messageQ/mq/logger"
	"messageQ/mq/storage"
)

type Queue struct {
	mu   sync.Mutex
	cond *sync.Cond
	data []Message

	store   storage.Storage
	topic   string
	queueID int
}

func NewQueue() *Queue {
	q := &Queue{
		data:    make([]Message, 0),
		queueID: 0,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func NewQueueWithStorage(store storage.Storage, topic string, queueID int) *Queue {
	q := &Queue{
		data:    make([]Message, 0),
		store:   store,
		topic:   topic,
		queueID: queueID,
	}
	q.cond = sync.NewCond(&q.mu)
	// load persisted messages into data
	if store != nil {
		msgs, err := store.Load(topic, queueID)
		if err != nil {
			logger.Error("Failed to load messages from storage",
				zap.String("topic", topic),
				zap.Int("queue_id", queueID),
				zap.Error(err))
		} else {
			// convert storage.Message to queue.Message
			for _, sm := range msgs {
				qm := Message{
					ID:        sm.ID,
					Body:      sm.Body,
					Tag:       sm.Tag,
					Retry:     sm.Retry,
					Timestamp: sm.Timestamp,
				}
				q.data = append(q.data, qm)
			}
		}
	}
	return q
}

// TryDequeue is deprecated - system uses ConsumeQueue + Offset pattern
func (q *Queue) TryDequeue() (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.data) == 0 {
		return Message{}, false
	}
	msg := q.data[0]
	q.data = q.data[1:]
	return msg, true
}

// Enqueue adds a message with an optional tag.
func (q *Queue) Enqueue(body string, tag string) Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if tag == "" {
		logger.Warn("Enqueue rejected: empty tag",
			zap.String("topic", q.topic),
			zap.Int("queue_id", q.queueID))
		return Message{}
	}

	uid, err := uuid.NewV7()
	if err != nil {
		uid = uuid.New()
	}
	msg := Message{
		ID:        uid.String(),
		Body:      body,
		Tag:       tag,
		Timestamp: time.Now(),
	}

	q.data = append(q.data, msg)
	if q.store != nil {
		sm := storage.Message{
			ID:        msg.ID,
			Body:      msg.Body,
			Tag:       msg.Tag,
			Retry:     msg.Retry,
			Timestamp: msg.Timestamp,
		}
		_ = q.store.Append(q.topic, q.queueID, sm)
	}
	q.cond.Signal()
	return msg
}

// EnqueueBody keeps backward compatibility for callers without tags.
func (q *Queue) EnqueueBody(body string) Message {
	return q.Enqueue(body, "")
}

// Dequeue is deprecated - system uses ConsumeQueue + Offset pattern
func (q *Queue) Dequeue() Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.data) == 0 {
		q.cond.Wait()
	}

	msg := q.data[0]
	q.data = q.data[1:]
	return msg
}

// TryDequeueTag is deprecated - system uses ConsumeQueue + Offset pattern
func (q *Queue) TryDequeueTag(tag string) (Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg, ok := q.popByTagLocked(tag)
	if !ok {
		return Message{}, false
	}
	return msg, true
}

// DequeueTag is deprecated - system uses ConsumeQueue + Offset pattern
func (q *Queue) DequeueTag(tag string) Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		msg, ok := q.popByTagLocked(tag)
		if ok {
			return msg
		}
		q.cond.Wait()
	}
}

// popByTagLocked removes and returns the first message matching tag.
func (q *Queue) popByTagLocked(tag string) (Message, bool) {
	if len(q.data) == 0 {
		return Message{}, false
	}
	if tag == "" {
		msg := q.data[0]
		q.data = q.data[1:]
		return msg, true
	}
	for i, m := range q.data {
		if m.Tag == tag {
			msg := m
			q.data = append(q.data[:i], q.data[i+1:]...)
			return msg, true
		}
	}
	return Message{}, false
}

func (q *Queue) persistRetry(msg Message) {
	if q.store == nil {
		return
	}
	sm := storage.Message{
		ID:        msg.ID,
		Body:      msg.Body,
		Tag:       msg.Tag,
		Retry:     msg.Retry,
		Timestamp: msg.Timestamp,
	}
	if err := q.store.Append(q.topic, q.queueID, sm); err != nil {
		logger.Error("Failed to persist retry message",
			zap.String("topic", q.topic),
			zap.Int("queue_id", q.queueID),
			zap.String("message_id", msg.ID),
			zap.Error(err))
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
		Tag:       msg.Tag,
		Retry:     msg.Retry,
		Timestamp: msg.Timestamp,
	}
	if err := q.store.Append(topicDLQ, 0, sm); err != nil {
		logger.Error("Failed to append message to DLQ",
			zap.String("topic", q.topic),
			zap.String("dlq_topic", topicDLQ),
			zap.String("message_id", msg.ID),
			zap.Error(err))
	}
	if f, ok := q.store.(interface{ FlushTopic(string, int) error }); ok {
		_ = f.FlushTopic(topicDLQ, 0)
	}
	// mark as acked in active topic so replay won't restore it
	if err := q.store.Ack(q.topic, q.queueID, msg.ID); err != nil {
		logger.Error("Failed to ack message in DLQ processing",
			zap.String("topic", q.topic),
			zap.Int("queue_id", q.queueID),
			zap.String("message_id", msg.ID),
			zap.Error(err))
	}
	if f, ok := q.store.(interface{ FlushTopic(string, int) error }); ok {
		_ = f.FlushTopic(q.topic, q.queueID)
	}
}
