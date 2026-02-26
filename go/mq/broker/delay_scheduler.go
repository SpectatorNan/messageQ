package broker

import (
	"container/heap"
	"math"
	"sync"
	"time"

	"github.com/SpectatorNan/messageQ/go/mq/storage"
)

// DelayedMessage represents a message scheduled for delayed delivery
type DelayedMessage struct {
	Message   storage.Message
	Topic     string
	QueueID   int
	ExecuteAt time.Time
	index     int // for heap
}

// DelayQueue implements a priority queue for delayed messages
type DelayQueue []*DelayedMessage

func (dq DelayQueue) Len() int           { return len(dq) }
func (dq DelayQueue) Less(i, j int) bool { return dq[i].ExecuteAt.Before(dq[j].ExecuteAt) }
func (dq DelayQueue) Swap(i, j int) {
	dq[i], dq[j] = dq[j], dq[i]
	dq[i].index = i
	dq[j].index = j
}

func (dq *DelayQueue) Push(x interface{}) {
	n := len(*dq)
	item := x.(*DelayedMessage)
	item.index = n
	*dq = append(*dq, item)
}

func (dq *DelayQueue) Pop() interface{} {
	old := *dq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*dq = old[0 : n-1]
	return item
}

// DelayScheduler manages delayed message delivery and retry scheduling
type DelayScheduler struct {
	store      storage.Storage
	delayQueue DelayQueue
	mu         sync.Mutex
	ticker     *time.Ticker
	quit       chan struct{}
	wg         sync.WaitGroup
}

// NewDelayScheduler creates a new delay scheduler
func NewDelayScheduler(store storage.Storage) *DelayScheduler {
	ds := &DelayScheduler{
		store:      store,
		delayQueue: make(DelayQueue, 0),
		quit:       make(chan struct{}),
	}
	heap.Init(&ds.delayQueue)
	return ds
}

// Start begins processing delayed messages
func (ds *DelayScheduler) Start() {
	ds.wg.Add(1)
	go ds.run()
}

// Stop gracefully stops the scheduler
func (ds *DelayScheduler) Stop() {
	close(ds.quit)
	ds.wg.Wait()
}

// Schedule adds a message for delayed delivery
func (ds *DelayScheduler) Schedule(dm *DelayedMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	heap.Push(&ds.delayQueue, dm)
}

// ScheduleWithDelay schedules a message with relative delay from now
func (ds *DelayScheduler) ScheduleWithDelay(topic string, queueID int, msg storage.Message, delay time.Duration) {
	dm := &DelayedMessage{
		Message:   msg,
		Topic:     topic,
		QueueID:   queueID,
		ExecuteAt: time.Now().Add(delay),
	}
	ds.Schedule(dm)
}

// CalculateRetryBackoff calculates exponential backoff delay for retry
func CalculateRetryBackoff(retryCount int) time.Duration {
	// Exponential backoff: 1s, 2s, 4s, 8s, 16s, max 60s
	seconds := math.Pow(2, float64(retryCount))
	if seconds > 60 {
		seconds = 60
	}
	return time.Duration(seconds) * time.Second
}

// run is the main processing loop
func (ds *DelayScheduler) run() {
	defer ds.wg.Done()
	
	// Check every 100ms for due messages
	ds.ticker = time.NewTicker(100 * time.Millisecond)
	defer ds.ticker.Stop()

	for {
		select {
		case <-ds.quit:
			return
		case <-ds.ticker.C:
			ds.processDelayedMessages()
		}
	}
}

// processDelayedMessages checks and delivers due messages
func (ds *DelayScheduler) processDelayedMessages() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	now := time.Now()
	
	for ds.delayQueue.Len() > 0 {
		// Peek at the earliest message
		dm := ds.delayQueue[0]
		
		if dm.ExecuteAt.After(now) {
			// Not yet due, stop processing
			break
		}
		
		// Pop and deliver the message
		heap.Pop(&ds.delayQueue)
		
		// Append to the original topic queue
		err := ds.store.Append(dm.Topic, dm.QueueID, dm.Message)
		if err != nil {
			// Log error but continue processing other messages
			// In production, might want to retry or dead-letter this
			continue
		}
	}
}

// Stats returns current scheduler statistics
func (ds *DelayScheduler) Stats() map[string]interface{} {
	ds.mu.Lock()
	defer ds.mu.Unlock()
	
	return map[string]interface{}{
		"pending_messages": ds.delayQueue.Len(),
		"next_execution":   ds.getNextExecutionTime(),
	}
}

// getNextExecutionTime returns the time of next scheduled message
func (ds *DelayScheduler) getNextExecutionTime() *time.Time {
	if ds.delayQueue.Len() == 0 {
		return nil
	}
	t := ds.delayQueue[0].ExecuteAt
	return &t
}
