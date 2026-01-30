package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Op represents an operation recorded in the WAL.
type Op struct {
	Type string  `json:"type"` // "ENQ" or "ACK"
	ID   int64   `json:"id,omitempty"`
	Msg  Message `json:"msg,omitempty"`
}

type WALStorage struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File
	writers map[string]*bufio.Writer

	flushInterval time.Duration
	quit          chan struct{}
	wg            sync.WaitGroup
	closeOnce     sync.Once
}

// NewWALStorage creates a WAL storage. flushInterval controls how often buffered writes are flushed and fsynced.
// If flushInterval is zero, a sensible default of 100ms is used.
func NewWALStorage(baseDir string, flushInterval ...time.Duration) *WALStorage {
	_ = os.MkdirAll(baseDir, 0o755)
	fi := 100 * time.Millisecond
	if len(flushInterval) > 0 && flushInterval[0] > 0 {
		fi = flushInterval[0]
	}
	w := &WALStorage{
		baseDir:       baseDir,
		files:         make(map[string]*os.File),
		writers:       make(map[string]*bufio.Writer),
		flushInterval: fi,
		quit:          make(chan struct{}),
	}
	w.wg.Add(1)
	go w.flusher()
	return w
}

func (w *WALStorage) walPath(topic string) string {
	return filepath.Join(w.baseDir, topic+".wal")
}

func (w *WALStorage) ensureTopicLocked(topic string) error {
	if _, ok := w.writers[topic]; ok {
		return nil
	}
	p := w.walPath(topic)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.files[topic] = f
	w.writers[topic] = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (w *WALStorage) writeOp(topic string, op Op) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic); err != nil {
		return err
	}
	b, _ := json.Marshal(op)
	_, err := w.writers[topic].Write(append(b, '\n'))
	if err != nil {
		return err
	}
	return nil
}

// Append writes an ENQ operation for the message to the WAL (buffered).
func (w *WALStorage) Append(topic string, msg Message) error {
	op := Op{Type: "ENQ", Msg: msg, ID: msg.ID}
	return w.writeOp(topic, op)
}

// Ack appends an ACK operation for the message id (buffered).
func (w *WALStorage) Ack(topic string, id int64) error {
	op := Op{Type: "ACK", ID: id}
	return w.writeOp(topic, op)
}

// Load replays the WAL and returns the list of messages that have been ENQ'd and not ACK'd, in enqueue order.
// It ensures buffered data for the topic is flushed to disk before replaying.
func (w *WALStorage) Load(topic string) ([]Message, error) {
	// flush the topic's buffer first to ensure we read pending data
	if err := w.FlushTopic(topic); err != nil {
		return nil, err
	}
	p := w.walPath(topic)
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return []Message{}, nil
		}
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	msgs := make(map[int64]Message)
	order := make([]int64, 0)
	acked := make(map[int64]struct{})

	for scanner.Scan() {
		var op Op
		line := scanner.Bytes()
		if err := json.Unmarshal(line, &op); err != nil {
			// skip malformed ops
			continue
		}
		switch op.Type {
		case "ENQ":
			if _, seen := msgs[op.ID]; !seen {
				order = append(order, op.ID)
			}
			msgs[op.ID] = op.Msg
		case "ACK":
			acked[op.ID] = struct{}{}
			delete(msgs, op.ID)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	res := make([]Message, 0, len(order))
	for _, id := range order {
		if _, isAcked := acked[id]; isAcked {
			continue
		}
		if m, ok := msgs[id]; ok {
			res = append(res, m)
		}
	}
	return res, nil
}

// flushTopicLocked assumes w.mu is held and flushes+fsyncs the topic file.
func (w *WALStorage) flushTopicLocked(topic string) error {
	wr, ok := w.writers[topic]
	if !ok {
		return nil
	}
	if err := wr.Flush(); err != nil {
		return err
	}
	f := w.files[topic]
	if f == nil {
		return nil
	}
	return f.Sync()
}

// FlushTopic flushes and fsyncs a single topic's pending writes.
func (w *WALStorage) FlushTopic(topic string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.flushTopicLocked(topic)
}

// Flush flushes all in-memory buffers and fsyncs files.
func (w *WALStorage) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for topic := range w.writers {
		if err := w.flushTopicLocked(topic); err != nil {
			return err
		}
	}
	return nil
}

func (w *WALStorage) flusher() {
	defer w.wg.Done()
	t := time.NewTicker(w.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = w.Flush()
		case <-w.quit:
			// final flush before exit
			_ = w.Flush()
			return
		}
	}
}

// Compact rewrites the WAL for the given topic to contain only ENQ ops for currently live messages.
// This helps to bound the WAL size.
func (w *WALStorage) Compact(topic string) error {
	// To avoid deadlock, call Load which flushes topic.
	msgs, err := w.Load(topic)
	if err != nil {
		return err
	}

	tmp := w.walPath(topic) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		op := Op{Type: "ENQ", ID: m.ID, Msg: m}
		b, _ := json.Marshal(op)
		if _, err := f.Write(append(b, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	f.Close()
	p := w.walPath(topic)
	if err := os.Rename(tmp, p); err != nil {
		return fmt.Errorf("compact rename: %w", err)
	}
	return nil
}

// Close stops the flusher and closes files.
func (w *WALStorage) Close() error {
	var firstErr error
	w.closeOnce.Do(func() {
		close(w.quit)
		w.wg.Wait()
		w.mu.Lock()
		defer w.mu.Unlock()
		for topic, wr := range w.writers {
			if err := wr.Flush(); err != nil && firstErr == nil {
				firstErr = err
			}
			if f := w.files[topic]; f != nil {
				if err := f.Sync(); err != nil && firstErr == nil {
					firstErr = err
				}
				if err := f.Close(); err != nil && firstErr == nil {
					firstErr = err
				}
			}
		}
	})
	return firstErr
}
