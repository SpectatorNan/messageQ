package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// LogType defines record type in WAL
type LogType byte

const (
	LogProduce LogType = 1
	LogAck     LogType = 2
	LogNack    LogType = 3
	LogRetry   LogType = 4
	LogDLQ     LogType = 5
)

// LogEntry is the JSON payload stored for each record (topic included for portability)
type LogEntry struct {
	Type  LogType `json:"type"`
	Topic string  `json:"topic"`
	Msg   Message `json:"msg"`
}

// WALStorage implements a segmented write-ahead log using a simple binary record format:
// [4-byte big-endian uint32 payloadLen][1-byte type][payload JSON bytes]
// It supports per-topic segments, rotation and compaction.
type WALStorage struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File      // current open file per topic (current segment)
	writers map[string]*bufio.Writer // current writer per topic

	flushInterval    time.Duration
	compactInterval  time.Duration
	compactThreshold int64 // bytes

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once

	// compaction coordination (per-topic)
	topicCompacting map[string]bool
	topicCond       map[string]*sync.Cond

	// per-topic counters to trigger rotation/compaction earlier (in-memory)
	bytesSinceCompact map[string]int64
}

// NewWALStorage creates a WAL storage. Optional params: flushInterval, compactInterval.
// Defaults: flushInterval=100ms, compactInterval=5m, compactThreshold=10MB.
func NewWALStorage(baseDir string, params ...time.Duration) *WALStorage {
	_ = os.MkdirAll(baseDir, 0o755)
	fi := 100 * time.Millisecond
	ci := 5 * time.Minute
	threshold := int64(10 * 1024 * 1024)
	if len(params) > 0 && params[0] > 0 {
		fi = params[0]
	}
	if len(params) > 1 && params[1] > 0 {
		ci = params[1]
	}
	w := &WALStorage{
		baseDir:           baseDir,
		files:             make(map[string]*os.File),
		writers:           make(map[string]*bufio.Writer),
		flushInterval:     fi,
		compactInterval:   ci,
		compactThreshold:  threshold,
		quit:              make(chan struct{}),
		bytesSinceCompact: make(map[string]int64),
		topicCompacting:   make(map[string]bool),
		topicCond:         make(map[string]*sync.Cond),
	}
	w.wg.Add(2)
	go w.flusher()
	go w.compactor()
	return w
}

func (w *WALStorage) walDir(topic string) string {
	return filepath.Join(w.baseDir, topic)
}

// ensureTopicLocked must be called with w.mu held (or from a context where we will hold it).
// It opens or creates the current highest-numbered segment in topic directory.
func (w *WALStorage) ensureTopicLocked(topic string) error {
	if _, ok := w.writers[topic]; ok {
		return nil
	}
	if _, ok := w.topicCond[topic]; !ok {
		w.topicCond[topic] = sync.NewCond(&w.mu)
	}
	topicDir := w.walDir(topic)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(topicDir)
	if err != nil {
		return err
	}
	maxSeg := int64(0)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".wal" {
			continue
		}
		base := name[:len(name)-len(".wal")]
		if n, err := strconv.ParseInt(base, 10, 10); err == nil {
			if n > maxSeg {
				maxSeg = n
			}
		}
	}
	if maxSeg == 0 {
		maxSeg = 1
	}
	p := filepath.Join(topicDir, fmt.Sprintf("%08d.wal", maxSeg))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.files[topic] = f
	w.writers[topic] = bufio.NewWriterSize(f, 64*1024)
	if _, ok := w.bytesSinceCompact[topic]; !ok {
		w.bytesSinceCompact[topic] = 0
	}
	return nil
}

// listTopicSegments returns absolute paths of segment files sorted by name
func (w *WALStorage) listTopicSegments(topic string) ([]string, error) {
	entries, err := os.ReadDir(w.walDir(topic))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	segs := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".wal" {
			continue
		}
		segs = append(segs, filepath.Join(w.walDir(topic), name))
	}
	sort.Strings(segs)
	return segs, nil
}

// rotateSegment closes current segment and opens a new numbered one
func (w *WALStorage) rotateSegment(topic string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic); err != nil {
		return err
	}
	segs, err := w.listTopicSegments(topic)
	if err != nil {
		return err
	}
	next := int64(1)
	if len(segs) > 0 {
		last := filepath.Base(segs[len(segs)-1])
		base := last[:len(last)-len(".wal")]
		if n, err := strconv.ParseInt(base, 10, 10); err == nil {
			next = n + 1
		}
	}
	if f, ok := w.files[topic]; ok {
		_ = w.writers[topic].Flush()
		_ = f.Sync()
		_ = f.Close()
		delete(w.files, topic)
		delete(w.writers, topic)
	}
	p := filepath.Join(w.walDir(topic), fmt.Sprintf("%08d.wal", next))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.files[topic] = f
	w.writers[topic] = bufio.NewWriterSize(f, 64*1024)
	w.bytesSinceCompact[topic] = 0
	return nil
}

// writeRecord writes a [len(uint32)][type byte][payload JSON] record to topic's current segment
func (w *WALStorage) writeRecord(topic string, typ LogType, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	payloadLen := uint32(len(data))
	recordLen := int64(4 + 1 + len(data))

	w.mu.Lock()
	if _, ok := w.topicCond[topic]; !ok {
		w.topicCond[topic] = sync.NewCond(&w.mu)
	}
	for w.topicCompacting[topic] {
		w.topicCond[topic].Wait()
	}
	if err := w.ensureTopicLocked(topic); err != nil {
		w.mu.Unlock()
		return err
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], payloadLen)
	if _, err := w.writers[topic].Write(lenBuf[:]); err != nil {
		w.mu.Unlock()
		return err
	}
	if _, err := w.writers[topic].Write([]byte{byte(typ)}); err != nil {
		w.mu.Unlock()
		return err
	}
	if _, err := w.writers[topic].Write(data); err != nil {
		w.mu.Unlock()
		return err
	}
	w.bytesSinceCompact[topic] += recordLen
	shouldRotate := w.bytesSinceCompact[topic] >= w.compactThreshold
	if shouldRotate {
		w.bytesSinceCompact[topic] = 0
	}
	w.mu.Unlock()

	if shouldRotate {
		_ = w.rotateSegment(topic)
	}
	return nil
}

// Append writes an ENQ operation for the message to the WAL (buffered binary JSON).
func (w *WALStorage) Append(topic string, msg Message) error {
	entry := LogEntry{Type: LogProduce, Topic: topic, Msg: msg}
	return w.writeRecord(topic, LogProduce, entry)
}

// Ack appends an ACK operation for the message id (buffered binary JSON).
func (w *WALStorage) Ack(topic string, id int64) error {
	entry := LogEntry{Type: LogAck, Topic: topic, Msg: Message{ID: id}}
	return w.writeRecord(topic, LogAck, entry)
}

// AppendSync writes and then flushes+fsyncs the topic so the write is durable when the call returns.
func (w *WALStorage) AppendSync(topic string, msg Message) error {
	if err := w.Append(topic, msg); err != nil {
		return err
	}
	return w.FlushTopic(topic)
}

// AckSync writes ACK and flushes to make it durable.
func (w *WALStorage) AckSync(topic string, id int64) error {
	if err := w.Ack(topic, id); err != nil {
		return err
	}
	return w.FlushTopic(topic)
}

// FlushTopic flushes and fsyncs a single topic's pending writes.
func (w *WALStorage) FlushTopic(topic string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic); err != nil {
		return err
	}
	wr := w.writers[topic]
	if wr != nil {
		if err := wr.Flush(); err != nil {
			return err
		}
	}
	if f := w.files[topic]; f != nil {
		return f.Sync()
	}
	return nil
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

// internal helper assumes w.mu is held
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

// Load replays all segment files for topic and returns the list of messages that have been ENQ'd and not ACK'd, in enqueue order.
func (w *WALStorage) Load(topic string) ([]Message, error) {
	// flush topic first
	if err := w.FlushTopic(topic); err != nil {
		return nil, err
	}
	segs, err := w.listTopicSegments(topic)
	if err != nil {
		return nil, err
	}
	msgs := make(map[int64]Message)
	order := make([]int64, 0)
	acked := make(map[int64]struct{})

	for _, seg := range segs {
		f, err := os.Open(seg)
		if err != nil {
			return nil, err
		}
		for {
			var lenBuf [4]byte
			if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return nil, err
			}
			n := binary.BigEndian.Uint32(lenBuf[:])
			if n == 0 {
				continue
			}
			// read type byte
			var tbuf [1]byte
			if _, err := io.ReadFull(f, tbuf[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return nil, err
			}
			typ := LogType(tbuf[0])
			payload := make([]byte, n)
			if _, err := io.ReadFull(f, payload); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return nil, err
			}
			switch typ {
			case LogProduce:
				var entry LogEntry
				if err := json.Unmarshal(payload, &entry); err != nil {
					continue
				}
				m := entry.Msg
				if _, seen := msgs[m.ID]; !seen {
					order = append(order, m.ID)
				}
				msgs[m.ID] = m
			case LogAck:
				var entry LogEntry
				if err := json.Unmarshal(payload, &entry); err != nil {
					continue
				}
				m := entry.Msg
				acked[m.ID] = struct{}{}
				delete(msgs, m.ID)
			default:
				// ignore other types for now
			}
		}
		f.Close()
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

func (w *WALStorage) flusher() {
	defer w.wg.Done()
	t := time.NewTicker(w.flushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			_ = w.Flush()
		case <-w.quit:
			_ = w.Flush()
			return
		}
	}
}

// compactor periodically checks topic directories and triggers compaction when threshold exceeded.
func (w *WALStorage) compactor() {
	defer w.wg.Done()
	t := time.NewTicker(w.compactInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			entries, err := os.ReadDir(w.baseDir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				topic := e.Name()
				segs, err := w.listTopicSegments(topic)
				if err != nil {
					continue
				}
				if len(segs) <= 1 {
					continue
				}
				var total int64
				for _, s := range segs[:len(segs)-1] {
					if info, err := os.Stat(s); err == nil {
						total += info.Size()
					}
				}
				if total >= w.compactThreshold {
					go func(tpc string) { _ = w.Compact(tpc) }(topic)
				}
			}
		case <-w.quit:
			return
		}
	}
}

// Compact rewrites the earliest compactable segments for the topic into a single compacted segment file.
func (w *WALStorage) Compact(topic string) error {
	w.mu.Lock()
	if w.topicCompacting[topic] {
		w.mu.Unlock()
		return nil
	}
	w.topicCompacting[topic] = true
	if _, ok := w.topicCond[topic]; !ok {
		w.topicCond[topic] = sync.NewCond(&w.mu)
	}
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.topicCompacting[topic] = false
		w.topicCond[topic].Broadcast()
		w.mu.Unlock()
	}()

	segs, err := w.listTopicSegments(topic)
	if err != nil {
		return err
	}
	if len(segs) <= 1 {
		return nil
	}
	toCompact := segs[:len(segs)-1]

	acked := make(map[int64]struct{})
	order := make([]int64, 0)
	msgs := make(map[int64]Message)
	for _, path := range toCompact {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		for {
			var lenBuf [4]byte
			if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return err
			}
			n := binary.BigEndian.Uint32(lenBuf[:])
			if n == 0 {
				continue
			}
			var tbuf [1]byte
			if _, err := io.ReadFull(f, tbuf[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return err
			}
			typ := LogType(tbuf[0])
			payload := make([]byte, n)
			if _, err := io.ReadFull(f, payload); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return err
			}
			switch typ {
			case LogProduce:
				var entry LogEntry
				if err := json.Unmarshal(payload, &entry); err != nil {
					continue
				}
				m := entry.Msg
				if _, seen := msgs[m.ID]; !seen {
					order = append(order, m.ID)
				}
				msgs[m.ID] = m
			case LogAck:
				var entry LogEntry
				if err := json.Unmarshal(payload, &entry); err != nil {
					continue
				}
				m := entry.Msg
				acked[m.ID] = struct{}{}
				delete(msgs, m.ID)
			}
		}
		f.Close()
	}

	tmp := filepath.Join(w.walDir(topic), ".compact.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, id := range order {
		if _, isAcked := acked[id]; isAcked {
			continue
		}
		m, ok := msgs[id]
		if !ok {
			continue
		}
		data, _ := json.Marshal(m)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write([]byte{byte(LogProduce)}); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(data); err != nil {
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

	first := toCompact[0]
	if err := os.Rename(tmp, first); err != nil {
		os.Remove(tmp)
		return err
	}
	for i := 1; i < len(toCompact); i++ {
		_ = os.Remove(toCompact[i])
	}

	w.mu.Lock()
	if oldf, ok := w.files[topic]; ok {
		_ = w.writers[topic].Flush()
		_ = oldf.Sync()
		_ = oldf.Close()
		delete(w.files, topic)
		delete(w.writers, topic)
	}
	_ = w.ensureTopicLocked(topic)
	w.mu.Unlock()

	return nil
}

// SetCompactThreshold sets the byte threshold to trigger segment rotation/compaction.
func (w *WALStorage) SetCompactThreshold(n int64) {
	w.mu.Lock()
	w.compactThreshold = n
	w.mu.Unlock()
}

// Close stops background goroutines and closes files. Safe to call multiple times.
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
