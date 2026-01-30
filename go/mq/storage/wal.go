package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Op represents an operation recorded in the WAL (binary/gob encoded).
type Op struct {
	Type string // "ENQ" or "ACK"
	ID   int64
	Msg  Message
}

// WALStorage implements a write-ahead log with buffered writes, periodic flush, and background compaction.
// It stores length-prefixed gob-encoded Op records for compact and fast binary IO.
// It is safe for concurrent use.
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

	// per-topic counters to trigger compaction earlier (in-memory)
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
	// register gob types
	gob.Register(Op{})
	gob.Register(Message{})
	return w
}

func (w *WALStorage) walPath(topic string) string {
	return filepath.Join(w.baseDir, topic+".wal")
}

// ensureTopicLocked must be called with w.mu held (or from a context where we will hold it).
func (w *WALStorage) ensureTopicLocked(topic string) error {
	// caller must hold w.mu
	if _, ok := w.writers[topic]; ok {
		return nil
	}
	// ensure per-topic cond exists
	if _, ok := w.topicCond[topic]; !ok {
		w.topicCond[topic] = sync.NewCond(&w.mu)
	}

	topicDir := filepath.Join(w.baseDir, topic)
	if err := os.MkdirAll(topicDir, 0o755); err != nil {
		return err
	}
	// find the highest segment number
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
	// initialize counter if absent
	if _, ok := w.bytesSinceCompact[topic]; !ok {
		w.bytesSinceCompact[topic] = 0
	}
	return nil
}

// helper: list segments for a topic in order
func (w *WALStorage) listTopicSegments(topic string) ([]string, error) {
	topicDir := filepath.Join(w.baseDir, topic)
	entries, err := os.ReadDir(topicDir)
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
		segs = append(segs, filepath.Join(topicDir, name))
	}
	sort.Strings(segs)
	return segs, nil
}

// helper: rotate current segment for a topic (close current and open a new numbered segment)
func (w *WALStorage) rotateSegment(topic string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic); err != nil {
		return err
	}
	// determine next segment number by scanning current files
	segs, err := w.listTopicSegments(topic)
	if err != nil {
		return err
	}
	next := int64(1)
	if len(segs) > 0 {
		// parse last segment filename to get number
		last := filepath.Base(segs[len(segs)-1])
		base := last[:len(last)-len(".wal")]
		if n, err := strconv.ParseInt(base, 10, 10); err == nil {
			next = n + 1
		}
	}
	// close current
	if f, ok := w.files[topic]; ok {
		_ = w.writers[topic].Flush()
		_ = f.Sync()
		_ = f.Close()
		delete(w.files, topic)
		delete(w.writers, topic)
	}
	// open next segment
	p := filepath.Join(w.baseDir, topic, fmt.Sprintf("%08d.wal", next))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.files[topic] = f
	w.writers[topic] = bufio.NewWriterSize(f, 64*1024)
	w.bytesSinceCompact[topic] = 0
	return nil
}

// writeOp now waits per-topic compacting flag instead of global
func (w *WALStorage) writeOp(topic string, op Op) error {
	// encode op to buffer (no locks while encoding)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(op); err != nil {
		return err
	}
	payload := buf.Bytes()
	recordLen := int64(len(payload) + 8) // length prefix + payload

	w.mu.Lock()
	// ensure topic state
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
	// write length prefix
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(payload)))
	if _, err := w.writers[topic].Write(lenBuf[:]); err != nil {
		w.mu.Unlock()
		return err
	}
	if _, err := w.writers[topic].Write(payload); err != nil {
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

// Append writes an ENQ operation for the message to the WAL (buffered binary).
func (w *WALStorage) Append(topic string, msg Message) error {
	op := Op{Type: "ENQ", Msg: msg, ID: msg.ID}
	return w.writeOp(topic, op)
}

// Ack appends an ACK operation for the message id (buffered binary).
func (w *WALStorage) Ack(topic string, id int64) error {
	op := Op{Type: "ACK", ID: id}
	return w.writeOp(topic, op)
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

// Load replays the binary WAL and returns the list of messages that have been ENQ'd and not ACK'd, in enqueue order.
// It flushes the topic buffer before reading.
func (w *WALStorage) Load(topic string) ([]Message, error) {
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

	msgs := make(map[int64]Message)
	order := make([]int64, 0)
	acked := make(map[int64]struct{})

	for {
		// read length
		var lenBuf [8]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		n := binary.BigEndian.Uint64(lenBuf[:])
		if n == 0 {
			continue
		}
		if n > (1 << 31) { // sanity cap to avoid huge allocations from corrupted data
			return nil, fmt.Errorf("record length too large: %d", n)
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		var op Op
		dec := gob.NewDecoder(bytes.NewReader(payload))
		if err := dec.Decode(&op); err != nil {
			// skip malformed
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
			// list topic directories under baseDir
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
				// compute total size of old segments (exclude last/current)
				var total int64
				for _, s := range segs[:len(segs)-1] {
					if info, err := os.Stat(s); err == nil {
						total += info.Size()
					}
				}
				if total >= w.compactThreshold {
					// trigger compaction asynchronously
					go func(tpc string) { _ = w.Compact(tpc) }(topic)
				}
			}
		case <-w.quit:
			return
		}
	}
}

// Compact rewrites the WAL for the given topic into binary ENQ records only for live messages.
func (w *WALStorage) Compact(topic string) error {
	// mark topic as compacting
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

	// list segments
	segs, err := w.listTopicSegments(topic)
	if err != nil {
		return err
	}
	if len(segs) <= 1 {
		// nothing to compact (only current or no segments)
		return nil
	}
	// compact all segments except the last (current)
	toCompact := segs[:len(segs)-1]

	// streaming two-pass: first collect ack set across toCompact
	acked := make(map[int64]struct{})
	order := make([]int64, 0)
	msgs := make(map[int64]Message)
	for _, path := range toCompact {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		for {
			var lenBuf [8]byte
			if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return err
			}
			n := binary.BigEndian.Uint64(lenBuf[:])
			if n == 0 {
				continue
			}
			payload := make([]byte, n)
			if _, err := io.ReadFull(f, payload); err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					break
				}
				f.Close()
				return err
			}
			var op Op
			if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&op); err != nil {
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
		f.Close()
	}

	// write compacted tmp file containing only live ENQ records from toCompact in original order
	tmp := filepath.Join(w.baseDir, topic, ".compact.tmp")
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
		op := Op{Type: "ENQ", ID: id, Msg: m}
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(op); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		var lenBuf [8]byte
		binary.BigEndian.PutUint64(lenBuf[:], uint64(buf.Len()))
		if _, err := f.Write(lenBuf[:]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(buf.Bytes()); err != nil {
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

	// replace the first compacted segment file with tmp, and remove other compacted segments
	first := toCompact[0]
	if err := os.Rename(tmp, first); err != nil {
		// cleanup tmp on failure
		os.Remove(tmp)
		return err
	}
	// remove the rest of compacted segment files (skip first since replaced)
	for i := 1; i < len(toCompact); i++ {
		_ = os.Remove(toCompact[i])
	}

	// reopen writer for topic (close current and reopen to refresh state)
	w.mu.Lock()
	if oldf, ok := w.files[topic]; ok {
		_ = w.writers[topic].Flush()
		_ = oldf.Sync()
		_ = oldf.Close()
		delete(w.files, topic)
		delete(w.writers, topic)
	}
	// ensure reopen (this will open the current segment file)
	_ = w.ensureTopicLocked(topic)
	w.mu.Unlock()

	return nil
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
