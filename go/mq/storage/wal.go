package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
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

var errBadCRC = errors.New("bad crc")
var errCorruptRecord = errors.New("corrupt record")

const (
	walMagic      = "MQW1"
	walVersion    = uint16(1)
	walHeaderSize = 8
)

// writeWALHeader writes the magic + version header to a new segment.
func writeWALHeader(w io.Writer) error {
	var hdr [walHeaderSize]byte
	copy(hdr[0:4], []byte(walMagic))
	binary.BigEndian.PutUint16(hdr[4:6], walVersion)
	// hdr[6:8] reserved
	_, err := w.Write(hdr[:])
	return err
}

// readWALHeader validates the magic + version header. Returns io.EOF for empty file.
func readWALHeader(r io.Reader) error {
	var hdr [walHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return io.EOF
		}
		return err
	}
	if string(hdr[0:4]) != walMagic {
		return fmt.Errorf("invalid wal magic")
	}
	ver := binary.BigEndian.Uint16(hdr[4:6])
	if ver != walVersion {
		return fmt.Errorf("unsupported wal version: %d", ver)
	}
	return nil
}

const (
	minRecordSize = 4 + 1 + 16 + 2 + 8 + 4 // crc + type + id(16) + retry + ts + bodyLen
	maxRecordSize = 64 * 1024 * 1024       // safety cap
)

// buildRecordBytes returns the full record bytes: [totalSize][crc][type][id(16)][retry][ts][bodyLen][body]
func buildRecordBytes(typ LogType, msg Message) ([]byte, int64, error) {
	body := []byte(msg.Body)
	if typ != LogProduce {
		body = nil
	}
	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	crc := uint32(0)
	if len(body) > 0 {
		crc = crc32.ChecksumIEEE(body)
	}
	uid, err := uuid.Parse(msg.ID)
	if err != nil {
		return nil, 0, err
	}
	uidBytes := uid[:]

	totalSize := uint32(4 + 1 + 16 + 2 + 8 + 4 + len(body))
	buf := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf[0:4], totalSize)
	off := 4
	binary.BigEndian.PutUint32(buf[off:off+4], crc)
	off += 4
	buf[off] = byte(typ)
	off += 1
	copy(buf[off:off+16], uidBytes)
	off += 16
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(msg.Retry))
	off += 2
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(ts.UnixNano()))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(body)))
	off += 4
	copy(buf[off:], body)
	return buf, int64(len(buf)), nil
}

// readRecord reads a single record from r; returns ok=false on EOF
func readRecord(r io.Reader) (LogType, Message, bool, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, Message{}, false, nil
		}
		return 0, Message{}, false, err
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < minRecordSize || total > maxRecordSize {
		return 0, Message{}, false, fmt.Errorf("invalid record size: %d", total)
	}
	payload := make([]byte, total)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, Message{}, false, nil
		}
		return 0, Message{}, false, err
	}
	off := 0
	crc := binary.BigEndian.Uint32(payload[off : off+4])
	off += 4
	typ := LogType(payload[off])
	off += 1
	idBytes := payload[off : off+16]
	off += 16
	uid, err := uuid.FromBytes(idBytes)
	if err != nil {
		return typ, Message{}, true, errCorruptRecord
	}
	msgID := uid.String()
	retry := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	ts := int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8
	bodyLen := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	if bodyLen < 0 || off+bodyLen > len(payload) {
		return typ, Message{ID: msgID, Retry: retry, Timestamp: time.Unix(0, ts)}, true, errCorruptRecord
	}
	body := payload[off : off+bodyLen]
	if bodyLen > 0 && crc32.ChecksumIEEE(body) != crc {
		return typ, Message{ID: msgID, Retry: retry, Timestamp: time.Unix(0, ts)}, true, errBadCRC
	}
	msg := Message{ID: msgID, Retry: retry, Timestamp: time.Unix(0, ts), Body: string(body)}
	return typ, msg, true, nil
}

// WALStorage implements a segmented write-ahead log using a simple binary record format:
// [4-byte totalSize][4-byte bodyCRC][1-byte type][16-byte id][2-byte retry][8-byte ts][4-byte bodyLen][body]
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

// queueKey builds a unique key for per-queue maps.
func queueKey(topic string, queueID int) string {
	return fmt.Sprintf("%s#%d", topic, queueID)
}

// commitLogDir returns the commitlog directory for a topic/queue.
func (w *WALStorage) commitLogDir(topic string, queueID int) string {
	return filepath.Join(w.baseDir, "commitlog", topic, fmt.Sprintf("%d", queueID))
}

// ensureTopicLocked must be called with w.mu held (or from a context where we will hold it).
// It opens or creates the current highest-numbered segment in topic/queue directory.
func (w *WALStorage) ensureTopicLocked(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	if _, ok := w.writers[key]; ok {
		return nil
	}
	if _, ok := w.topicCond[key]; !ok {
		w.topicCond[key] = sync.NewCond(&w.mu)
	}
	commitDir := w.commitLogDir(topic, queueID)
	if err := os.MkdirAll(commitDir, 0o755); err != nil {
		return err
	}
	// create consumequeue dir for layout completeness (not used yet)
	_ = os.MkdirAll(filepath.Join(w.baseDir, "consumequeue", topic, fmt.Sprintf("%d", queueID)), 0o755)
	entries, err := os.ReadDir(commitDir)
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
	p := filepath.Join(commitDir, fmt.Sprintf("%08d.wal", maxSeg))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	// write header if file is new/empty
	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		if err := writeWALHeader(f); err != nil {
			f.Close()
			return err
		}
		_ = f.Sync()
		_ = syncDir(commitDir)
	}
	w.files[key] = f
	w.writers[key] = bufio.NewWriterSize(f, 64*1024)
	if _, ok := w.bytesSinceCompact[key]; !ok {
		w.bytesSinceCompact[key] = 0
	}
	return nil
}

// listTopicSegments returns absolute paths of segment files sorted by name for a topic/queue.
func (w *WALStorage) listTopicSegments(topic string, queueID int) ([]string, error) {
	commitDir := w.commitLogDir(topic, queueID)
	entries, err := os.ReadDir(commitDir)
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
		segs = append(segs, filepath.Join(commitDir, name))
	}
	sort.Strings(segs)
	return segs, nil
}

// rotateSegment closes current segment and opens a new numbered one.
func (w *WALStorage) rotateSegment(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic, queueID); err != nil {
		return err
	}
	segs, err := w.listTopicSegments(topic, queueID)
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
	if f, ok := w.files[key]; ok {
		_ = w.writers[key].Flush()
		_ = f.Sync()
		_ = f.Close()
		delete(w.files, key)
		delete(w.writers, key)
	}
	commitDir := w.commitLogDir(topic, queueID)
	p := filepath.Join(commitDir, fmt.Sprintf("%08d.wal", next))
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := writeWALHeader(f); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	w.files[key] = f
	w.writers[key] = bufio.NewWriterSize(f, 64*1024)
	w.bytesSinceCompact[key] = 0
	_ = syncDir(commitDir)
	return nil
}

// writeRecord writes a binary record to topic/queue's current segment.
func (w *WALStorage) writeRecord(topic string, queueID int, typ LogType, msg Message) error {
	key := queueKey(topic, queueID)
	rec, recLen, err := buildRecordBytes(typ, msg)
	if err != nil {
		return err
	}

	w.mu.Lock()
	if _, ok := w.topicCond[key]; !ok {
		w.topicCond[key] = sync.NewCond(&w.mu)
	}
	for w.topicCompacting[key] {
		w.topicCond[key].Wait()
	}
	if err := w.ensureTopicLocked(topic, queueID); err != nil {
		w.mu.Unlock()
		return err
	}
	if _, err := w.writers[key].Write(rec); err != nil {
		w.mu.Unlock()
		return err
	}
	w.bytesSinceCompact[key] += recLen
	shouldRotate := w.bytesSinceCompact[key] >= w.compactThreshold
	if shouldRotate {
		w.bytesSinceCompact[key] = 0
	}
	w.mu.Unlock()

	if shouldRotate {
		_ = w.rotateSegment(topic, queueID)
	}
	return nil
}

// Append writes an ENQ operation for the message to the WAL (binary + CRC).
func (w *WALStorage) Append(topic string, queueID int, msg Message) error {
	return w.writeRecord(topic, queueID, LogProduce, msg)
}

// Ack appends an ACK operation for the message id (binary + CRC, body empty).
func (w *WALStorage) Ack(topic string, queueID int, id string) error {
	return w.writeRecord(topic, queueID, LogAck, Message{ID: id})
}

// AppendSync writes and then flushes+fsyncs the topic/queue so the write is durable when the call returns.
func (w *WALStorage) AppendSync(topic string, queueID int, msg Message) error {
	if err := w.Append(topic, queueID, msg); err != nil {
		return err
	}
	return w.FlushTopic(topic, queueID)
}

// AckSync writes ACK and flushes to make it durable.
func (w *WALStorage) AckSync(topic string, queueID int, id string) error {
	if err := w.Ack(topic, queueID, id); err != nil {
		return err
	}
	return w.FlushTopic(topic, queueID)
}

// FlushTopic flushes and fsyncs a single topic/queue's pending writes.
func (w *WALStorage) FlushTopic(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic, queueID); err != nil {
		return err
	}
	wr := w.writers[key]
	if wr != nil {
		if err := wr.Flush(); err != nil {
			return err
		}
	}
	if f := w.files[key]; f != nil {
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

// Load replays all segment files for topic/queue and returns the list of messages that have been ENQ'd and not ACK'd, in enqueue order.
func (w *WALStorage) Load(topic string, queueID int) ([]Message, error) {
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, err
	}
	segs, err := w.listTopicSegments(topic, queueID)
	if err != nil {
		return nil, err
	}
	msgs := make(map[string]Message)
	order := make([]string, 0)
	acked := make(map[string]struct{})

	for _, seg := range segs {
		f, err := os.Open(seg)
		if err != nil {
			return nil, err
		}
		if info, err := f.Stat(); err == nil && info.Size() == 0 {
			f.Close()
			continue
		}
		if err := readWALHeader(f); err != nil {
			if err == io.EOF {
				f.Close()
				continue
			}
			f.Close()
			return nil, err
		}
		for {
			typ, msg, ok, err := readRecord(f)
			if err != nil {
				if errors.Is(err, errBadCRC) || errors.Is(err, errCorruptRecord) {
					continue
				}
				f.Close()
				return nil, err
			}
			if !ok {
				break
			}
			switch typ {
			case LogProduce:
				if _, seen := msgs[msg.ID]; !seen {
					order = append(order, msg.ID)
				}
				msgs[msg.ID] = msg
			case LogAck:
				acked[msg.ID] = struct{}{}
				delete(msgs, msg.ID)
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

// Compact rewrites the earliest compactable segments for the topic/queue into a single compacted segment file.
func (w *WALStorage) Compact(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	if w.topicCompacting[key] {
		w.mu.Unlock()
		return nil
	}
	w.topicCompacting[key] = true
	if _, ok := w.topicCond[key]; !ok {
		w.topicCond[key] = sync.NewCond(&w.mu)
	}
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.topicCompacting[key] = false
		w.topicCond[key].Broadcast()
		w.mu.Unlock()
	}()

	segs, err := w.listTopicSegments(topic, queueID)
	if err != nil {
		return err
	}
	if len(segs) <= 1 {
		return nil
	}
	toCompact := segs[:len(segs)-1]

	acked := make(map[string]struct{})
	order := make([]string, 0)
	msgs := make(map[string]Message)
	for _, path := range toCompact {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		if info, err := f.Stat(); err == nil && info.Size() == 0 {
			f.Close()
			continue
		}
		if err := readWALHeader(f); err != nil {
			if err == io.EOF {
				f.Close()
				continue
			}
			f.Close()
			return err
		}
		for {
			typ, msg, ok, err := readRecord(f)
			if err != nil {
				if errors.Is(err, errBadCRC) || errors.Is(err, errCorruptRecord) {
					continue
				}
				f.Close()
				return err
			}
			if !ok {
				break
			}
			switch typ {
			case LogProduce:
				if _, seen := msgs[msg.ID]; !seen {
					order = append(order, msg.ID)
				}
				msgs[msg.ID] = msg
			case LogAck:
				acked[msg.ID] = struct{}{}
				delete(msgs, msg.ID)
			}
		}
		f.Close()
	}

	commitDir := w.commitLogDir(topic, queueID)
	tmp := filepath.Join(commitDir, ".compact.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeWALHeader(f); err != nil {
		f.Close()
		os.Remove(tmp)
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
		rec, _, err := buildRecordBytes(LogProduce, m)
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		if _, err := f.Write(rec); err != nil {
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
	_ = syncDir(commitDir)
	for i := 1; i < len(toCompact); i++ {
		_ = os.Remove(toCompact[i])
	}
	_ = syncDir(commitDir)

	w.mu.Lock()
	if oldf, ok := w.files[key]; ok {
		_ = w.writers[key].Flush()
		_ = oldf.Sync()
		_ = oldf.Close()
		delete(w.files, key)
		delete(w.writers, key)
	}
	_ = w.ensureTopicLocked(topic, queueID)
	w.mu.Unlock()

	return nil
}

// syncDir performs an fsync on the directory containing path to ensure rename visibility.
func syncDir(dirPath string) error {
	// open the directory and sync it
	d, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

// compactor periodically checks commitlog directories and triggers compaction when threshold exceeded.
func (w *WALStorage) compactor() {
	defer w.wg.Done()
	t := time.NewTicker(w.compactInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			commitBase := filepath.Join(w.baseDir, "commitlog")
			topics, err := os.ReadDir(commitBase)
			if err != nil {
				continue
			}
			for _, te := range topics {
				if !te.IsDir() {
					continue
				}
				topic := te.Name()
				queues, err := os.ReadDir(filepath.Join(commitBase, topic))
				if err != nil {
					continue
				}
				for _, qe := range queues {
					if !qe.IsDir() {
						continue
					}
					qid, err := strconv.Atoi(qe.Name())
					if err != nil {
						continue
					}
					segs, err := w.listTopicSegments(topic, qid)
					if err != nil || len(segs) <= 1 {
						continue
					}
					var total int64
					for _, s := range segs[:len(segs)-1] {
						if info, err := os.Stat(s); err == nil {
							total += info.Size()
						}
					}
					if total >= w.compactThreshold {
						go func(tp string, q int) { _ = w.Compact(tp, q) }(topic, qid)
					}
				}
			}
		case <-w.quit:
			return
		}
	}
}
