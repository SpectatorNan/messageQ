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

var errBadCRC = errors.New("bad crc")
var errCorruptRecord = errors.New("corrupt record")

const (
	walMagic      = "MQW1"
	walVersion    = uint16(2)
	walHeaderSize = 8
)

// writeWALHeader writes the magic + version header to a new segment.
func writeWALHeader(w io.Writer) error {
	var hdr [walHeaderSize]byte
	copy(hdr[0:4], []byte(walMagic))
	binary.BigEndian.PutUint16(hdr[4:6], walVersion)
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
	minRecordSize = 4 + 16 + 2 + 8 + 2 + 4 // crc + id(16) + retry + ts + tagLen + bodyLen
	maxRecordSize = 64 * 1024 * 1024       // safety cap
	cqEntrySize   = 20
)

// buildRecordBytes returns record bytes: [totalSize][crc][id(16)][retry][ts][tagLen][tag][bodyLen][body]
func buildRecordBytes(msg Message) ([]byte, int64, error) {
	body := []byte(msg.Body)
	tag := []byte(msg.Tag)
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
	if len(tag) > 0xFFFF {
		return nil, 0, fmt.Errorf("tag too long")
	}
	totalSize := uint32(4 + 16 + 2 + 8 + 2 + len(tag) + 4 + len(body))
	buf := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf[0:4], totalSize)
	off := 4
	binary.BigEndian.PutUint32(buf[off:off+4], crc)
	off += 4
	copy(buf[off:off+16], uidBytes)
	off += 16
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(msg.Retry))
	off += 2
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(ts.UnixNano()))
	off += 8
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(tag)))
	off += 2
	copy(buf[off:off+len(tag)], tag)
	off += len(tag)
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(body)))
	off += 4
	copy(buf[off:], body)
	return buf, int64(len(buf)), nil
}

// readRecordPayload parses a record payload (without the length prefix).
func readRecordPayload(payload []byte) (Message, error) {
	off := 0
	crc := binary.BigEndian.Uint32(payload[off : off+4])
	off += 4
	idBytes := payload[off : off+16]
	off += 16
	uid, err := uuid.FromBytes(idBytes)
	if err != nil {
		return Message{}, errCorruptRecord
	}
	msgID := uid.String()
	retry := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	ts := int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8
	tagLen := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	if tagLen < 0 || off+tagLen > len(payload) {
		return Message{ID: msgID, Retry: retry, Timestamp: time.Unix(0, ts)}, errCorruptRecord
	}
	tag := string(payload[off : off+tagLen])
	off += tagLen
	bodyLen := int(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	if bodyLen < 0 || off+bodyLen > len(payload) {
		return Message{ID: msgID, Retry: retry, Tag: tag, Timestamp: time.Unix(0, ts)}, errCorruptRecord
	}
	body := payload[off : off+bodyLen]
	if bodyLen > 0 && crc32.ChecksumIEEE(body) != crc {
		return Message{ID: msgID, Retry: retry, Tag: tag, Timestamp: time.Unix(0, ts)}, errBadCRC
	}
	msg := Message{ID: msgID, Retry: retry, Tag: tag, Timestamp: time.Unix(0, ts), Body: string(body)}
	return msg, nil
}

// readRecord reads a single record from r; returns ok=false on EOF
func readRecord(r io.Reader) (Message, bool, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Message{}, false, nil
		}
		return Message{}, false, err
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < minRecordSize || total > maxRecordSize {
		return Message{}, false, fmt.Errorf("invalid record size: %d", total)
	}
	payload := make([]byte, total)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Message{}, false, nil
		}
		return Message{}, false, err
	}
	msg, err := readRecordPayload(payload)
	if err != nil {
		return msg, true, err
	}
	return msg, true, nil
}

// readRecordAt reads a record at the given position in a segment file.
func readRecordAt(f *os.File, pos int64) (Message, int64, error) {
	if _, err := f.Seek(pos, io.SeekStart); err != nil {
		return Message{}, 0, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return Message{}, 0, err
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	if total < minRecordSize || total > maxRecordSize {
		return Message{}, 0, fmt.Errorf("invalid record size: %d", total)
	}
	payload := make([]byte, total)
	if _, err := io.ReadFull(f, payload); err != nil {
		return Message{}, 0, err
	}
	msg, err := readRecordPayload(payload)
	if err != nil {
		return msg, 0, err
	}
	return msg, int64(4 + total), nil
}

// WALStorage implements a segmented write-ahead log using a simple binary record format:
// [4-byte totalSize][4-byte bodyCRC][1-byte type][16-byte id][2-byte retry][8-byte ts][4-byte bodyLen][body]
// It supports per-topic segments, rotation and compaction.
type WALStorage struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File      // current open file per topic/queue (current segment)
	writers map[string]*bufio.Writer // current writer per topic/queue

	cqFiles   map[string]*os.File      // consumequeue files per topic/queue
	cqWriters map[string]*bufio.Writer // consumequeue writers per topic/queue

	segmentID  map[string]int64 // current segment id per topic/queue
	segmentPos map[string]int64 // current write position per topic/queue

	flushInterval    time.Duration
	compactInterval  time.Duration
	compactThreshold int64 // bytes

	quit      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once

	// compaction coordination (per-topic/queue)
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
		cqFiles:           make(map[string]*os.File),
		cqWriters:         make(map[string]*bufio.Writer),
		segmentID:         make(map[string]int64),
		segmentPos:        make(map[string]int64),
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

// consumeQueueDir returns the consumequeue directory for a topic/queue.
func (w *WALStorage) consumeQueueDir(topic string, queueID int) string {
	return filepath.Join(w.baseDir, "consumequeue", topic, fmt.Sprintf("%d", queueID))
}

// consumeQueueFile returns the consumequeue file path for a topic/queue.
func (w *WALStorage) consumeQueueFile(topic string, queueID int) string {
	return filepath.Join(w.consumeQueueDir(topic, queueID), "00000001.cq")
}

// ensureConsumeQueueLocked opens consumequeue file for topic/queue.
func (w *WALStorage) ensureConsumeQueueLocked(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	if _, ok := w.cqWriters[key]; ok {
		return nil
	}
	cqDir := w.consumeQueueDir(topic, queueID)
	if err := os.MkdirAll(cqDir, 0o755); err != nil {
		return err
	}
	p := w.consumeQueueFile(topic, queueID)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.cqFiles[key] = f
	w.cqWriters[key] = bufio.NewWriterSize(f, 64*1024)
	return nil
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
	if info, err := f.Stat(); err == nil {
		if info.Size() == 0 {
			if err := writeWALHeader(f); err != nil {
				f.Close()
				return err
			}
			_ = f.Sync()
			_ = syncDir(commitDir)
			w.segmentPos[key] = walHeaderSize
		} else {
			w.segmentPos[key] = info.Size()
		}
	}
	w.segmentID[key] = maxSeg
	w.files[key] = f
	w.writers[key] = bufio.NewWriterSize(f, 64*1024)
	if _, ok := w.bytesSinceCompact[key]; !ok {
		w.bytesSinceCompact[key] = 0
	}
	return w.ensureConsumeQueueLocked(topic, queueID)
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
	w.segmentID[key] = next
	w.segmentPos[key] = walHeaderSize
	w.files[key] = f
	w.writers[key] = bufio.NewWriterSize(f, 64*1024)
	w.bytesSinceCompact[key] = 0
	_ = syncDir(commitDir)
	return nil
}

// writeRecord writes a binary record to topic/queue's current segment.
func (w *WALStorage) writeRecord(topic string, queueID int, msg Message) error {
	key := queueKey(topic, queueID)
	rec, recLen, err := buildRecordBytes(msg)
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
	pos := w.segmentPos[key]
	segID := w.segmentID[key]
	if _, err := w.writers[key].Write(rec); err != nil {
		w.mu.Unlock()
		return err
	}
	w.segmentPos[key] = pos + recLen
	w.bytesSinceCompact[key] += recLen
	shouldRotate := w.bytesSinceCompact[key] >= w.compactThreshold
	if shouldRotate {
		w.bytesSinceCompact[key] = 0
	}
	// write consumequeue entry (tag hash included)
	_ = w.writeConsumeQueueEntryLocked(topic, queueID, uint32(segID), uint64(pos), uint32(recLen), hashTag(msg.Tag))
	w.mu.Unlock()

	if shouldRotate {
		_ = w.rotateSegment(topic, queueID)
	}
	return nil
}

// hashTag computes a tag hash for consumequeue filtering.
func hashTag(tag string) uint32 {
	if tag == "" {
		return 0
	}
	return crc32.ChecksumIEEE([]byte(tag))
}

// writeConsumeQueueEntryLocked appends an index entry to consumequeue.
func (w *WALStorage) writeConsumeQueueEntryLocked(topic string, queueID int, segID uint32, pos uint64, size uint32, tagHash uint32) error {
	key := queueKey(topic, queueID)
	if err := w.ensureConsumeQueueLocked(topic, queueID); err != nil {
		return err
	}
	var buf [20]byte
	binary.BigEndian.PutUint32(buf[0:4], segID)
	binary.BigEndian.PutUint64(buf[4:12], pos)
	binary.BigEndian.PutUint32(buf[12:16], size)
	binary.BigEndian.PutUint32(buf[16:20], tagHash)
	_, err := w.cqWriters[key].Write(buf[:])
	return err
}

// Append writes an ENQ operation for the message to the WAL.
func (w *WALStorage) Append(topic string, queueID int, msg Message) error {
	return w.writeRecord(topic, queueID, msg)
}

// Ack is a no-op in commitlog-only mode (offsets handle consumption).
func (w *WALStorage) Ack(topic string, queueID int, id string) error {
	return nil
}

// AppendSync writes and then flushes+fsyncs the topic/queue so the write is durable when the call returns.
func (w *WALStorage) AppendSync(topic string, queueID int, msg Message) error {
	if err := w.Append(topic, queueID, msg); err != nil {
		return err
	}
	return w.FlushTopic(topic, queueID)
}

// AckSync is a no-op in commitlog-only mode.
func (w *WALStorage) AckSync(topic string, queueID int, id string) error {
	return nil
}

// Load replays all segment files for topic/queue and returns the list of messages in enqueue order.
func (w *WALStorage) Load(topic string, queueID int) ([]Message, error) {
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, err
	}
	segs, err := w.listTopicSegments(topic, queueID)
	if err != nil {
		return nil, err
	}
	msgs := make([]Message, 0)

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
			msg, ok, err := readRecord(f)
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
			msgs = append(msgs, msg)
		}
		f.Close()
	}
	return msgs, nil
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

	msgs := make([]Message, 0)
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
			msg, ok, err := readRecord(f)
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
			msgs = append(msgs, msg)
		}
		f.Close()
	}

	commitDir := w.commitLogDir(topic, queueID)
	tmp := filepath.Join(commitDir, ".compact.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := writeWALHeader(f); err != nil {
		return err
	}
	for _, msg := range msgs {
		rec, _, err := buildRecordBytes(msg)
		if err != nil {
			return err
		}
		if _, err := f.Write(rec); err != nil {
			return err
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	_ = os.Rename(tmp, segs[len(segs)-1])
	_ = syncDir(commitDir)
	return nil
}

// flushTopicLocked flushes and fsyncs a single topic/queue's pending writes.
func (w *WALStorage) flushTopicLocked(key string) error {
	if wr, ok := w.writers[key]; ok {
		if err := wr.Flush(); err != nil {
			return err
		}
	}
	if f, ok := w.files[key]; ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if cqw, ok := w.cqWriters[key]; ok {
		if err := cqw.Flush(); err != nil {
			return err
		}
	}
	if cf, ok := w.cqFiles[key]; ok {
		return cf.Sync()
	}
	return nil
}

// FlushTopic flushes and fsyncs a single topic/queue's pending writes.
func (w *WALStorage) FlushTopic(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureTopicLocked(topic, queueID); err != nil {
		return err
	}
	return w.flushTopicLocked(key)
}

// Flush flushes all in-memory buffers and fsyncs files.
func (w *WALStorage) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for key := range w.writers {
		if err := w.flushTopicLocked(key); err != nil {
			return err
		}
	}
	return nil
}

// Close closes all open segments and waits for background flush/compact goroutines.
func (w *WALStorage) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.quit)
		w.wg.Wait()
		for _, f := range w.files {
			_ = f.Close()
		}
		for _, w := range w.writers {
			_ = w.Flush()
		}
		for _, f := range w.cqFiles {
			_ = f.Close()
		}
		for _, w := range w.cqWriters {
			_ = w.Flush()
		}
	})
	return err
}

// Stats reports the current storage statistics.
func (w *WALStorage) Stats() (map[string]interface{}, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	stats := make(map[string]interface{})
	for key, f := range w.files {
		if info, err := f.Stat(); err == nil {
			stats[key] = map[string]interface{}{
				"size": info.Size(),
			}
		}
	}
	return stats, nil
}

// SetCompactThreshold sets the byte threshold to trigger segment rotation/compaction.
func (w *WALStorage) SetCompactThreshold(n int64) {
	w.mu.Lock()
	w.compactThreshold = n
	w.mu.Unlock()
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

// syncDir performs an fsync on the directory containing path to ensure rename visibility.
func syncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ReadFromConsumeQueue scans consumequeue from offset and returns up to max messages.
func (w *WALStorage) ReadFromConsumeQueue(topic string, queueID int, offset int64, max int, tag string) ([]Message, int64, error) {
	if offset < 0 {
		offset = 0
	}
	if max <= 0 {
		max = 1
	}
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, offset, err
	}
	cqPath := w.consumeQueueFile(topic, queueID)
	cf, err := os.Open(cqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, offset, nil
		}
		return nil, offset, err
	}
	defer cf.Close()

	hash := hashTag(tag)
	start := offset * cqEntrySize
	if _, err := cf.Seek(start, io.SeekStart); err != nil {
		return nil, offset, err
	}

	msgs := make([]Message, 0, max)
	idx := offset
	for len(msgs) < max {
		var buf [cqEntrySize]byte
		if _, err := io.ReadFull(cf, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return msgs, idx, err
		}
		segID := binary.BigEndian.Uint32(buf[0:4])
		pos := binary.BigEndian.Uint64(buf[4:12])
		_ = binary.BigEndian.Uint32(buf[12:16])
		tagHash := binary.BigEndian.Uint32(buf[16:20])
		idx++
		if hash != 0 && tagHash != hash {
			continue
		}
		segPath := filepath.Join(w.commitLogDir(topic, queueID), fmt.Sprintf("%08d.wal", segID))
		segFile, err := os.Open(segPath)
		if err != nil {
			continue
		}
		msg, _, err := readRecordAt(segFile, int64(pos))
		segFile.Close()
		if err != nil {
			continue
		}
		msgs = append(msgs, msg)
	}
	return msgs, idx, nil
}

// ReadFromConsumeQueueWithOffsets scans consumequeue from offset and returns messages with their offsets.
func (w *WALStorage) ReadFromConsumeQueueWithOffsets(topic string, queueID int, offset int64, max int, tag string) ([]MessageWithOffset, int64, error) {
	if offset < 0 {
		offset = 0
	}
	if max <= 0 {
		max = 1
	}
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, offset, err
	}
	cqPath := w.consumeQueueFile(topic, queueID)
	cf, err := os.Open(cqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, offset, nil
		}
		return nil, offset, err
	}
	defer cf.Close()

	hash := hashTag(tag)
	start := offset * cqEntrySize
	if _, err := cf.Seek(start, io.SeekStart); err != nil {
		return nil, offset, err
	}

	msgs := make([]MessageWithOffset, 0, max)
	idx := offset
	for len(msgs) < max {
		var buf [cqEntrySize]byte
		if _, err := io.ReadFull(cf, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return msgs, idx, err
		}
		segID := binary.BigEndian.Uint32(buf[0:4])
		pos := binary.BigEndian.Uint64(buf[4:12])
		_ = binary.BigEndian.Uint32(buf[12:16])
		tagHash := binary.BigEndian.Uint32(buf[16:20])
		entryOffset := idx
		idx++
		if hash != 0 && tagHash != hash {
			continue
		}
		segPath := filepath.Join(w.commitLogDir(topic, queueID), fmt.Sprintf("%08d.wal", segID))
		segFile, err := os.Open(segPath)
		if err != nil {
			continue
		}
		msg, _, err := readRecordAt(segFile, int64(pos))
		_ = segFile.Close()
		if err != nil {
			continue
		}
		msgs = append(msgs, MessageWithOffset{Message: msg, Offset: entryOffset})
	}

	return msgs, idx, nil
}
