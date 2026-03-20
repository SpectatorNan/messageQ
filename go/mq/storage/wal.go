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

// buildRecordBytes returns record bytes:
// [totalSize][crc][id(16)][retry][ts][tagLen][tag][bodyLen][body][correlationIdLen][correlationId]
func buildRecordBytes(msg Message) ([]byte, int64, error) {
	body := []byte(msg.Body)
	tag := []byte(msg.Tag)
	correlationID := []byte(msg.CorrelationID)
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
	if len(correlationID) > 0xFFFF {
		return nil, 0, fmt.Errorf("correlation id too long")
	}
	totalSize := uint32(4 + 16 + 2 + 8 + 2 + len(tag) + 4 + len(body) + 2 + len(correlationID))
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
	off += len(body)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(correlationID)))
	off += 2
	copy(buf[off:], correlationID)
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
	off += bodyLen
	msg := Message{ID: msgID, Retry: retry, Tag: tag, Timestamp: time.Unix(0, ts), Body: string(body)}
	if off == len(payload) {
		return msg, nil
	}
	if off+2 > len(payload) {
		return msg, errCorruptRecord
	}
	correlationIDLen := int(binary.BigEndian.Uint16(payload[off : off+2]))
	off += 2
	if off+correlationIDLen > len(payload) {
		return msg, errCorruptRecord
	}
	msg.CorrelationID = string(payload[off : off+correlationIDLen])
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
// It supports per-topic segments, size-based rotation, and sealed-segment retention pruning.
type WALStorage struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*os.File      // current open file per topic/queue (current segment)
	writers map[string]*bufio.Writer // current writer per topic/queue

	cqFiles          map[string]*os.File      // consumequeue files per topic/queue
	cqWriters        map[string]*bufio.Writer // consumequeue writers per topic/queue
	consumeQueueBase map[string]int64

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

	retentionWindow time.Duration
	pruneObserver   func(topic string, queueID int, msg Message) error
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
		consumeQueueBase:  make(map[string]int64),
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
	if err := w.recoverConsumeQueueTrimLocked(topic, queueID); err != nil {
		return err
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
	w.ensureTopicCondLocked(key)
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
	w.ensureTopicCondLocked(key)
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
	w.waitForTopicReady(topic, queueID)
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

func (w *WALStorage) withTopicCompaction(topic string, queueID int, fn func() error) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	if w.topicCompacting[key] {
		w.mu.Unlock()
		return nil
	}
	w.topicCompacting[key] = true
	w.ensureTopicCondLocked(key)
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.topicCompacting[key] = false
		w.topicCond[key].Broadcast()
		w.mu.Unlock()
	}()

	return fn()
}

func parseSegmentIDFromPath(path string) (uint32, error) {
	name := filepath.Base(path)
	if filepath.Ext(name) != ".wal" {
		return 0, fmt.Errorf("invalid segment path %s", path)
	}
	base := name[:len(name)-len(".wal")]
	n, err := strconv.ParseUint(base, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(n), nil
}

func segmentHasMessages(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.Size() > walHeaderSize, nil
}

func (w *WALStorage) consumedSegmentCandidates(topic string, queueID int, base, cutoffOffset int64, latestSegID uint32) ([]uint32, error) {
	if cutoffOffset <= base {
		return nil, nil
	}
	cqPath := w.consumeQueueFile(topic, queueID)
	f, err := os.Open(cqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	seen := make(map[uint32]struct{})
	candidates := make([]uint32, 0)
	logicalOffset := base
	var currentSeg uint32
	haveCurrent := false

	for {
		var buf [cqEntrySize]byte
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if haveCurrent && logicalOffset <= cutoffOffset && currentSeg != latestSegID {
					if _, ok := seen[currentSeg]; !ok {
						candidates = append(candidates, currentSeg)
					}
				}
				return candidates, nil
			}
			return nil, err
		}
		segID := binary.BigEndian.Uint32(buf[0:4])
		if !haveCurrent {
			currentSeg = segID
			haveCurrent = true
			logicalOffset++
			continue
		}
		if segID != currentSeg {
			if currentSeg != latestSegID {
				if logicalOffset <= cutoffOffset {
					if _, ok := seen[currentSeg]; !ok {
						seen[currentSeg] = struct{}{}
						candidates = append(candidates, currentSeg)
					}
				}
			}
			if logicalOffset >= cutoffOffset {
				return candidates, nil
			}
			currentSeg = segID
		}
		logicalOffset++
	}
}

func (w *WALStorage) segmentEligibleForPrune(path string, cutoff time.Time) ([]Message, bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil && info.Size() <= walHeaderSize {
		return nil, false, nil
	}

	if err := readWALHeader(f); err != nil {
		if err == io.EOF {
			return nil, false, nil
		}
		return nil, false, err
	}

	msgs := make([]Message, 0)
	for {
		msg, ok, err := readRecord(f)
		if err != nil {
			if errors.Is(err, errBadCRC) || errors.Is(err, errCorruptRecord) {
				return nil, false, err
			}
			return nil, false, err
		}
		if !ok {
			break
		}
		if msg.Timestamp.IsZero() || msg.Timestamp.After(cutoff) {
			return nil, false, nil
		}
		msgs = append(msgs, msg)
	}
	if len(msgs) == 0 {
		return nil, false, nil
	}
	return msgs, true, nil
}

func (w *WALStorage) currentPruneObserver() func(topic string, queueID int, msg Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pruneObserver
}

// PruneConsumedSegments deletes sealed segments whose consumequeue entries are all before cutoffOffset.
func (w *WALStorage) PruneConsumedSegments(topic string, queueID int, cutoffOffset int64) (int, error) {
	if cutoffOffset <= 0 || isRetentionExemptTopic(topic) {
		return 0, nil
	}
	deleted := 0
	err := w.withTopicCompaction(topic, queueID, func() error {
		if err := w.FlushTopic(topic, queueID); err != nil {
			return err
		}

		w.mu.Lock()
		base, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
		w.mu.Unlock()
		if err != nil {
			return err
		}
		cqPath := w.consumeQueueFile(topic, queueID)
		info, err := os.Stat(cqPath)
		tail := base
		if err == nil {
			tail += info.Size() / cqEntrySize
		} else if !os.IsNotExist(err) {
			return err
		}
		if cutoffOffset > tail {
			cutoffOffset = tail
		}
		if cutoffOffset <= base {
			return nil
		}

		segs, err := w.listTopicSegments(topic, queueID)
		if err != nil {
			return err
		}
		if len(segs) == 0 {
			return nil
		}

		if cutoffOffset >= tail {
			hasMessages, err := segmentHasMessages(segs[len(segs)-1])
			if err != nil && !os.IsNotExist(err) {
				return err
			}
			if err == nil && hasMessages {
				if err := w.rotateSegment(topic, queueID); err != nil {
					return err
				}
				if err := w.FlushTopic(topic, queueID); err != nil {
					return err
				}
				segs, err = w.listTopicSegments(topic, queueID)
				if err != nil {
					return err
				}
				if len(segs) == 0 {
					return nil
				}
			}
		}

		latestSegID, err := parseSegmentIDFromPath(segs[len(segs)-1])
		if err != nil {
			return err
		}
		candidates, err := w.consumedSegmentCandidates(topic, queueID, base, cutoffOffset, latestSegID)
		if err != nil {
			return err
		}
		if len(candidates) == 0 {
			return nil
		}

		candidateSet := make(map[uint32]struct{}, len(candidates))
		for _, segID := range candidates {
			candidateSet[segID] = struct{}{}
		}
		commitDir := w.commitLogDir(topic, queueID)
		for _, seg := range segs[:len(segs)-1] {
			segID, err := parseSegmentIDFromPath(seg)
			if err != nil {
				return err
			}
			if _, ok := candidateSet[segID]; !ok {
				continue
			}
			if err := os.Remove(seg); err != nil && !os.IsNotExist(err) {
				return err
			}
			deleted++
		}
		if deleted > 0 {
			if err := syncDir(commitDir); err != nil {
				return err
			}
		}
		if err := w.trimConsumeQueueHead(topic, queueID); err != nil {
			return err
		}
		w.mu.Lock()
		newBase, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if newBase > base {
			if _, err := w.PruneDeliveryEvents(deliveryEventLogicalTopic(topic), topic, queueID, newBase); err != nil {
				return err
			}
		}
		return nil
	})
	return deleted, err
}

// Compact no longer rewrites commitlog segments because consumequeue entries retain
// physical segment positions. Safe retention cleanup happens via PruneExpiredSegments.
func (w *WALStorage) Compact(topic string, queueID int) error {
	return w.FlushTopic(topic, queueID)
}

// PruneExpiredSegments deletes sealed segments whose records all fall before the cutoff.
// Consumequeue entries are left intact, so group offsets remain valid and reads simply skip
// any missing historical segments.
func (w *WALStorage) PruneExpiredSegments(topic string, queueID int, cutoff time.Time) error {
	if cutoff.IsZero() {
		return nil
	}
	if isRetentionExemptTopic(topic) {
		return nil
	}
	return w.withTopicCompaction(topic, queueID, func() error {
		if err := w.FlushTopic(topic, queueID); err != nil {
			return err
		}
		w.mu.Lock()
		baseBefore, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
		w.mu.Unlock()
		if err != nil {
			return err
		}

		segs, err := w.listTopicSegments(topic, queueID)
		if err != nil {
			return err
		}
		if len(segs) == 0 {
			return nil
		}

		latestMessages, latestEligible, err := w.segmentEligibleForPrune(segs[len(segs)-1], cutoff)
		if err == nil && latestEligible && len(latestMessages) > 0 {
			if err := w.rotateSegment(topic, queueID); err != nil {
				return err
			}
			if err := w.FlushTopic(topic, queueID); err != nil {
				return err
			}
			segs, err = w.listTopicSegments(topic, queueID)
			if err != nil {
				return err
			}
		}
		if len(segs) <= 1 {
			return nil
		}

		observer := w.currentPruneObserver()
		commitDir := w.commitLogDir(topic, queueID)
		changed := false
		for _, seg := range segs[:len(segs)-1] {
			msgs, eligible, err := w.segmentEligibleForPrune(seg, cutoff)
			if err != nil {
				return err
			}
			if !eligible {
				continue
			}
			for _, msg := range msgs {
				if observer == nil {
					continue
				}
				if err := observer(topic, queueID, msg); err != nil {
					return err
				}
			}
			if err := os.Remove(seg); err != nil && !os.IsNotExist(err) {
				return err
			}
			changed = true
		}
		if changed {
			if err := syncDir(commitDir); err != nil {
				return err
			}
		}
		if err := w.trimConsumeQueueHead(topic, queueID); err != nil {
			return err
		}
		w.mu.Lock()
		baseAfter, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
		w.mu.Unlock()
		if err != nil {
			return err
		}
		if baseAfter > baseBefore {
			if _, err := w.PruneDeliveryEvents(deliveryEventLogicalTopic(topic), topic, queueID, baseAfter); err != nil {
				return err
			}
		}
		return nil
	})
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

// ConsumeQueueDepth returns the logical tail offset of the consumequeue for a topic/queue.
func (w *WALStorage) ConsumeQueueDepth(topic string, queueID int) int64 {
	w.waitForTopicReady(topic, queueID)
	if err := w.FlushTopic(topic, queueID); err != nil {
		return 0
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	cqPath := w.consumeQueueFile(topic, queueID)
	info, err := os.Stat(cqPath)
	if err != nil {
		return base
	}
	return base + info.Size()/cqEntrySize
}

// CommitLogBytes returns the total size in bytes of all commitlog segments for a topic/queue.
func (w *WALStorage) CommitLogBytes(topic string, queueID int) int64 {
	dir := w.commitLogDir(topic, queueID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}

// SetCompactThreshold sets the byte threshold to trigger segment rotation.
func (w *WALStorage) SetCompactThreshold(n int64) {
	w.mu.Lock()
	w.compactThreshold = n
	w.mu.Unlock()
}

// SetRetentionWindow configures how far back sealed segments are retained before pruning.
// A non-positive duration disables retention pruning.
func (w *WALStorage) SetRetentionWindow(d time.Duration) {
	w.mu.Lock()
	w.retentionWindow = d
	w.mu.Unlock()
}

// SetPruneObserver registers a callback invoked for every message in a segment just before
// that segment is pruned.
func (w *WALStorage) SetPruneObserver(observer func(topic string, queueID int, msg Message) error) {
	w.mu.Lock()
	w.pruneObserver = observer
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

// compactor periodically sweeps sealed segments that have fully aged past the retention window.
func (w *WALStorage) compactor() {
	defer w.wg.Done()
	t := time.NewTicker(w.compactInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			w.mu.Lock()
			retentionWindow := w.retentionWindow
			w.mu.Unlock()
			if retentionWindow <= 0 {
				continue
			}
			cutoff := time.Now().Add(-retentionWindow)
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
					_ = w.PruneExpiredSegments(topic, qid, cutoff)
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
	w.waitForTopicReady(topic, queueID)
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, offset, err
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	if offset < base {
		offset = base
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
	start := (offset - base) * cqEntrySize
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
	w.waitForTopicReady(topic, queueID)
	if err := w.FlushTopic(topic, queueID); err != nil {
		return nil, offset, err
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	if offset < base {
		offset = base
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
	start := (offset - base) * cqEntrySize
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
