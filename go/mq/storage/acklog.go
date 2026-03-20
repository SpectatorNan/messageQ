package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DeliveryEventProcessing = "processing"
	DeliveryEventAck        = "ack"
	DeliveryEventRetry      = "retry"
	DeliveryEventCancelled  = "cancelled"
	DeliveryEventExpired    = "expired"
	DeliveryEventDLQ        = "dlq"
)

const deliveryLogTopicScope = "__topic__"
const deliveryLogAllQueues = "all.jsonl"
const deliveryLogScanBuffer = 16 * 1024 * 1024
const deliveryLogRetryMarker = ".retry."

// DeliveryEventRecord persists consumer lifecycle events for future replay and cleanup work.
type DeliveryEventRecord struct {
	Event         string     `json:"event"`
	Group         string     `json:"group,omitempty"`
	Topic         string     `json:"topic"`
	StorageTopic  string     `json:"storage_topic,omitempty"`
	QueueID       *int       `json:"queue_id,omitempty"`
	MsgID         string     `json:"msg_id"`
	Offset        *int64     `json:"offset,omitempty"`
	NextOffset    *int64     `json:"next_offset,omitempty"`
	Body          string     `json:"body,omitempty"`
	Tag           string     `json:"tag,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Retry         int        `json:"retry"`
	Timestamp     time.Time  `json:"timestamp"`
	ScheduledAt   *time.Time `json:"scheduled_at,omitempty"`
	ConsumedAt    *time.Time `json:"consumed_at,omitempty"`
	EventAt       time.Time  `json:"event_at"`
}

// AckRecord persists a completed delivery event for future auditing/watermark work.
type AckRecord struct {
	Group         string    `json:"group"`
	Topic         string    `json:"topic"`
	StorageTopic  string    `json:"storage_topic,omitempty"`
	QueueID       int       `json:"queue_id"`
	MsgID         string    `json:"msg_id"`
	Offset        int64     `json:"offset"`
	NextOffset    int64     `json:"next_offset"`
	Tag           string    `json:"tag,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Retry         int       `json:"retry"`
	Timestamp     time.Time `json:"timestamp"`
	AckedAt       time.Time `json:"acked_at"`
}

func deliveryLogGroupKey(group string) string {
	if strings.TrimSpace(group) == "" {
		return deliveryLogTopicScope
	}
	return group
}

func deliveryLogQueueFile(queueID *int) string {
	if queueID == nil {
		return deliveryLogAllQueues
	}
	return strconv.Itoa(*queueID) + ".jsonl"
}

func (w *WALStorage) deliveryLogPath(group, topic string, queueID *int) string {
	return filepath.Join(w.baseDir, "acklog", deliveryLogGroupKey(group), topic, deliveryLogQueueFile(queueID))
}

func readDeliveryEventsFromFile(path string) ([]DeliveryEventRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var out []DeliveryEventRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), deliveryLogScanBuffer)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var rec DeliveryEventRecord
		if err := json.Unmarshal([]byte(line), &rec); err == nil && rec.Topic != "" && rec.MsgID != "" {
			if rec.Event == "" {
				var legacy AckRecord
				if err := json.Unmarshal([]byte(line), &legacy); err != nil {
					return nil, err
				}
				out = append(out, legacyAckToDeliveryEvent(legacy))
				continue
			}
			normalizeDeliveryEventRecord(&rec)
			out = append(out, rec)
			continue
		}

		var legacy AckRecord
		if err := json.Unmarshal([]byte(line), &legacy); err != nil {
			return nil, err
		}
		out = append(out, legacyAckToDeliveryEvent(legacy))
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeDeliveryEventRecord(rec *DeliveryEventRecord) {
	if rec.Event == "" {
		rec.Event = DeliveryEventAck
	}
	if rec.StorageTopic == "" {
		rec.StorageTopic = rec.Topic
	}
}

func legacyAckToDeliveryEvent(rec AckRecord) DeliveryEventRecord {
	queueID := rec.QueueID
	offset := rec.Offset
	nextOffset := rec.NextOffset
	return DeliveryEventRecord{
		Event:         DeliveryEventAck,
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  rec.StorageTopic,
		QueueID:       &queueID,
		MsgID:         rec.MsgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		EventAt:       rec.AckedAt,
	}
}

func deliveryEventToAck(rec DeliveryEventRecord) (AckRecord, bool) {
	if rec.Event != DeliveryEventAck || rec.QueueID == nil || rec.Offset == nil || rec.NextOffset == nil {
		return AckRecord{}, false
	}
	return AckRecord{
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  rec.StorageTopic,
		QueueID:       *rec.QueueID,
		MsgID:         rec.MsgID,
		Offset:        *rec.Offset,
		NextOffset:    *rec.NextOffset,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		AckedAt:       rec.EventAt,
	}, true
}

// AppendDeliveryEvent appends a consumer lifecycle event to the delivery log.
func (w *WALStorage) AppendDeliveryEvent(rec DeliveryEventRecord) error {
	if rec.Event == "" || rec.Topic == "" || rec.MsgID == "" {
		return fmt.Errorf("invalid delivery event record")
	}
	if rec.EventAt.IsZero() {
		rec.EventAt = time.Now()
	}
	if rec.StorageTopic == "" {
		rec.StorageTopic = rec.Topic
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	dir := filepath.Dir(w.deliveryLogPath(rec.Group, rec.Topic, rec.QueueID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := w.deliveryLogPath(rec.Group, rec.Topic, rec.QueueID)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return syncDir(dir)
}

func deliveryEventRecordOffsetRange(rec DeliveryEventRecord) (int64, int64, bool) {
	if rec.Offset != nil {
		if rec.NextOffset != nil {
			return *rec.Offset, *rec.NextOffset, true
		}
		return *rec.Offset, *rec.Offset + 1, true
	}
	if rec.NextOffset != nil {
		return *rec.NextOffset - 1, *rec.NextOffset, true
	}
	return 0, 0, false
}

func deliveryEventLogicalTopic(storageTopic string) string {
	idx := strings.LastIndex(storageTopic, deliveryLogRetryMarker)
	if idx <= 0 || idx+len(deliveryLogRetryMarker) >= len(storageTopic) {
		return storageTopic
	}
	return storageTopic[:idx]
}

func writeDeliveryEventsFile(path string, records []DeliveryEventRecord) error {
	dir := filepath.Dir(path)
	if len(records) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return syncDir(dir)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	for _, rec := range records {
		b, err := json.Marshal(rec)
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
		b = append(b, '\n')
		if _, err := f.Write(b); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

func (w *WALStorage) pruneDeliveryEventFile(path, storageTopic string, queueID int, cutoffOffset int64) (int, error) {
	records, err := readDeliveryEventsFromFile(path)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, nil
	}
	kept := make([]DeliveryEventRecord, 0, len(records))
	deleted := 0
	for _, rec := range records {
		if rec.StorageTopic != storageTopic {
			kept = append(kept, rec)
			continue
		}
		if rec.QueueID == nil || *rec.QueueID != queueID {
			kept = append(kept, rec)
			continue
		}
		_, nextOffset, ok := deliveryEventRecordOffsetRange(rec)
		if !ok || nextOffset > cutoffOffset {
			kept = append(kept, rec)
			continue
		}
		deleted++
	}
	if deleted == 0 {
		return 0, nil
	}
	if err := writeDeliveryEventsFile(path, kept); err != nil {
		return 0, err
	}
	return deleted, nil
}

// PruneDeliveryEvents removes offset-scoped delivery events that fall fully before a pruned queue prefix.
func (w *WALStorage) PruneDeliveryEvents(logicalTopic, storageTopic string, queueID int, cutoffOffset int64) (int, error) {
	if logicalTopic == "" || storageTopic == "" || cutoffOffset <= 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	root := filepath.Join(w.baseDir, "acklog")
	scopeEntries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	totalDeleted := 0
	for _, scopeEntry := range scopeEntries {
		if !scopeEntry.IsDir() {
			continue
		}
		dir := filepath.Join(root, scopeEntry.Name(), logicalTopic)
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return totalDeleted, err
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".jsonl") {
				continue
			}
			deleted, err := w.pruneDeliveryEventFile(filepath.Join(dir, file.Name()), storageTopic, queueID, cutoffOffset)
			if err != nil {
				return totalDeleted, err
			}
			totalDeleted += deleted
		}
	}
	return totalDeleted, nil
}

// LoadDeliveryEvents loads persisted lifecycle events for a group/topic/queue.
func (w *WALStorage) LoadDeliveryEvents(group, topic string, queueID *int) ([]DeliveryEventRecord, error) {
	if topic == "" {
		return nil, fmt.Errorf("invalid delivery log key")
	}
	return readDeliveryEventsFromFile(w.deliveryLogPath(group, topic, queueID))
}

// ListTopicDeliveryEvents loads topic-scoped delivery events across all queues.
func (w *WALStorage) ListTopicDeliveryEvents(topic, event string, limit int) ([]DeliveryEventRecord, error) {
	if topic == "" {
		return nil, fmt.Errorf("invalid topic")
	}
	if limit <= 0 {
		limit = 50
	}
	dir := filepath.Join(w.baseDir, "acklog", deliveryLogTopicScope, topic)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]DeliveryEventRecord, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		records, err := readDeliveryEventsFromFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			if rec.Group != "" {
				continue
			}
			if event != "" && rec.Event != event {
				continue
			}
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].EventAt.After(out[j].EventAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ListGroupDeliveryEvents loads group-scoped delivery events across all queues.
func (w *WALStorage) ListGroupDeliveryEvents(group, topic, event string, limit int) ([]DeliveryEventRecord, error) {
	if strings.TrimSpace(group) == "" || topic == "" {
		return nil, fmt.Errorf("invalid group or topic")
	}
	if limit <= 0 {
		limit = 50
	}
	dir := filepath.Join(w.baseDir, "acklog", deliveryLogGroupKey(group), topic)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]DeliveryEventRecord, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		records, err := readDeliveryEventsFromFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		for _, rec := range records {
			if rec.Group != group {
				continue
			}
			if event != "" && rec.Event != event {
				continue
			}
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].EventAt.After(out[j].EventAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ListGroupDeliveryTopics returns every group/topic namespace with group-scoped delivery logs.
func (w *WALStorage) ListGroupDeliveryTopics() (map[string][]string, error) {
	root := filepath.Join(w.baseDir, "acklog")
	groupEntries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make(map[string][]string)
	for _, groupEntry := range groupEntries {
		if !groupEntry.IsDir() || groupEntry.Name() == deliveryLogTopicScope {
			continue
		}
		group := groupEntry.Name()
		topicEntries, err := os.ReadDir(filepath.Join(root, group))
		if err != nil {
			return nil, err
		}
		topics := make([]string, 0, len(topicEntries))
		for _, topicEntry := range topicEntries {
			if topicEntry.IsDir() {
				topics = append(topics, topicEntry.Name())
			}
		}
		if len(topics) == 0 {
			continue
		}
		sort.Strings(topics)
		out[group] = topics
	}
	return out, nil
}

// AppendAck appends a completed-delivery event to the ack log.
func (w *WALStorage) AppendAck(rec AckRecord) error {
	queueID := rec.QueueID
	offset := rec.Offset
	nextOffset := rec.NextOffset
	return w.AppendDeliveryEvent(DeliveryEventRecord{
		Event:         DeliveryEventAck,
		Group:         rec.Group,
		Topic:         rec.Topic,
		StorageTopic:  rec.StorageTopic,
		QueueID:       &queueID,
		MsgID:         rec.MsgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Tag:           rec.Tag,
		CorrelationID: rec.CorrelationID,
		Retry:         rec.Retry,
		Timestamp:     rec.Timestamp,
		EventAt:       rec.AckedAt,
	})
}

// LoadAckLog loads persisted ack events for a group/topic/queue.
func (w *WALStorage) LoadAckLog(group, topic string, queueID int) ([]AckRecord, error) {
	qid := queueID
	events, err := w.LoadDeliveryEvents(group, topic, &qid)
	if err != nil {
		return nil, err
	}
	out := make([]AckRecord, 0, len(events))
	for _, event := range events {
		rec, ok := deliveryEventToAck(event)
		if ok {
			out = append(out, rec)
		}
	}
	return out, nil
}
