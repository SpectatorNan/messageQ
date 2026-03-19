package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CancelledRecord persists message termination state for a topic/message.
type CancelledRecord struct {
	Group         string     `json:"group"`
	Topic         string     `json:"topic"`
	StorageTopic  string     `json:"storage_topic,omitempty"`
	MsgID         string     `json:"msg_id"`
	State         string     `json:"state,omitempty"`
	Body          string     `json:"body,omitempty"`
	Tag           string     `json:"tag,omitempty"`
	CorrelationID string     `json:"correlation_id,omitempty"`
	Retry         int        `json:"retry"`
	Timestamp     time.Time  `json:"timestamp"`
	QueueID       *int       `json:"queue_id,omitempty"`
	Offset        *int64     `json:"offset,omitempty"`
	NextOffset    *int64     `json:"next_offset,omitempty"`
	ScheduledAt   *time.Time `json:"scheduled_at,omitempty"`
	ConsumedAt    *time.Time `json:"consumed_at,omitempty"`
	CancelledAt   time.Time  `json:"cancelled_at"`
}

func normalizeCancelledRecord(rec *CancelledRecord) {
	if rec.State == "" {
		rec.State = "cancelled"
	}
	if rec.StorageTopic == "" {
		rec.StorageTopic = rec.Topic
	}
}

// SaveCancelled persists a cancelled record.
func (w *WALStorage) SaveCancelled(rec CancelledRecord) error {
	if rec.Topic == "" || rec.MsgID == "" {
		return fmt.Errorf("invalid cancelled record")
	}
	dir := filepath.Join(w.baseDir, "cancelled", rec.Topic)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, rec.MsgID+".json")
	tmp := path + ".tmp"
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmp, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	_ = syncDir(dir)
	return nil
}

// LoadCancelled loads all persisted cancelled records.
func (w *WALStorage) LoadCancelled() ([]CancelledRecord, error) {
	base := filepath.Join(w.baseDir, "cancelled")
	var out []CancelledRecord
	info, err := os.Stat(base)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return out, nil
	}

	err = filepath.Walk(base, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var rec CancelledRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return err
		}
		normalizeCancelledRecord(&rec)
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LoadCancelledByID loads a persisted terminal record for a topic/message.
func (w *WALStorage) LoadCancelledByID(topic, msgID string) (CancelledRecord, bool, error) {
	if topic == "" || msgID == "" {
		return CancelledRecord{}, false, fmt.Errorf("invalid cancelled key")
	}
	path := filepath.Join(w.baseDir, "cancelled", topic, msgID+".json")
	b, err := os.ReadFile(path)
	if err == nil {
		var rec CancelledRecord
		if err := json.Unmarshal(b, &rec); err != nil {
			return CancelledRecord{}, false, err
		}
		normalizeCancelledRecord(&rec)
		return rec, true, nil
	}
	if !os.IsNotExist(err) {
		return CancelledRecord{}, false, err
	}

	var match CancelledRecord
	found := false
	stopWalk := errors.New("stop walk")
	base := filepath.Join(w.baseDir, "cancelled")
	walkErr := filepath.Walk(base, func(candidate string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info == nil || info.IsDir() || info.Name() != msgID+".json" {
			return nil
		}
		payload, readErr := os.ReadFile(candidate)
		if readErr != nil {
			return readErr
		}
		var rec CancelledRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return err
		}
		normalizeCancelledRecord(&rec)
		if rec.Topic == topic && rec.MsgID == msgID {
			match = rec
			found = true
			return stopWalk
		}
		return nil
	})
	if walkErr != nil {
		if errors.Is(walkErr, stopWalk) {
			return match, found, nil
		}
		if os.IsNotExist(walkErr) {
			return CancelledRecord{}, false, nil
		}
		return CancelledRecord{}, false, walkErr
	}
	return match, found, nil
}

// ListCancelledByTopic lists the latest persisted terminal records for a topic/state.
func (w *WALStorage) ListCancelledByTopic(topic, state string, limit int) ([]CancelledRecord, error) {
	if topic == "" {
		return nil, fmt.Errorf("invalid topic")
	}
	if limit <= 0 {
		limit = 50
	}
	records, err := w.LoadCancelled()
	if err != nil {
		return nil, err
	}
	entries := make([]CancelledRecord, 0, len(records))
	for _, rec := range records {
		if rec.Topic != topic {
			continue
		}
		if state != "" && rec.State != state {
			continue
		}
		entries = append(entries, rec)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CancelledAt.After(entries[j].CancelledAt)
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

// DeleteCancelled removes a persisted terminal record for a topic/message.
func (w *WALStorage) DeleteCancelled(topic, msgID string) error {
	if topic == "" || msgID == "" {
		return fmt.Errorf("invalid cancelled key")
	}
	path := filepath.Join(w.baseDir, "cancelled", topic, msgID+".json")
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	dir := filepath.Dir(path)
	_ = syncDir(dir)
	return nil
}
