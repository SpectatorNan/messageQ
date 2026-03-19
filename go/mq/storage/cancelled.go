package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CancelledRecord persists message termination state for a topic/message.
type CancelledRecord struct {
	Group         string     `json:"group"`
	Topic         string     `json:"topic"`
	MsgID         string     `json:"msg_id"`
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
		out = append(out, rec)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
