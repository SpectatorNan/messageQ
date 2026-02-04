package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// ProcessingRecord persists in-flight processing state for crash recovery.
type ProcessingRecord struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	QueueID   int       `json:"queue_id"`
	MsgID     string    `json:"msg_id"`
	Body      string    `json:"body"`
	Tag       string    `json:"tag"`
	Retry     int       `json:"retry"`
	Timestamp time.Time `json:"timestamp"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SaveProcessing persists an in-flight processing record.
func (w *WALStorage) SaveProcessing(rec ProcessingRecord) error {
	if rec.Group == "" || rec.Topic == "" || rec.MsgID == "" {
		return fmt.Errorf("invalid processing record")
	}
	dir := filepath.Join(w.baseDir, "processing", rec.Group, rec.Topic, strconv.Itoa(rec.QueueID))
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

// RemoveProcessing removes a persisted processing record.
func (w *WALStorage) RemoveProcessing(group, topic string, queueID int, msgID string) error {
	if group == "" || topic == "" || msgID == "" {
		return fmt.Errorf("invalid processing key")
	}
	path := filepath.Join(w.baseDir, "processing", group, topic, strconv.Itoa(queueID), msgID+".json")
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return nil
}

// LoadProcessing loads all persisted processing records.
func (w *WALStorage) LoadProcessing() ([]ProcessingRecord, error) {
	base := filepath.Join(w.baseDir, "processing")
	var out []ProcessingRecord
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
		var rec ProcessingRecord
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
