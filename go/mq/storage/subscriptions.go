package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SubscriptionRecord persists the normalized subscription tag for a consumer group/topic.
type SubscriptionRecord struct {
	Group     string    `json:"group"`
	Topic     string    `json:"topic"`
	Tag       string    `json:"tag"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// SaveSubscription persists a subscription record.
func (w *WALStorage) SaveSubscription(rec SubscriptionRecord) error {
	if rec.Group == "" || rec.Topic == "" || rec.Tag == "" {
		return fmt.Errorf("invalid subscription record")
	}
	dir := filepath.Join(w.baseDir, "subscriptions", rec.Group)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(dir, rec.Topic+".json")
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

// LoadSubscriptions loads all persisted subscription records.
func (w *WALStorage) LoadSubscriptions() ([]SubscriptionRecord, error) {
	base := filepath.Join(w.baseDir, "subscriptions")
	var out []SubscriptionRecord
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
		var rec SubscriptionRecord
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
