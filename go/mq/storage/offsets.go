package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CommitOffset persists the consumer group offset for a topic/queue.
func (w *WALStorage) CommitOffset(group, topic string, queueID int, offset int64) error {
	if group == "" || topic == "" {
		return fmt.Errorf("invalid group or topic")
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	if offset < base {
		offset = base
	}
	dir := filepath.Join(w.baseDir, "offsets", group, topic)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	finalPath := filepath.Join(dir, fmt.Sprintf("%d.offset", queueID))
	tmpPath := finalPath + ".tmp"
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(offset))
	if err := os.WriteFile(tmpPath, buf[:], 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmpPath, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	_ = syncDir(dir)
	return nil
}

// GetOffset reads the consumer group offset for a topic/queue.
func (w *WALStorage) GetOffset(group, topic string, queueID int) (int64, bool, error) {
	if group == "" || topic == "" {
		return 0, false, fmt.Errorf("invalid group or topic")
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	path := filepath.Join(w.baseDir, "offsets", group, topic, fmt.Sprintf("%d.offset", queueID))
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if len(b) < 8 {
		return 0, false, fmt.Errorf("invalid offset file")
	}
	offset := int64(binary.BigEndian.Uint64(b[:8]))
	if offset < base {
		offset = base
	}
	return offset, true, nil
}

// ListOffsetsByTopicQueue loads all persisted offsets for a topic/queue across groups.
func (w *WALStorage) ListOffsetsByTopicQueue(topic string, queueID int) (map[string]int64, error) {
	if topic == "" {
		return nil, fmt.Errorf("invalid topic")
	}
	base := w.ConsumeQueueBaseOffset(topic, queueID)
	root := filepath.Join(w.baseDir, "offsets")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]int64{}, nil
		}
		return nil, err
	}

	out := make(map[string]int64)
	filename := fmt.Sprintf("%d.offset", queueID)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		group := strings.TrimSpace(entry.Name())
		if group == "" {
			continue
		}
		path := filepath.Join(root, group, topic, filename)
		b, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		if len(b) < 8 {
			return nil, fmt.Errorf("invalid offset file for group %s topic %s queue %d", group, topic, queueID)
		}
		offset := int64(binary.BigEndian.Uint64(b[:8]))
		if offset < base {
			offset = base
		}
		out[group] = offset
	}
	return out, nil
}
