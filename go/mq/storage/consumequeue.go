package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const consumeQueueBaseOffsetFileName = "base.offset"
const consumeQueueTrimPrefix = "00000001.cq.trim."
const consumeQueueTrimSuffix = ".tmp"

func (w *WALStorage) consumeQueueBaseOffsetFile(topic string, queueID int) string {
	return filepath.Join(w.consumeQueueDir(topic, queueID), consumeQueueBaseOffsetFileName)
}

func (w *WALStorage) ensureTopicCondLocked(key string) {
	if _, ok := w.topicCond[key]; !ok {
		w.topicCond[key] = sync.NewCond(&w.mu)
	}
}

func (w *WALStorage) waitForTopicReady(topic string, queueID int) {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	w.ensureTopicCondLocked(key)
	for w.topicCompacting[key] {
		w.topicCond[key].Wait()
	}
	w.mu.Unlock()
}

func (w *WALStorage) getConsumeQueueBaseOffsetLocked(topic string, queueID int) (int64, error) {
	key := queueKey(topic, queueID)
	if base, ok := w.consumeQueueBase[key]; ok {
		return base, nil
	}
	path := w.consumeQueueBaseOffsetFile(topic, queueID)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			w.consumeQueueBase[key] = 0
			return 0, nil
		}
		return 0, err
	}
	if len(b) < 8 {
		return 0, fmt.Errorf("invalid consumequeue base offset file")
	}
	base := int64(binary.BigEndian.Uint64(b[:8]))
	w.consumeQueueBase[key] = base
	return base, nil
}

func (w *WALStorage) setConsumeQueueBaseOffsetLocked(topic string, queueID int, base int64) error {
	dir := w.consumeQueueDir(topic, queueID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	path := w.consumeQueueBaseOffsetFile(topic, queueID)
	tmpPath := path + ".tmp"
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(base))
	if err := os.WriteFile(tmpPath, buf[:], 0o644); err != nil {
		return err
	}
	if f, err := os.OpenFile(tmpPath, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	w.consumeQueueBase[queueKey(topic, queueID)] = base
	return nil
}

func (w *WALStorage) ConsumeQueueBaseOffset(topic string, queueID int) int64 {
	w.waitForTopicReady(topic, queueID)
	if err := w.FlushTopic(topic, queueID); err != nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	base, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
	if err != nil {
		return 0
	}
	return base
}

func parseConsumeQueueTrimTarget(name string) (int64, bool) {
	if !strings.HasPrefix(name, consumeQueueTrimPrefix) || !strings.HasSuffix(name, consumeQueueTrimSuffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, consumeQueueTrimPrefix), consumeQueueTrimSuffix)
	target, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return target, true
}

func (w *WALStorage) recoverConsumeQueueTrimLocked(topic string, queueID int) error {
	dir := w.consumeQueueDir(topic, queueID)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	base, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
	if err != nil {
		return err
	}
	cqPath := w.consumeQueueFile(topic, queueID)
	changed := false
	for _, entry := range entries {
		targetBase, ok := parseConsumeQueueTrimTarget(entry.Name())
		if !ok {
			continue
		}
		tmpPath := filepath.Join(dir, entry.Name())
		if base >= targetBase {
			if err := os.Rename(tmpPath, cqPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		} else {
			if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
		changed = true
	}
	if changed {
		return syncDir(dir)
	}
	return nil
}

func (w *WALStorage) closeConsumeQueueLocked(key string) error {
	if cqw, ok := w.cqWriters[key]; ok {
		if err := cqw.Flush(); err != nil {
			return err
		}
		delete(w.cqWriters, key)
	}
	if cf, ok := w.cqFiles[key]; ok {
		if err := cf.Sync(); err != nil {
			return err
		}
		if err := cf.Close(); err != nil {
			return err
		}
		delete(w.cqFiles, key)
	}
	return nil
}

func (w *WALStorage) trimConsumeQueueHead(topic string, queueID int) error {
	key := queueKey(topic, queueID)
	w.mu.Lock()
	base, err := w.getConsumeQueueBaseOffsetLocked(topic, queueID)
	if err != nil {
		w.mu.Unlock()
		return err
	}
	if err := w.flushTopicLocked(key); err != nil {
		w.mu.Unlock()
		return err
	}
	if err := w.closeConsumeQueueLocked(key); err != nil {
		w.mu.Unlock()
		return err
	}
	cqPath := w.consumeQueueFile(topic, queueID)
	commitDir := w.commitLogDir(topic, queueID)
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		_ = w.ensureConsumeQueueLocked(topic, queueID)
		w.mu.Unlock()
	}()

	cf, err := os.Open(cqPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer cf.Close()

	trimEntries := int64(0)
	for {
		var buf [cqEntrySize]byte
		if _, err := io.ReadFull(cf, buf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return err
		}
		segID := binary.BigEndian.Uint32(buf[0:4])
		segPath := filepath.Join(commitDir, fmt.Sprintf("%08d.wal", segID))
		if _, err := os.Stat(segPath); err == nil {
			break
		} else if !os.IsNotExist(err) {
			return err
		}
		trimEntries++
	}
	if trimEntries == 0 {
		return nil
	}

	newBase := base + trimEntries
	tmpPath := cqPath + ".trim." + strconv.FormatInt(newBase, 10) + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := cf.Seek(trimEntries*cqEntrySize, io.SeekStart); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if _, err := io.Copy(tmpFile, cf); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return err
	}
	if err := tmpFile.Close(); err != nil {
		return err
	}

	w.mu.Lock()
	err = w.setConsumeQueueBaseOffsetLocked(topic, queueID, newBase)
	w.mu.Unlock()
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, cqPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(cqPath))
}
