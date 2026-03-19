package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

const systemDelayTopicName = "__DELAY_TOPIC__"

func (w *WALStorage) closeCommitLogLocked(key string) error {
	if wr, ok := w.writers[key]; ok {
		if err := wr.Flush(); err != nil {
			return err
		}
		delete(w.writers, key)
	}
	if f, ok := w.files[key]; ok {
		if err := f.Sync(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		delete(w.files, key)
	}
	return nil
}

func (w *WALStorage) closeTopicLocked(key string) error {
	if err := w.closeCommitLogLocked(key); err != nil {
		return err
	}
	return w.closeConsumeQueueLocked(key)
}

func removeMatchingFiles(dir string, match func(name string) bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !match(entry.Name()) {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// ReplaceTopicMessages rewrites a topic/queue to contain exactly the provided messages.
// This is used by the delay scheduler snapshot topic to keep only the latest persisted state.
func (w *WALStorage) ReplaceTopicMessages(topic string, queueID int, msgs []Message) error {
	return w.withTopicCompaction(topic, queueID, func() error {
		key := queueKey(topic, queueID)

		w.mu.Lock()
		if err := w.closeTopicLocked(key); err != nil {
			w.mu.Unlock()
			return err
		}
		delete(w.segmentID, key)
		delete(w.segmentPos, key)
		delete(w.bytesSinceCompact, key)
		delete(w.consumeQueueBase, key)
		w.mu.Unlock()

		commitDir := w.commitLogDir(topic, queueID)
		if err := os.MkdirAll(commitDir, 0o755); err != nil {
			return err
		}
		cqDir := w.consumeQueueDir(topic, queueID)
		if err := os.MkdirAll(cqDir, 0o755); err != nil {
			return err
		}

		walTmp := filepath.Join(commitDir, ".rewrite.tmp.wal")
		cqTmp := filepath.Join(cqDir, ".rewrite.tmp.cq")

		walFile, err := os.OpenFile(walTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if err := writeWALHeader(walFile); err != nil {
			_ = walFile.Close()
			return err
		}
		pos := int64(walHeaderSize)

		cqFile, err := os.OpenFile(cqTmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			_ = walFile.Close()
			return err
		}

		for _, msg := range msgs {
			rec, recLen, err := buildRecordBytes(msg)
			if err != nil {
				_ = walFile.Close()
				_ = cqFile.Close()
				return err
			}
			if _, err := walFile.Write(rec); err != nil {
				_ = walFile.Close()
				_ = cqFile.Close()
				return err
			}
			var cqEntry [cqEntrySize]byte
			binary.BigEndian.PutUint32(cqEntry[0:4], 1)
			binary.BigEndian.PutUint64(cqEntry[4:12], uint64(pos))
			binary.BigEndian.PutUint32(cqEntry[12:16], uint32(recLen))
			binary.BigEndian.PutUint32(cqEntry[16:20], hashTag(msg.Tag))
			if _, err := cqFile.Write(cqEntry[:]); err != nil {
				_ = walFile.Close()
				_ = cqFile.Close()
				return err
			}
			pos += recLen
		}

		if err := walFile.Sync(); err != nil {
			_ = walFile.Close()
			_ = cqFile.Close()
			return err
		}
		if err := walFile.Close(); err != nil {
			_ = cqFile.Close()
			return err
		}
		if err := cqFile.Sync(); err != nil {
			_ = cqFile.Close()
			return err
		}
		if err := cqFile.Close(); err != nil {
			return err
		}

		if err := removeMatchingFiles(commitDir, func(name string) bool {
			return name != filepath.Base(walTmp) && filepath.Ext(name) == ".wal"
		}); err != nil {
			return err
		}
		if err := os.Rename(walTmp, filepath.Join(commitDir, "00000001.wal")); err != nil {
			return err
		}
		if err := syncDir(commitDir); err != nil {
			return err
		}

		if err := removeMatchingFiles(cqDir, func(name string) bool {
			return name == consumeQueueBaseOffsetFileName || (name != filepath.Base(cqTmp) && filepath.Ext(name) == ".cq")
		}); err != nil {
			return err
		}
		if err := os.Rename(cqTmp, w.consumeQueueFile(topic, queueID)); err != nil {
			return err
		}
		w.mu.Lock()
		err = w.setConsumeQueueBaseOffsetLocked(topic, queueID, 0)
		w.mu.Unlock()
		if err != nil {
			return err
		}

		w.mu.Lock()
		defer w.mu.Unlock()
		w.consumeQueueBase[key] = 0
		if err := w.ensureTopicLocked(topic, queueID); err != nil {
			return err
		}
		w.segmentID[key] = 1
		w.segmentPos[key] = pos
		w.bytesSinceCompact[key] = 0
		return nil
	})
}

func isRetentionExemptTopic(topic string) bool {
	return topic == systemDelayTopicName
}
