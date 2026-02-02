package example

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"messageQ/mq/storage"
)

func writeMessages(store *storage.WALStorage, topic string, count int, bodySize int, syncWrite bool) error {
	for i := 0; i < count; i++ {
		uid, err := uuid.NewV7()
		if err != nil {
			uid = uuid.New()
		}
		msg := storage.Message{
			ID:        uid.String(),
			Body:      string(make([]byte, bodySize)),
			Retry:     0,
			Timestamp: time.Now(),
		}
		if syncWrite {
			if err := store.AppendSync(topic, 0, msg); err != nil {
				return err
			}
		} else {
			if err := store.Append(topic, 0, msg); err != nil {
				return err
			}
		}
	}
	return nil
}

func walFileSize(dir string, topic string) (int64, error) {
	p := filepath.Join(dir, "commitlog", topic, "0", "00000001.wal")
	s, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	return s.Size(), nil
}

func cleanupDir(dir string) error {
	return os.RemoveAll(dir)
}

func BenchmarkWAL_Intervals(b *testing.B) {
	cases := []struct {
		name  string
		flush time.Duration
	}{
		{"sync", 100 * time.Millisecond}, // keep flushInterval large but use AppendSync for true sync case
		{"10ms", 10 * time.Millisecond},
		{"100ms", 100 * time.Millisecond},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			dir := filepath.Join("./benchdata", c.name)
			_ = cleanupDir(dir)
			if cleanTestData {
				defer cleanupDir(dir)
			}
			store := storage.NewWALStorage(dir, c.flush, 10*time.Minute)
			defer store.Close()

			// choose workload
			count := 1000
			bodySize := 128 // bytes
			syncWrite := c.name == "sync"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writeMessages(store, "bench", count, bodySize, syncWrite); err != nil {
					b.Fatalf("write failed: %v", err)
				}
			}
			b.StopTimer()
			_ = store.Flush()
			sz, _ := walFileSize(dir, "bench")
			b.ReportMetric(float64(sz), "wal_bytes")
		})
	}
}
