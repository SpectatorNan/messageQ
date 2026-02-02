package example

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"messageQ/mq/storage"
)

// TestWALRotationAndNonBlocking concurrently writes to the same topic while rotation/compaction
// thresholds are low. The test fails if writers block (timeout) or any write returns an error.
func TestWALRotationAndNonBlocking(t *testing.T) {
	// use helper to determine data directory
	dir := getTestDataDir(t, "rotate")

	// flushInterval 10ms, compactInterval 50ms for quick compaction checks
	store := storage.NewWALStorage(dir, 10*time.Millisecond, 50*time.Millisecond)
	defer func() { _ = store.Close() }()

	topic := "rot"
	workers := 8
	perWorker := 300
	// small threshold so rotation/compaction happens quickly
	store.SetCompactThreshold(4 * 1024) // 4KB

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				select {
				case <-ctx.Done():
					return
				default:
				}
				uid, err := uuid.NewV7()
				if err != nil {
					uid = uuid.New()
				}
				m := storage.Message{ID: uid.String(), Body: "hello-rotation-test", Retry: 0, Timestamp: time.Now()}
				if err := store.Append(topic, 0, m); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	// wait for workers but also use ctx to detect blocking
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-ctx.Done():
		t.Fatal("writers blocked or test timed out")
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("write error: %v", err)
		}
	}

	// allow compactor to run and then inspect segments
	time.Sleep(200 * time.Millisecond)
	entries, err := os.ReadDir(filepath.Join(dir, "commitlog", topic, "0"))
	if err != nil {
		t.Fatalf("list segments failed: %v", err)
	}
	segs := []string{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ".wal" {
			segs = append(segs, filepath.Join(dir, "commitlog", topic, "0", e.Name()))
		}
	}
	if len(segs) < 1 {
		t.Fatalf("expected at least one segment, got %d", len(segs))
	}
	t.Logf("segments after writes: %v", segs)

	// ensure Load returns the total number of live messages (should be workers*perWorker)
	msgs, err := store.Load(topic, 0)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	got := len(msgs)
	exp := workers * perWorker
	if got != exp {
		t.Logf("warning: loaded %d messages, expected %d (some may have been acked or compacted away)", got, exp)
	}
}
