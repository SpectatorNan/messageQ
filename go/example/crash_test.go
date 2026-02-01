package example

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"messageQ/mq/storage"
)

// TestPartialWriteIgnored simulates a partially-written record at the end of a segment
// and verifies that Load ignores the incomplete record and returns only complete ones.
func TestPartialWriteIgnored(t *testing.T) {
	dir := t.TempDir()
	topic := "crash1"
	topicDir := filepath.Join(dir, topic)
	_ = os.MkdirAll(topicDir, 0o755)
	p := filepath.Join(topicDir, "00000001.wal")
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	defer f.Close()

	// write one complete record (LogEntry JSON)
	m1 := storage.Message{ID: 1, Body: "hello", Retry: 0, Timestamp: time.Now()}
	entry1 := storage.LogEntry{Type: storage.LogProduce, Topic: topic, Msg: m1}
	b1, _ := json.Marshal(entry1)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b1)))
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write len: %v", err)
	}
	if _, err := f.Write([]byte{byte(storage.LogProduce)}); err != nil {
		t.Fatalf("write type: %v", err)
	}
	if _, err := f.Write(b1); err != nil {
		t.Fatalf("write payload: %v", err)
	}

	// write a partial record (length says 100 but only write 10 bytes)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(100))
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write partial len: %v", err)
	}
	if _, err := f.Write([]byte{byte(storage.LogProduce)}); err != nil {
		t.Fatalf("write partial type: %v", err)
	}
	partial := []byte("partial123")
	if _, err := f.Write(partial); err != nil {
		t.Fatalf("write partial payload: %v", err)
	}
	f.Sync()
	f.Close()

	// open storage and load
	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()
	msgs, err := store.Load(topic)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != 1 {
		t.Fatalf("expected only complete record id=1, got: %#v", msgs)
	}
}

// TestLeftoverTmpIgnored ensures that the presence of .compact.tmp does not confuse recovery.
func TestLeftoverTmpIgnored(t *testing.T) {
	dir := t.TempDir()
	topic := "crash2"
	topicDir := filepath.Join(dir, topic)
	_ = os.MkdirAll(topicDir, 0o755)

	// create two segments with records
	for si := 1; si <= 2; si++ {
		seg := filepath.Join(topicDir, filesystemSegmentName(si))
		f, err := os.OpenFile(seg, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			t.Fatalf("open seg %d: %v", si, err)
		}
		m := storage.Message{ID: int64(si), Body: "m", Retry: 0, Timestamp: time.Now()}
		entry := storage.LogEntry{Type: storage.LogProduce, Topic: topic, Msg: m}
		b, _ := json.Marshal(entry)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			t.Fatalf("write len: %v", err)
		}
		if _, err := f.Write([]byte{byte(storage.LogProduce)}); err != nil {
			t.Fatalf("write type: %v", err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatalf("write payload: %v", err)
		}
		f.Sync()
		f.Close()
	}

	// create a leftover .compact.tmp with some content (should be ignored by listTopicSegments)
	tmp := filepath.Join(topicDir, ".compact.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("create tmp: %v", err)
	}
	f.Write([]byte("garbage"))
	f.Sync()
	f.Close()

	store := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store.Close()
	msgs, err := store.Load(topic)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages from segments, got %d", len(msgs))
	}
}

// TestCloseFlushes ensures Close flushes buffered writes to disk so a reopened store can recover them.
func TestCloseFlushes(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewWALStorage(dir, 100*time.Millisecond)
	// append without AppendSync
	err := store.Append("t", storage.Message{ID: 10, Body: "persist", Retry: 0, Timestamp: time.Now()})
	if err != nil {
		t.Fatalf("append failed: %v", err)
	}
	// Close should flush and sync
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	// reopen
	store2 := storage.NewWALStorage(dir, 10*time.Millisecond)
	defer store2.Close()
	msgs, err := store2.Load("t")
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != 10 {
		t.Fatalf("expected recovered msg id=10, got %#v", msgs)
	}
}

// filesystemSegmentName returns zero-padded segment file name for index
func filesystemSegmentName(i int) string {
	return fmt.Sprintf("%08d.wal", i)
}
