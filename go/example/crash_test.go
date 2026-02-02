package example

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"messageQ/mq/storage"
)

// buildRecordBytes for tests (matches wal.go format)
func buildRecordBytes(typ storage.LogType, msg storage.Message) []byte {
	body := []byte(msg.Body)
	if typ != storage.LogProduce {
		body = nil
	}
	ts := msg.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	crc := uint32(0)
	if len(body) > 0 {
		crc = crc32.ChecksumIEEE(body)
	}
	uid, err := uuid.Parse(msg.ID)
	if err != nil {
		uid = uuid.Nil
	}
	totalSize := uint32(4 + 1 + 16 + 2 + 8 + 4 + len(body))
	buf := make([]byte, 4+totalSize)
	binary.BigEndian.PutUint32(buf[0:4], totalSize)
	off := 4
	binary.BigEndian.PutUint32(buf[off:off+4], crc)
	off += 4
	buf[off] = byte(typ)
	off++
	copy(buf[off:off+16], uid[:])
	off += 16
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(msg.Retry))
	off += 2
	binary.BigEndian.PutUint64(buf[off:off+8], uint64(ts.UnixNano()))
	off += 8
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(body)))
	off += 4
	copy(buf[off:], body)
	return buf
}

// writeWALHeader writes the magic+version header used by WAL.
func writeWALHeader(w io.Writer) error {
	var hdr [8]byte
	copy(hdr[0:4], []byte("MQW1"))
	binary.BigEndian.PutUint16(hdr[4:6], 1)
	_, err := w.Write(hdr[:])
	return err
}

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

	if err := writeWALHeader(f); err != nil {
		t.Fatalf("write header: %v", err)
	}

	// write one complete record
	uid := uuid.New()
	m1 := storage.Message{ID: uid.String(), Body: "hello", Retry: 0, Timestamp: time.Now()}
	rec1 := buildRecordBytes(storage.LogProduce, m1)
	if _, err := f.Write(rec1); err != nil {
		t.Fatalf("write record: %v", err)
	}

	// write a partial record (length says 100 but only write 10 bytes of payload)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(100))
	if _, err := f.Write(lenBuf[:]); err != nil {
		t.Fatalf("write partial len: %v", err)
	}
	// write only a small part of the payload bytes
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
	if len(msgs) != 1 || msgs[0].ID != uid.String() {
		t.Fatalf("expected only complete record id=%s, got: %#v", uid.String(), msgs)
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
		if err := writeWALHeader(f); err != nil {
			f.Close()
			t.Fatalf("write header: %v", err)
		}
		uid := uuid.New()
		m := storage.Message{ID: uid.String(), Body: "m", Retry: 0, Timestamp: time.Now()}
		rec := buildRecordBytes(storage.LogProduce, m)
		if _, err := f.Write(rec); err != nil {
			t.Fatalf("write record: %v", err)
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
	uid := uuid.New()
	err := store.Append("t", storage.Message{ID: uid.String(), Body: "persist", Retry: 0, Timestamp: time.Now()})
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
	if len(msgs) != 1 || msgs[0].ID != uid.String() {
		t.Fatalf("expected recovered msg id=%s, got %#v", uid.String(), msgs)
	}
}

// filesystemSegmentName returns zero-padded segment file name for index
func filesystemSegmentName(i int) string {
	return fmt.Sprintf("%08d.wal", i)
}
