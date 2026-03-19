package storage

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPruneExpiredSegmentsKeepsConsumeQueueOffsets(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	oldMsg := Message{
		ID:        uuid.NewString(),
		Body:      "old-body",
		Tag:       "tag-a",
		Timestamp: time.Now().Add(-48 * time.Hour),
	}
	freshMsg := Message{
		ID:        uuid.NewString(),
		Body:      "fresh-body",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}

	if err := store.AppendSync("orders", 0, oldMsg); err != nil {
		t.Fatalf("append old message: %v", err)
	}
	if err := store.rotateSegment("orders", 0); err != nil {
		t.Fatalf("rotate segment: %v", err)
	}
	if err := store.AppendSync("orders", 0, freshMsg); err != nil {
		t.Fatalf("append fresh message: %v", err)
	}

	if err := store.PruneExpiredSegments("orders", 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune expired segments: %v", err)
	}

	segs, err := store.listTopicSegments("orders", 0)
	if err != nil {
		t.Fatalf("list topic segments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected 1 remaining segment, got %d", len(segs))
	}
	if filepath.Base(segs[0]) != "00000002.wal" {
		t.Fatalf("expected latest segment to remain, got %s", filepath.Base(segs[0]))
	}

	msgs, nextOffset, err := store.ReadFromConsumeQueueWithOffsets("orders", 0, 0, 10, "")
	if err != nil {
		t.Fatalf("read from consumequeue: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 visible message after pruning, got %d", len(msgs))
	}
	if msgs[0].ID != freshMsg.ID {
		t.Fatalf("expected fresh message %s, got %s", freshMsg.ID, msgs[0].ID)
	}
	if msgs[0].Offset != 1 {
		t.Fatalf("expected fresh message offset 1 after skipped deleted segment, got %d", msgs[0].Offset)
	}
	if nextOffset != 2 {
		t.Fatalf("expected next offset 2, got %d", nextOffset)
	}
}

func TestPruneExpiredSegmentsRotatesExpiredCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	oldMsg := Message{
		ID:        uuid.NewString(),
		Body:      "old-only",
		Tag:       "tag-a",
		Timestamp: time.Now().Add(-72 * time.Hour),
	}

	if err := store.AppendSync("archive", 0, oldMsg); err != nil {
		t.Fatalf("append old message: %v", err)
	}

	if err := store.PruneExpiredSegments("archive", 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune expired current segment: %v", err)
	}

	segs, err := store.listTopicSegments("archive", 0)
	if err != nil {
		t.Fatalf("list topic segments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected a single replacement segment, got %d", len(segs))
	}
	if filepath.Base(segs[0]) != "00000002.wal" {
		t.Fatalf("expected rotated replacement segment, got %s", filepath.Base(segs[0]))
	}

	msgs, nextOffset, err := store.ReadFromConsumeQueueWithOffsets("archive", 0, 0, 10, "")
	if err != nil {
		t.Fatalf("read from consumequeue: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no visible messages after pruning expired current segment, got %d", len(msgs))
	}
	if nextOffset != 1 {
		t.Fatalf("expected consumequeue cursor to advance across deleted entry, got %d", nextOffset)
	}
}

func TestConsumeQueueBaseOffsetPersistsAndClampsOffsets(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)

	oldMsg := Message{
		ID:        uuid.NewString(),
		Body:      "old-body",
		Tag:       "tag-a",
		Timestamp: time.Now().Add(-48 * time.Hour),
	}
	freshMsg := Message{
		ID:        uuid.NewString(),
		Body:      "fresh-body",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}

	if err := store.AppendSync("payments", 0, oldMsg); err != nil {
		t.Fatalf("append old message: %v", err)
	}
	if err := store.rotateSegment("payments", 0); err != nil {
		t.Fatalf("rotate segment: %v", err)
	}
	if err := store.AppendSync("payments", 0, freshMsg); err != nil {
		t.Fatalf("append fresh message: %v", err)
	}
	if err := store.PruneExpiredSegments("payments", 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune expired segments: %v", err)
	}

	if base := store.ConsumeQueueBaseOffset("payments", 0); base != 1 {
		t.Fatalf("expected base offset 1 after trimming head, got %d", base)
	}
	if tail := store.ConsumeQueueDepth("payments", 0); tail != 2 {
		t.Fatalf("expected logical tail offset 2, got %d", tail)
	}

	if err := store.CommitOffset("group-a", "payments", 0, 0); err != nil {
		t.Fatalf("commit offset below base: %v", err)
	}
	offset, ok, err := store.GetOffset("group-a", "payments", 0)
	if err != nil {
		t.Fatalf("get offset: %v", err)
	}
	if !ok || offset != 1 {
		t.Fatalf("expected clamped offset 1, got %d (ok=%v)", offset, ok)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = reopened.Close()
	}()

	if base := reopened.ConsumeQueueBaseOffset("payments", 0); base != 1 {
		t.Fatalf("expected persisted base offset 1 after reopen, got %d", base)
	}
	msgs, nextOffset, err := reopened.ReadFromConsumeQueueWithOffsets("payments", 0, 0, 10, "")
	if err != nil {
		t.Fatalf("read from rebased consumequeue: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != freshMsg.ID {
		t.Fatalf("expected rebased consumequeue to return fresh message, got %+v", msgs)
	}
	if msgs[0].Offset != 1 || nextOffset != 2 {
		t.Fatalf("expected logical offsets [1,2), got offset=%d next=%d", msgs[0].Offset, nextOffset)
	}
}
