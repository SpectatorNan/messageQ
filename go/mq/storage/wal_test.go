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

func TestPruneExpiredSegmentsSkipsSystemDelayTopic(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	snapshot := Message{
		ID:        uuid.NewString(),
		Body:      "delay-snapshot",
		Tag:       "__DELAY_META__",
		Timestamp: time.Now().Add(-72 * time.Hour),
	}
	if err := store.AppendSync(systemDelayTopicName, 0, snapshot); err != nil {
		t.Fatalf("append delay snapshot: %v", err)
	}

	if err := store.PruneExpiredSegments(systemDelayTopicName, 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune system delay topic: %v", err)
	}

	msgs, err := store.Load(systemDelayTopicName, 0)
	if err != nil {
		t.Fatalf("load system delay topic: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != snapshot.ID {
		t.Fatalf("expected delay snapshot to be retained, got %+v", msgs)
	}
}

func TestPruneConsumedSegmentsKeepsUnreadMessages(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	first := Message{
		ID:        uuid.NewString(),
		Body:      "first",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}
	second := Message{
		ID:        uuid.NewString(),
		Body:      "second",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}

	if err := store.AppendSync("orders", 0, first); err != nil {
		t.Fatalf("append first message: %v", err)
	}
	if err := store.rotateSegment("orders", 0); err != nil {
		t.Fatalf("rotate segment: %v", err)
	}
	if err := store.AppendSync("orders", 0, second); err != nil {
		t.Fatalf("append second message: %v", err)
	}

	deleted, err := store.PruneConsumedSegments("orders", 0, 1)
	if err != nil {
		t.Fatalf("prune consumed segments: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 consumed segment to be pruned, got %d", deleted)
	}
	if base := store.ConsumeQueueBaseOffset("orders", 0); base != 1 {
		t.Fatalf("expected base offset 1 after consumed prune, got %d", base)
	}
	msgs, nextOffset, err := store.ReadFromConsumeQueueWithOffsets("orders", 0, 0, 10, "")
	if err != nil {
		t.Fatalf("read rebased consumequeue: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != second.ID {
		t.Fatalf("expected second message to remain visible, got %+v", msgs)
	}
	if msgs[0].Offset != 1 || nextOffset != 2 {
		t.Fatalf("expected logical offsets [1,2), got offset=%d next=%d", msgs[0].Offset, nextOffset)
	}
}

func TestPruneConsumedSegmentsRotatesFullyConsumedCurrentSegment(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	msg := Message{
		ID:        uuid.NewString(),
		Body:      "only",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}
	if err := store.AppendSync("archive", 0, msg); err != nil {
		t.Fatalf("append message: %v", err)
	}

	deleted, err := store.PruneConsumedSegments("archive", 0, 1)
	if err != nil {
		t.Fatalf("prune fully consumed current segment: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 consumed segment to be pruned, got %d", deleted)
	}
	if base := store.ConsumeQueueBaseOffset("archive", 0); base != 1 {
		t.Fatalf("expected base offset 1 after fully consumed prune, got %d", base)
	}
	segs, err := store.listTopicSegments("archive", 0)
	if err != nil {
		t.Fatalf("list topic segments: %v", err)
	}
	if len(segs) != 1 || filepath.Base(segs[0]) != "00000002.wal" {
		t.Fatalf("expected a rotated empty segment to remain, got %v", segs)
	}
}

func TestAckLogAppendAndLoad(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)

	record := AckRecord{
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  "orders.retry.group-a",
		QueueID:       0,
		MsgID:         uuid.NewString(),
		Offset:        7,
		NextOffset:    8,
		Tag:           "tag-a",
		CorrelationID: "corr-1",
		Retry:         2,
		Timestamp:     time.Now().Add(-time.Minute).UTC().Round(0),
		AckedAt:       time.Now().UTC().Round(0),
	}
	if err := store.AppendAck(record); err != nil {
		t.Fatalf("append ack record: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = reopened.Close()
	}()

	records, err := reopened.LoadAckLog("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("load ack log: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 ack record, got %d", len(records))
	}
	got := records[0]
	if got.Group != record.Group || got.Topic != record.Topic || got.StorageTopic != record.StorageTopic {
		t.Fatalf("unexpected ack routing: %+v", got)
	}
	if got.MsgID != record.MsgID || got.Offset != record.Offset || got.NextOffset != record.NextOffset {
		t.Fatalf("unexpected ack offsets: %+v", got)
	}
	if got.CorrelationID != record.CorrelationID || got.Retry != record.Retry {
		t.Fatalf("unexpected ack metadata: %+v", got)
	}
}

func TestDeliveryLogSupportsTopicScopedEvents(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)

	eventAt := time.Now().UTC().Round(0)
	record := DeliveryEventRecord{
		Event:         DeliveryEventCancelled,
		Topic:         "orders",
		StorageTopic:  "orders.retry.group-a",
		MsgID:         uuid.NewString(),
		CorrelationID: "corr-topic",
		Retry:         2,
		Timestamp:     time.Now().Add(-time.Minute).UTC().Round(0),
		EventAt:       eventAt,
	}
	if err := store.AppendDeliveryEvent(record); err != nil {
		t.Fatalf("append topic-scoped delivery event: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = reopened.Close()
	}()

	events, err := reopened.LoadDeliveryEvents("", "orders", nil)
	if err != nil {
		t.Fatalf("load topic-scoped delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 topic-scoped delivery event, got %d", len(events))
	}
	got := events[0]
	if got.Event != DeliveryEventCancelled || got.Topic != record.Topic || got.StorageTopic != record.StorageTopic {
		t.Fatalf("unexpected topic-scoped delivery event: %+v", got)
	}
	if got.Group != "" || got.MsgID != record.MsgID || got.CorrelationID != record.CorrelationID {
		t.Fatalf("unexpected topic-scoped delivery metadata: %+v", got)
	}
}

func TestListTopicDeliveryEventsAcrossQueueFiles(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	queueID := 0
	older := DeliveryEventRecord{
		Event:        DeliveryEventCancelled,
		Topic:        "orders",
		StorageTopic: "orders",
		QueueID:      &queueID,
		MsgID:        uuid.NewString(),
		EventAt:      time.Now().Add(-time.Minute).UTC().Round(0),
	}
	newer := DeliveryEventRecord{
		Event:        DeliveryEventCancelled,
		Topic:        "orders",
		StorageTopic: "orders",
		MsgID:        uuid.NewString(),
		EventAt:      time.Now().UTC().Round(0),
	}
	if err := store.AppendDeliveryEvent(older); err != nil {
		t.Fatalf("append older topic delivery event: %v", err)
	}
	if err := store.AppendDeliveryEvent(newer); err != nil {
		t.Fatalf("append newer topic delivery event: %v", err)
	}

	events, err := store.ListTopicDeliveryEvents("orders", DeliveryEventCancelled, 10)
	if err != nil {
		t.Fatalf("list topic delivery events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 topic delivery events, got %d", len(events))
	}
	if events[0].MsgID != newer.MsgID || events[1].MsgID != older.MsgID {
		t.Fatalf("expected topic delivery events in reverse chronological order, got %+v", events)
	}
}

func TestListGroupDeliveryEventsAcrossQueueFiles(t *testing.T) {
	dir := t.TempDir()
	store := NewWALStorage(dir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	queueZero := 0
	queueOne := 1
	older := DeliveryEventRecord{
		Event:        DeliveryEventAck,
		Group:        "group-a",
		Topic:        "orders",
		StorageTopic: "orders",
		QueueID:      &queueZero,
		MsgID:        uuid.NewString(),
		Offset:       func() *int64 { v := int64(3); return &v }(),
		NextOffset:   func() *int64 { v := int64(4); return &v }(),
		EventAt:      time.Now().Add(-time.Minute).UTC().Round(0),
	}
	newer := DeliveryEventRecord{
		Event:        DeliveryEventAck,
		Group:        "group-a",
		Topic:        "orders",
		StorageTopic: "orders",
		QueueID:      &queueOne,
		MsgID:        uuid.NewString(),
		Offset:       func() *int64 { v := int64(8); return &v }(),
		NextOffset:   func() *int64 { v := int64(9); return &v }(),
		EventAt:      time.Now().UTC().Round(0),
	}
	if err := store.AppendDeliveryEvent(older); err != nil {
		t.Fatalf("append older group delivery event: %v", err)
	}
	if err := store.AppendDeliveryEvent(newer); err != nil {
		t.Fatalf("append newer group delivery event: %v", err)
	}

	events, err := store.ListGroupDeliveryEvents("group-a", "orders", DeliveryEventAck, 10)
	if err != nil {
		t.Fatalf("list group delivery events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 group delivery events, got %d", len(events))
	}
	if events[0].MsgID != newer.MsgID || events[1].MsgID != older.MsgID {
		t.Fatalf("expected group delivery events in reverse chronological order, got %+v", events)
	}
}
