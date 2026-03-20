package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/SpectatorNan/messageQ/go/mq/queue"
	"github.com/SpectatorNan/messageQ/go/mq/storage"
	"github.com/google/uuid"
)

type failingProcessingStore struct {
	*storage.WALStorage
	saveErr            error
	processingEventErr error
	retryEventErr      error
	terminalEventErr   error
	delaySnapshotErr   error
}

func (s *failingProcessingStore) SaveProcessing(rec storage.ProcessingRecord) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.WALStorage.SaveProcessing(rec)
}

func (s *failingProcessingStore) AppendDeliveryEvent(rec storage.DeliveryEventRecord) error {
	if rec.Event == storage.DeliveryEventProcessing && s.processingEventErr != nil {
		return s.processingEventErr
	}
	if rec.Event == storage.DeliveryEventRetry && s.retryEventErr != nil {
		return s.retryEventErr
	}
	if (rec.Event == storage.DeliveryEventCancelled || rec.Event == storage.DeliveryEventExpired) && s.terminalEventErr != nil {
		return s.terminalEventErr
	}
	return s.WALStorage.AppendDeliveryEvent(rec)
}

func (s *failingProcessingStore) ReplaceTopicMessages(topic string, queueID int, msgs []storage.Message) error {
	if topic == SystemDelayTopic && s.delaySnapshotErr != nil {
		return s.delaySnapshotErr
	}
	return s.WALStorage.ReplaceTopicMessages(topic, queueID, msgs)
}

func TestPrunedRetrySegmentMarksLogicalTopicExpired(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	retryTopic := GetRetryTopicName("group-a", "orders")
	oldMsg := storage.Message{
		ID:        uuid.NewString(),
		Body:      "bad-payload",
		Tag:       "tag-a",
		Timestamp: time.Now().Add(-72 * time.Hour),
	}

	if err := store.AppendSync(retryTopic, 0, oldMsg); err != nil {
		t.Fatalf("append retry message: %v", err)
	}

	if err := store.PruneExpiredSegments(retryTopic, 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune retry segment: %v", err)
	}

	if !b.IsExpired("orders", oldMsg.ID) {
		t.Fatalf("expected logical topic message %s to be marked expired", oldMsg.ID)
	}

	expired := b.ListExpired("orders", 10)
	if len(expired) != 1 {
		t.Fatalf("expected exactly 1 expired entry, got %d", len(expired))
	}
	if expired[0].MsgID != oldMsg.ID {
		t.Fatalf("expected expired message %s, got %s", oldMsg.ID, expired[0].MsgID)
	}
	if expired[0].Topic != "orders" {
		t.Fatalf("expected expired logical topic orders, got %s", expired[0].Topic)
	}

	queueID := 0
	events, err := store.LoadDeliveryEvents("", "orders", &queueID)
	if err != nil {
		t.Fatalf("load expired delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 expired delivery event, got %d", len(events))
	}
	if events[0].Event != storage.DeliveryEventExpired {
		t.Fatalf("expected expired delivery event, got %s", events[0].Event)
	}
	if events[0].StorageTopic != retryTopic {
		t.Fatalf("expected expired event storage topic %s, got %s", retryTopic, events[0].StorageTopic)
	}
}

func TestRetryTopicInitialOffsetUsesBaseOffset(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	retryTopic, err := b.ensureRetryTopic("group-a", "orders")
	if err != nil {
		t.Fatalf("ensure retry topic: %v", err)
	}

	oldMsg := storage.Message{
		ID:        uuid.NewString(),
		Body:      "expired-retry",
		Tag:       "tag-a",
		Timestamp: time.Now().Add(-72 * time.Hour),
	}
	freshMsg := storage.Message{
		ID:        uuid.NewString(),
		Body:      "fresh-retry",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}

	store.SetCompactThreshold(1)
	if err := store.AppendSync(retryTopic, 0, oldMsg); err != nil {
		t.Fatalf("append old retry message: %v", err)
	}
	store.SetCompactThreshold(10 * 1024 * 1024)
	if err := store.AppendSync(retryTopic, 0, freshMsg); err != nil {
		t.Fatalf("append fresh retry message: %v", err)
	}
	if err := store.PruneExpiredSegments(retryTopic, 0, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("prune retry segment: %v", err)
	}

	msgs, offset, nextOffset, err := b.ConsumeWithRetry("group-a", "orders", 0, "tag-a", 1)
	if err != nil {
		t.Fatalf("consume with retry: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 retry message, got %d", len(msgs))
	}
	if msgs[0].ID != freshMsg.ID {
		t.Fatalf("expected fresh retry message %s, got %s", freshMsg.ID, msgs[0].ID)
	}
	if offset != 1 || nextOffset != 2 {
		t.Fatalf("expected retry offsets [1,2), got [%d,%d)", offset, nextOffset)
	}
}

func TestDelaySnapshotsKeepLatestStateAndReload(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)

	delayOne := storage.Message{
		ID:        uuid.NewString(),
		Body:      "delay-one",
		Tag:       "tag-a",
		Timestamp: time.Now(),
	}
	delayTwo := storage.Message{
		ID:        uuid.NewString(),
		Body:      "delay-two",
		Tag:       "tag-b",
		Timestamp: time.Now(),
	}

	b.GetDelayScheduler().ScheduleWithDelay("orders", 0, delayOne, time.Hour)
	b.GetDelayScheduler().ScheduleWithDelay("orders", 0, delayTwo, 2*time.Hour)

	snapshots, err := store.Load(SystemDelayTopic, 0)
	if err != nil {
		t.Fatalf("load delay snapshots: %v", err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected exactly one delay snapshot, got %d", len(snapshots))
	}
	if snapshots[0].Tag != "__DELAY_META__" {
		t.Fatalf("expected delay meta tag, got %s", snapshots[0].Tag)
	}

	if err := b.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}

	reloaded := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = reloaded.Close()
	}()

	if reloaded.GetDelayScheduler().delayQueue.Len() != 2 {
		t.Fatalf("expected 2 delayed messages after reload, got %d", reloaded.GetDelayScheduler().delayQueue.Len())
	}
	ids := make(map[string]bool, 2)
	for _, item := range reloaded.GetDelayScheduler().delayQueue {
		ids[item.Message.ID] = true
	}
	if !ids[delayOne.ID] || !ids[delayTwo.ID] {
		t.Fatalf("expected delay queue to restore both snapshots, got ids=%v", ids)
	}
}

func TestCompleteProcessingWritesAckLogWithStorageTopic(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "retry-message",
		Tag:           "tag-a",
		CorrelationID: "corr-ack",
		Retry:         1,
		Timestamp:     time.Now().Add(-time.Minute),
	}
	storageTopic := GetRetryTopicName("group-a", "orders")
	if err := b.BeginProcessing("group-a", "orders", storageTopic, 0, 7, 8, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	ok, err := b.CompleteProcessing(msg.ID, "group-a", "orders")
	if err != nil {
		t.Fatalf("complete processing: %v", err)
	}
	if !ok {
		t.Fatalf("expected complete processing to succeed")
	}

	records, err := store.LoadAckLog("group-a", "orders", 0)
	if err != nil {
		t.Fatalf("load ack log: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 ack record, got %d", len(records))
	}
	got := records[0]
	if got.StorageTopic != storageTopic {
		t.Fatalf("expected storage topic %s, got %s", storageTopic, got.StorageTopic)
	}
	if got.Offset != 7 || got.NextOffset != 8 {
		t.Fatalf("expected offsets [7,8), got [%d,%d)", got.Offset, got.NextOffset)
	}
	if got.MsgID != msg.ID || got.CorrelationID != msg.CorrelationID {
		t.Fatalf("unexpected ack payload: %+v", got)
	}
}

func TestRetryProcessingWritesDeliveryEventWithStorageTopic(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "retry-me",
		Tag:           "tag-a",
		CorrelationID: "corr-retry",
		Timestamp:     time.Now().Add(-time.Minute),
	}
	storageTopic := GetRetryTopicName("group-a", "orders")
	if err := b.BeginProcessing("group-a", "orders", storageTopic, 0, 3, 4, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	if ok, err := b.RetryProcessing(msg.ID, "group-a", "orders"); err != nil {
		t.Fatalf("retry processing: %v", err)
	} else if !ok {
		t.Fatalf("expected retry processing to succeed")
	}

	events, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("load retry delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 retry delivery event, got %d", len(events))
	}
	got := events[0]
	if got.Event != storage.DeliveryEventRetry {
		t.Fatalf("expected retry delivery event, got %s", got.Event)
	}
	if got.StorageTopic != storageTopic {
		t.Fatalf("expected retry event storage topic %s, got %s", storageTopic, got.StorageTopic)
	}
	if got.QueueID == nil || *got.QueueID != 0 {
		t.Fatalf("expected retry event queue 0, got %+v", got.QueueID)
	}
	if got.Offset == nil || got.NextOffset == nil || *got.Offset != 3 || *got.NextOffset != 4 {
		t.Fatalf("expected retry offsets [3,4), got offset=%v next=%v", got.Offset, got.NextOffset)
	}
	if got.ScheduledAt == nil {
		t.Fatalf("expected retry event scheduled_at to be set")
	}
}

func TestBeginProcessingReturnsErrorWhenPersistenceFails(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	walStore := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingProcessingStore{
		WALStorage: walStore,
		saveErr:    errors.New("save processing failed"),
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "persist-me",
		Tag:           "tag-a",
		CorrelationID: "corr-processing-error",
		Timestamp:     time.Now(),
	}

	err := b.BeginProcessing("group-a", "orders", "orders", 0, 11, 12, msg)
	if err == nil {
		t.Fatalf("expected begin processing to return an error")
	}

	entries := b.ListProcessing("group-a", "orders", 10)
	if len(entries) != 1 || entries[0].MsgID != msg.ID {
		t.Fatalf("expected in-memory processing entry for %s, got %+v", msg.ID, entries)
	}

	if ok, err := b.RetryProcessing(msg.ID, "group-a", "orders"); err != nil {
		t.Fatalf("retry rollback: %v", err)
	} else if !ok {
		t.Fatalf("expected retry rollback to succeed")
	}

	records, err := walStore.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no persisted processing records after retry rollback, got %d", len(records))
	}

	events, err := walStore.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("load retry delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 retry event after rollback, got %d", len(events))
	}
	if events[0].Event != storage.DeliveryEventRetry {
		t.Fatalf("expected retry delivery event, got %s", events[0].Event)
	}
}

func TestBeginProcessingWritesProcessingDeliveryEvent(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "processing-me",
		Tag:           "tag-a",
		CorrelationID: "corr-processing",
		Retry:         2,
		Timestamp:     time.Now().Add(-time.Minute),
	}
	storageTopic := GetRetryTopicName("group-a", "orders")
	if err := b.BeginProcessing("group-a", "orders", storageTopic, 0, 7, 8, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	queueID := 0
	events, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventProcessing, 10)
	if err != nil {
		t.Fatalf("list processing delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 processing delivery event, got %d", len(events))
	}
	got := events[0]
	if got.StorageTopic != storageTopic {
		t.Fatalf("expected processing event storage topic %s, got %s", storageTopic, got.StorageTopic)
	}
	if got.QueueID == nil || *got.QueueID != queueID {
		t.Fatalf("expected processing event queue 0, got %+v", got.QueueID)
	}
	if got.Offset == nil || got.NextOffset == nil || *got.Offset != 7 || *got.NextOffset != 8 {
		t.Fatalf("expected processing offsets [7,8), got offset=%v next=%v", got.Offset, got.NextOffset)
	}
	if got.ConsumedAt == nil || got.ConsumedAt.IsZero() {
		t.Fatalf("expected processing event consumed_at to be set")
	}
	if got.EventAt.IsZero() {
		t.Fatalf("expected processing event event_at to be set")
	}
	if got.MsgID != msg.ID || got.CorrelationID != msg.CorrelationID || got.Retry != msg.Retry {
		t.Fatalf("unexpected processing event payload: %+v", got)
	}

	records, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected processing fallback records to be removed after processing event append, got %d", len(records))
	}
}

func TestBeginProcessingReturnsErrorWhenProcessingEventPersistenceFails(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	walStore := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingProcessingStore{
		WALStorage:         walStore,
		processingEventErr: errors.New("append processing event failed"),
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "persist-event-me",
		Tag:           "tag-a",
		CorrelationID: "corr-processing-event-error",
		Timestamp:     time.Now(),
	}

	err := b.BeginProcessing("group-a", "orders", "orders", 0, 13, 14, msg)
	if err == nil {
		t.Fatalf("expected begin processing to return an error")
	}

	entries := b.ListProcessing("group-a", "orders", 10)
	if len(entries) != 1 || entries[0].MsgID != msg.ID {
		t.Fatalf("expected in-memory processing entry for %s, got %+v", msg.ID, entries)
	}

	if ok, err := b.RetryProcessing(msg.ID, "group-a", "orders"); err != nil {
		t.Fatalf("retry rollback: %v", err)
	} else if !ok {
		t.Fatalf("expected retry rollback to succeed")
	}

	records, err := walStore.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected no persisted processing records after retry rollback, got %d", len(records))
	}

	events, err := walStore.ListGroupDeliveryEvents("group-a", "orders", "", 10)
	if err != nil {
		t.Fatalf("list delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 delivery event after rollback, got %d", len(events))
	}
	if events[0].Event != storage.DeliveryEventRetry {
		t.Fatalf("expected retry delivery event after rollback, got %s", events[0].Event)
	}
}

func TestRetryProcessingReturnsErrorWhenRetryEventPersistenceFails(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	walStore := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingProcessingStore{
		WALStorage:    walStore,
		retryEventErr: errors.New("append retry event failed"),
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "retry-fail-me",
		Tag:           "tag-a",
		CorrelationID: "corr-retry-event-error",
		Timestamp:     time.Now(),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 1, 2, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	if ok, err := b.RetryProcessing(msg.ID, "group-a", "orders"); err == nil {
		t.Fatalf("expected retry processing to return an error")
	} else if ok {
		t.Fatalf("expected retry processing to report failure")
	}
	if !b.ValidateProcessing(msg.ID, "group-a", "orders") {
		t.Fatalf("expected message to remain processing after retry event failure")
	}
	if b.GetDelayScheduler().delayQueue.Len() != 0 {
		t.Fatalf("expected scheduled retry rollback after event failure, got %d pending", b.GetDelayScheduler().delayQueue.Len())
	}
	events, err := walStore.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no retry delivery events after retry event failure, got %d", len(events))
	}
}

func TestRetryProcessingReturnsErrorWhenRetrySchedulePersistenceFails(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	walStore := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingProcessingStore{
		WALStorage:       walStore,
		delaySnapshotErr: errors.New("persist delay snapshot failed"),
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "retry-schedule-fail-me",
		Tag:           "tag-a",
		CorrelationID: "corr-retry-schedule-error",
		Timestamp:     time.Now(),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 1, 2, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	if ok, err := b.RetryProcessing(msg.ID, "group-a", "orders"); err == nil {
		t.Fatalf("expected retry scheduling to return an error")
	} else if ok {
		t.Fatalf("expected retry scheduling to report failure")
	}
	if !b.ValidateProcessing(msg.ID, "group-a", "orders") {
		t.Fatalf("expected message to remain processing after retry scheduling failure")
	}
	if b.GetDelayScheduler().delayQueue.Len() != 0 {
		t.Fatalf("expected no scheduled retry after persistence failure, got %d pending", b.GetDelayScheduler().delayQueue.Len())
	}
	events, err := walStore.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("expected no retry delivery events after retry scheduling failure, got %d", len(events))
	}
}

func TestTerminateMessageReturnsErrorWhenTerminalEventPersistenceFails(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	walStore := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = walStore.Close()
	}()

	store := &failingProcessingStore{
		WALStorage:       walStore,
		terminalEventErr: errors.New("append terminal event failed"),
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "cancel-fail-me",
		Tag:           "tag-a",
		CorrelationID: "corr-cancel-event-error",
		Timestamp:     time.Now(),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 1, 2, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	if ok, err := b.TerminateMessage(msg.ID, "orders"); err == nil {
		t.Fatalf("expected terminate message to return an error")
	} else if ok {
		t.Fatalf("expected terminate message to report failure")
	}
	if !b.ValidateProcessing(msg.ID, "group-a", "orders") {
		t.Fatalf("expected message to remain processing after terminal event failure")
	}
	if b.IsCancelled("orders", msg.ID) {
		t.Fatalf("expected message not to be marked cancelled after terminal event failure")
	}
}

func TestListProcessingReplaysCurrentEntriesFromDeliveryEvents(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "replay-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-replay-processing",
		Timestamp:     time.Now(),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 21, 22, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	key := processingKey("group-a", "orders", msg.ID)
	b.lock.Lock()
	delete(b.processing, key)
	b.lock.Unlock()

	entries := b.ListProcessing("group-a", "orders", 10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 replayed processing entry, got %d", len(entries))
	}
	got := entries[0]
	if got.MsgID != msg.ID || got.CorrelationID != msg.CorrelationID {
		t.Fatalf("unexpected replayed processing entry: %+v", got)
	}
	if got.Offset != 21 || got.NextOffset != 22 {
		t.Fatalf("expected replayed offsets [21,22), got [%d,%d)", got.Offset, got.NextOffset)
	}
}

func TestListProcessingEventViewClearsStaleMemoryAfterAck(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "acked-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-acked-processing",
		Timestamp:     time.Now(),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 23, 24, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	key := processingKey("group-a", "orders", msg.ID)
	b.lock.Lock()
	stale := b.processing[key]
	b.lock.Unlock()

	if ok, err := b.CompleteProcessing(msg.ID, "group-a", "orders"); err != nil {
		t.Fatalf("complete processing: %v", err)
	} else if !ok {
		t.Fatalf("expected processing completion to succeed")
	}

	b.lock.Lock()
	stale.State = stateProcessing
	b.processing[key] = stale
	b.lock.Unlock()

	entries := b.ListProcessing("group-a", "orders", 10)
	if len(entries) != 0 {
		t.Fatalf("expected settled ack event to clear stale memory processing entry, got %+v", entries)
	}
}

func TestSweepProcessingTimeoutsExpiresStaleInFlightMessages(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	b.SetProcessingTimeout(time.Second)
	b.SetMessageRetention(time.Second)
	b.SetMessageExpiryFactor(2)

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "stale-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-stale-processing",
		Timestamp:     time.Now().Add(-3 * time.Second),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 5, 6, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}

	expired, retried := b.sweepProcessingTimeoutsAt(now().Add(2 * time.Second))
	if expired != 1 || retried != 0 {
		t.Fatalf("expected 1 expired and 0 retried entries, got expired=%d retried=%d", expired, retried)
	}
	if !b.IsExpired("orders", msg.ID) {
		t.Fatalf("expected message %s to be marked expired", msg.ID)
	}
	if entries := b.ListProcessing("group-a", "orders", 10); len(entries) != 0 {
		t.Fatalf("expected no processing entries after expiry, got %d", len(entries))
	}
	processingRecords, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(processingRecords) != 0 {
		t.Fatalf("expected processing records to be removed after expiry, got %d", len(processingRecords))
	}

	queueID := 0
	events, err := store.LoadDeliveryEvents("", "orders", &queueID)
	if err != nil {
		t.Fatalf("load delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 expired delivery event, got %d", len(events))
	}
	if events[0].Event != storage.DeliveryEventExpired {
		t.Fatalf("expected expired delivery event, got %s", events[0].Event)
	}
	if events[0].Offset == nil || events[0].NextOffset == nil || *events[0].Offset != 5 || *events[0].NextOffset != 6 {
		t.Fatalf("expected expired event offsets [5,6), got offset=%v next=%v", events[0].Offset, events[0].NextOffset)
	}
}

func TestRecoverProcessingRecordsExpireStaleEntries(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	offset := int64(7)
	nextOffset := int64(8)
	msgID := uuid.NewString()
	err := store.SaveProcessing(storage.ProcessingRecord{
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  "orders",
		QueueID:       0,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         msgID,
		Body:          "recovered-stale-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-recovered-stale-processing",
		Retry:         2,
		Timestamp:     time.Now().Add(-15 * 24 * time.Hour),
		UpdatedAt:     time.Now().Add(-10 * time.Second),
	})
	if err != nil {
		t.Fatalf("save processing record: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if !b.IsExpired("orders", msgID) {
		t.Fatalf("expected recovered processing record %s to be marked expired", msgID)
	}
	processingRecords, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(processingRecords) != 0 {
		t.Fatalf("expected recovered processing records to be removed, got %d", len(processingRecords))
	}
	if b.GetDelayScheduler().delayQueue.Len() != 0 {
		t.Fatalf("expected no retry to be scheduled for expired recovered processing")
	}

	queueID := 0
	events, err := store.LoadDeliveryEvents("", "orders", &queueID)
	if err != nil {
		t.Fatalf("load delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 recovered expired delivery event, got %d", len(events))
	}
	if events[0].Event != storage.DeliveryEventExpired {
		t.Fatalf("expected expired delivery event after recovery, got %s", events[0].Event)
	}
	if events[0].Offset == nil || events[0].NextOffset == nil || *events[0].Offset != 7 || *events[0].NextOffset != 8 {
		t.Fatalf("expected recovered expired offsets [7,8), got offset=%v next=%v", events[0].Offset, events[0].NextOffset)
	}
}

func TestRecoverProcessingRecordsSkipsTopicSettledEvents(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	offset := int64(9)
	nextOffset := int64(10)
	msgID := uuid.NewString()
	ts := time.Now()
	err := store.SaveProcessing(storage.ProcessingRecord{
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  "orders",
		QueueID:       0,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         msgID,
		Body:          "stale-settled-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-stale-settled-processing",
		Retry:         1,
		Timestamp:     ts,
		UpdatedAt:     ts,
	})
	if err != nil {
		t.Fatalf("save processing record: %v", err)
	}

	queueID := 0
	eventAt := ts.Add(time.Second)
	if err := store.AppendDeliveryEvent(storage.DeliveryEventRecord{
		Event:         storage.DeliveryEventCancelled,
		Topic:         "orders",
		StorageTopic:  "orders",
		QueueID:       &queueID,
		MsgID:         msgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          "stale-settled-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-stale-settled-processing",
		Retry:         1,
		Timestamp:     ts,
		EventAt:       eventAt,
	}); err != nil {
		t.Fatalf("append cancelled delivery event: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if b.GetDelayScheduler().delayQueue.Len() != 0 {
		t.Fatalf("expected no retry to be scheduled for already-settled processing")
	}
	if got := b.GetRetryCount(msgID); got != 0 {
		t.Fatalf("expected retry count to remain 0 for settled processing, got %d", got)
	}
	processingRecords, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(processingRecords) != 0 {
		t.Fatalf("expected settled processing record to be removed, got %d", len(processingRecords))
	}
	retryEvents, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(retryEvents) != 0 {
		t.Fatalf("expected no retry delivery events for already-settled processing, got %d", len(retryEvents))
	}
}

func TestRecoverProcessingRecordsWriteRetryEventAndAdvanceRetryCount(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	retryTopic := GetRetryTopicName("group-a", "orders")
	offset := int64(11)
	nextOffset := int64(12)
	msgID := uuid.NewString()
	err := store.SaveProcessing(storage.ProcessingRecord{
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  retryTopic,
		QueueID:       0,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		MsgID:         msgID,
		Body:          "recovered-retry-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-recovered-retry-processing",
		Retry:         2,
		Timestamp:     time.Now(),
		UpdatedAt:     time.Now(),
	})
	if err != nil {
		t.Fatalf("save processing record: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if got := b.GetRetryCount(msgID); got != 3 {
		t.Fatalf("expected retry count to advance to 3 after recovery, got %d", got)
	}
	if b.GetDelayScheduler().delayQueue.Len() != 1 {
		t.Fatalf("expected one recovered retry to be scheduled, got %d", b.GetDelayScheduler().delayQueue.Len())
	}

	queueID := 0
	events, err := store.LoadDeliveryEvents("group-a", "orders", &queueID)
	if err != nil {
		t.Fatalf("load delivery events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 recovered retry delivery event, got %d", len(events))
	}
	got := events[0]
	if got.Event != storage.DeliveryEventRetry {
		t.Fatalf("expected recovered retry delivery event, got %s", got.Event)
	}
	if got.StorageTopic != retryTopic {
		t.Fatalf("expected recovered retry storage topic %s, got %s", retryTopic, got.StorageTopic)
	}
	if got.Retry != 3 {
		t.Fatalf("expected recovered retry event count 3, got %d", got.Retry)
	}
	if got.ScheduledAt == nil {
		t.Fatalf("expected recovered retry event scheduled_at to be set")
	}
	if got.Offset == nil || got.NextOffset == nil || *got.Offset != 11 || *got.NextOffset != 12 {
		t.Fatalf("expected recovered retry offsets [11,12), got offset=%v next=%v", got.Offset, got.NextOffset)
	}
}

func TestRecoverProcessingRecordsPreferProcessingDeliveryEventState(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	msgID := uuid.NewString()
	ts := time.Now()
	err := store.SaveProcessing(storage.ProcessingRecord{
		Group:         "group-a",
		Topic:         "orders",
		QueueID:       0,
		MsgID:         msgID,
		Body:          "replay-preferred-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-replay-preferred-processing",
		Retry:         2,
		Timestamp:     ts,
		UpdatedAt:     ts,
	})
	if err != nil {
		t.Fatalf("save processing record: %v", err)
	}

	retryTopic := GetRetryTopicName("group-a", "orders")
	queueID := 0
	offset := int64(11)
	nextOffset := int64(12)
	consumedAt := ts.Add(2 * time.Second)
	if err := store.AppendDeliveryEvent(storage.DeliveryEventRecord{
		Event:         storage.DeliveryEventProcessing,
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  retryTopic,
		QueueID:       &queueID,
		MsgID:         msgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          "replay-preferred-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-replay-preferred-processing",
		Retry:         2,
		Timestamp:     ts,
		ConsumedAt:    &consumedAt,
		EventAt:       consumedAt,
	}); err != nil {
		t.Fatalf("append processing delivery event: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if got := b.GetRetryCount(msgID); got != 3 {
		t.Fatalf("expected retry count to advance to 3 after replay-based recovery, got %d", got)
	}
	if b.GetDelayScheduler().delayQueue.Len() != 1 {
		t.Fatalf("expected one replay-based retry to be scheduled, got %d", b.GetDelayScheduler().delayQueue.Len())
	}
	processingRecords, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(processingRecords) != 0 {
		t.Fatalf("expected replay-based recovery to remove processing record, got %d", len(processingRecords))
	}
	retryEvents, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(retryEvents) != 1 {
		t.Fatalf("expected 1 retry delivery event after replay-based recovery, got %d", len(retryEvents))
	}
	got := retryEvents[0]
	if got.StorageTopic != retryTopic {
		t.Fatalf("expected replay-based retry storage topic %s, got %s", retryTopic, got.StorageTopic)
	}
	if got.Offset == nil || got.NextOffset == nil || *got.Offset != 11 || *got.NextOffset != 12 {
		t.Fatalf("expected replay-based retry offsets [11,12), got offset=%v next=%v", got.Offset, got.NextOffset)
	}
}

func TestRecoverProcessingRecordsFromProcessingDeliveryEventsWithoutPersistedRecord(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	msgID := uuid.NewString()
	ts := time.Now()
	retryTopic := GetRetryTopicName("group-a", "orders")
	queueID := 0
	offset := int64(21)
	nextOffset := int64(22)
	consumedAt := ts.Add(2 * time.Second)
	if err := store.AppendDeliveryEvent(storage.DeliveryEventRecord{
		Event:         storage.DeliveryEventProcessing,
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  retryTopic,
		QueueID:       &queueID,
		MsgID:         msgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          "event-only-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-event-only-processing",
		Retry:         2,
		Timestamp:     ts,
		ConsumedAt:    &consumedAt,
		EventAt:       consumedAt,
	}); err != nil {
		t.Fatalf("append processing delivery event: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if got := b.GetRetryCount(msgID); got != 3 {
		t.Fatalf("expected retry count to advance to 3 after event-only recovery, got %d", got)
	}
	if b.GetDelayScheduler().delayQueue.Len() != 1 {
		t.Fatalf("expected one event-only retry to be scheduled, got %d", b.GetDelayScheduler().delayQueue.Len())
	}
	processingRecords, err := store.LoadProcessing()
	if err != nil {
		t.Fatalf("load processing records: %v", err)
	}
	if len(processingRecords) != 0 {
		t.Fatalf("expected no persisted processing records after event-only recovery, got %d", len(processingRecords))
	}
	retryEvents, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(retryEvents) != 1 {
		t.Fatalf("expected 1 retry delivery event after event-only recovery, got %d", len(retryEvents))
	}
	got := retryEvents[0]
	if got.StorageTopic != retryTopic {
		t.Fatalf("expected event-only retry storage topic %s, got %s", retryTopic, got.StorageTopic)
	}
	if got.Offset == nil || got.NextOffset == nil || *got.Offset != 21 || *got.NextOffset != 22 {
		t.Fatalf("expected event-only retry offsets [21,22), got offset=%v next=%v", got.Offset, got.NextOffset)
	}
}

func TestRecoverProcessingRecordsEventOnlySkipsTopicSettledEvents(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	msgID := uuid.NewString()
	ts := time.Now()
	queueID := 0
	offset := int64(31)
	nextOffset := int64(32)
	consumedAt := ts.Add(2 * time.Second)
	if err := store.AppendDeliveryEvent(storage.DeliveryEventRecord{
		Event:         storage.DeliveryEventProcessing,
		Group:         "group-a",
		Topic:         "orders",
		StorageTopic:  "orders",
		QueueID:       &queueID,
		MsgID:         msgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          "event-only-settled-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-event-only-settled-processing",
		Retry:         1,
		Timestamp:     ts,
		ConsumedAt:    &consumedAt,
		EventAt:       consumedAt,
	}); err != nil {
		t.Fatalf("append processing delivery event: %v", err)
	}
	if err := store.AppendDeliveryEvent(storage.DeliveryEventRecord{
		Event:         storage.DeliveryEventCancelled,
		Topic:         "orders",
		StorageTopic:  "orders",
		QueueID:       &queueID,
		MsgID:         msgID,
		Offset:        &offset,
		NextOffset:    &nextOffset,
		Body:          "event-only-settled-processing",
		Tag:           "tag-a",
		CorrelationID: "corr-event-only-settled-processing",
		Retry:         1,
		Timestamp:     ts,
		EventAt:       consumedAt.Add(time.Second),
	}); err != nil {
		t.Fatalf("append cancelled delivery event: %v", err)
	}

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if b.GetDelayScheduler().delayQueue.Len() != 0 {
		t.Fatalf("expected no retry to be scheduled for event-only settled processing")
	}
	if got := b.GetRetryCount(msgID); got != 0 {
		t.Fatalf("expected retry count to remain 0 for event-only settled processing, got %d", got)
	}
	retryEvents, err := store.ListGroupDeliveryEvents("group-a", "orders", storage.DeliveryEventRetry, 10)
	if err != nil {
		t.Fatalf("list retry delivery events: %v", err)
	}
	if len(retryEvents) != 0 {
		t.Fatalf("expected no retry delivery events for event-only settled processing, got %d", len(retryEvents))
	}
}

func TestListCompletedUsesPersistedAckHistoryAfterRestart(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)

	b := NewBrokerWithPersistence(store, 1, dataDir)
	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "acked-message",
		Tag:           "tag-a",
		CorrelationID: "corr-completed",
		Timestamp:     time.Now().Add(-time.Minute),
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 9, 10, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}
	ok, err := b.CompleteProcessing(msg.ID, "group-a", "orders")
	if err != nil {
		t.Fatalf("complete processing: %v", err)
	}
	if !ok {
		t.Fatalf("expected complete processing to succeed")
	}
	if err := b.Close(); err != nil {
		t.Fatalf("close broker: %v", err)
	}

	reloaded := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = reloaded.Close()
	}()
	entries := reloaded.ListCompleted("group-a", "orders", 10)
	if len(entries) != 1 {
		t.Fatalf("expected 1 completed entry after restart, got %d", len(entries))
	}
	got := entries[0]
	if got.MsgID != msg.ID || got.CorrelationID != msg.CorrelationID {
		t.Fatalf("unexpected completed entry after restart: %+v", got)
	}
	if got.Offset != 9 || got.NextOffset != 10 {
		t.Fatalf("expected completed offsets [9,10), got [%d,%d)", got.Offset, got.NextOffset)
	}
	if got.AckedAt.IsZero() {
		t.Fatalf("expected acked timestamp to be preserved after restart")
	}
}

func TestSweepTerminalTombstonesPreservesCancelledHistory(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := storage.Message{
		ID:            uuid.NewString(),
		Body:          "cancel-me",
		Tag:           "tag-a",
		CorrelationID: "corr-cancel",
		Timestamp:     time.Now(),
	}
	if err := store.AppendSync("orders", 0, msg); err != nil {
		t.Fatalf("append message: %v", err)
	}
	if ok, err := b.TerminateMessage(msg.ID, "orders"); err != nil {
		t.Fatalf("terminate message: %v", err)
	} else if !ok {
		t.Fatalf("expected terminate to succeed")
	}
	if !b.IsCancelled("orders", msg.ID) {
		t.Fatalf("expected message to be cancelled before sweep")
	}

	if err := b.CommitOffset("group-a", "orders", 0, 1); err != nil {
		t.Fatalf("commit offset: %v", err)
	}
	deleted, err := b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("sweep tombstones: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 swept tombstone, got %d", deleted)
	}
	if b.IsCancelled("orders", msg.ID) {
		t.Fatalf("expected tombstone lookup to be cleared after sweep")
	}

	cancelled := b.ListCancelled("orders", 10)
	if len(cancelled) != 1 {
		t.Fatalf("expected cancelled history to remain available, got %d entries", len(cancelled))
	}
	if cancelled[0].MsgID != msg.ID || cancelled[0].CorrelationID != msg.CorrelationID {
		t.Fatalf("unexpected cancelled history entry: %+v", cancelled[0])
	}
}

func TestSweepTerminalTombstonesWaitsForSettledWatermark(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	first := storage.Message{ID: uuid.NewString(), Body: "first", Tag: "tag-a", Timestamp: time.Now()}
	second := storage.Message{ID: uuid.NewString(), Body: "second", Tag: "tag-a", Timestamp: time.Now()}
	if err := store.AppendSync("orders", 0, first); err != nil {
		t.Fatalf("append first message: %v", err)
	}
	if err := store.AppendSync("orders", 0, second); err != nil {
		t.Fatalf("append second message: %v", err)
	}
	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 0, 1, queue.Message{
		ID:        first.ID,
		Body:      first.Body,
		Tag:       first.Tag,
		Timestamp: first.Timestamp,
	}); err != nil {
		t.Fatalf("begin first processing: %v", err)
	}
	if ok, err := b.TerminateMessage(second.ID, "orders"); err != nil {
		t.Fatalf("terminate message: %v", err)
	} else if !ok {
		t.Fatalf("expected terminate to succeed")
	}
	if err := b.CommitOffset("group-a", "orders", 0, 2); err != nil {
		t.Fatalf("commit offset: %v", err)
	}

	deleted, err := b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("sweep tombstones before settled watermark: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no tombstone sweep before settled watermark is contiguous, got %d", deleted)
	}
	if !b.IsCancelled("orders", second.ID) {
		t.Fatalf("expected cancelled tombstone to remain visible")
	}

	if ok, err := b.CompleteProcessing(first.ID, "group-a", "orders"); err != nil {
		t.Fatalf("complete first processing: %v", err)
	} else if !ok {
		t.Fatalf("expected first processing to complete")
	}

	deleted, err = b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("sweep tombstones after settled watermark: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 swept tombstone after settled watermark advanced, got %d", deleted)
	}
	if b.IsCancelled("orders", second.ID) {
		t.Fatalf("expected cancelled tombstone lookup to be cleared after sweep")
	}
}

func TestSweepRetryTombstonesWaitsForRetryOffset(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	msg := queue.Message{
		ID:            uuid.NewString(),
		Body:          "cancel-retry",
		Tag:           "tag-a",
		CorrelationID: "corr-retry-cancel",
		Timestamp:     time.Now(),
	}
	retryTopic := GetRetryTopicName("group-a", "orders")
	if err := b.BeginProcessing("group-a", "orders", retryTopic, 0, 0, 1, msg); err != nil {
		t.Fatalf("begin processing: %v", err)
	}
	if ok, err := b.TerminateMessage(msg.ID, "orders"); err != nil {
		t.Fatalf("terminate message: %v", err)
	} else if !ok {
		t.Fatalf("expected terminate to succeed")
	}

	deleted, err := b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("initial retry tombstone sweep: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected retry tombstone to remain without retry offset, got %d deletions", deleted)
	}
	if !b.IsCancelled("orders", msg.ID) {
		t.Fatalf("expected retry tombstone to remain visible")
	}

	if err := b.CommitOffset("group-a", retryTopic, 0, 1); err != nil {
		t.Fatalf("commit retry offset: %v", err)
	}
	deleted, err = b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("second retry tombstone sweep: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected retry tombstone to be swept after retry offset advanced, got %d", deleted)
	}
	if b.IsCancelled("orders", msg.ID) {
		t.Fatalf("expected retry tombstone lookup to be cleared after sweep")
	}

	cancelled := b.ListCancelled("orders", 10)
	if len(cancelled) != 1 {
		t.Fatalf("expected retry cancellation history to remain available, got %d entries", len(cancelled))
	}
	if cancelled[0].StorageTopic != retryTopic {
		t.Fatalf("expected retry cancellation history to retain storage topic %s, got %s", retryTopic, cancelled[0].StorageTopic)
	}
}

func TestSweepRetryTombstonesWaitsForSettledWatermark(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	retryTopic := GetRetryTopicName("group-a", "orders")
	first := queue.Message{ID: uuid.NewString(), Body: "retry-first", Tag: "tag-a", Timestamp: time.Now()}
	second := queue.Message{ID: uuid.NewString(), Body: "retry-second", Tag: "tag-a", Timestamp: time.Now()}

	if err := b.BeginProcessing("group-a", "orders", retryTopic, 0, 0, 1, first); err != nil {
		t.Fatalf("begin first retry processing: %v", err)
	}
	if err := b.BeginProcessing("group-a", "orders", retryTopic, 0, 1, 2, second); err != nil {
		t.Fatalf("begin second retry processing: %v", err)
	}
	if ok, err := b.TerminateMessage(second.ID, "orders"); err != nil {
		t.Fatalf("terminate message: %v", err)
	} else if !ok {
		t.Fatalf("expected retry terminate to succeed")
	}
	if err := b.CommitOffset("group-a", retryTopic, 0, 2); err != nil {
		t.Fatalf("commit retry offset: %v", err)
	}

	deleted, err := b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("retry tombstone sweep before settled watermark: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected retry tombstone to remain before settled watermark is contiguous, got %d", deleted)
	}
	if !b.IsCancelled("orders", second.ID) {
		t.Fatalf("expected retry tombstone to remain visible")
	}

	if ok, err := b.CompleteProcessing(first.ID, "group-a", "orders"); err != nil {
		t.Fatalf("complete first retry processing: %v", err)
	} else if !ok {
		t.Fatalf("expected first retry processing to complete")
	}

	deleted, err = b.SweepTerminalTombstones()
	if err != nil {
		t.Fatalf("retry tombstone sweep after settled watermark: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected retry tombstone to be swept after settled watermark advanced, got %d", deleted)
	}
	if b.IsCancelled("orders", second.ID) {
		t.Fatalf("expected retry tombstone lookup to be cleared after sweep")
	}
}

func TestSweepConsumedSegmentsUsesCommittedOffsetsAndSettledEvents(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if err := b.CreateTopic("orders", TopicTypeNormal, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	first := storage.Message{ID: uuid.NewString(), Body: "first", Tag: "tag-a", Timestamp: time.Now()}
	second := storage.Message{ID: uuid.NewString(), Body: "second", Tag: "tag-a", Timestamp: time.Now()}
	store.SetCompactThreshold(1)
	if err := store.AppendSync("orders", 0, first); err != nil {
		t.Fatalf("append first message: %v", err)
	}
	store.SetCompactThreshold(10 * 1024 * 1024)
	if err := store.AppendSync("orders", 0, second); err != nil {
		t.Fatalf("append second message: %v", err)
	}

	if err := b.BeginProcessing("group-a", "orders", "orders", 0, 0, 1, queue.Message{
		ID:        first.ID,
		Body:      first.Body,
		Tag:       first.Tag,
		Timestamp: first.Timestamp,
	}); err != nil {
		t.Fatalf("begin group-a processing: %v", err)
	}
	if ok, err := b.CompleteProcessing(first.ID, "group-a", "orders"); err != nil {
		t.Fatalf("complete group-a processing: %v", err)
	} else if !ok {
		t.Fatalf("expected group-a processing to complete")
	}
	if err := b.CommitOffset("group-a", "orders", 0, 1); err != nil {
		t.Fatalf("commit group-a offset: %v", err)
	}
	if err := b.CommitOffset("group-b", "orders", 0, 1); err != nil {
		t.Fatalf("commit group-b offset: %v", err)
	}

	deleted, err := b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("sweep consumed segments before all groups settle: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no consumed segment prune while a group still lacks terminal event, got %d", deleted)
	}

	if err := b.BeginProcessing("group-b", "orders", "orders", 0, 0, 1, queue.Message{
		ID:        first.ID,
		Body:      first.Body,
		Tag:       first.Tag,
		Timestamp: first.Timestamp,
	}); err != nil {
		t.Fatalf("begin group-b processing: %v", err)
	}
	if ok, err := b.CompleteProcessing(first.ID, "group-b", "orders"); err != nil {
		t.Fatalf("complete group-b processing: %v", err)
	} else if !ok {
		t.Fatalf("expected group-b processing to complete")
	}
	deleted, err = b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("sweep consumed segments: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 consumed segment prune, got %d", deleted)
	}
	if base := b.ConsumeQueueBaseOffset("orders", 0); base != 1 {
		t.Fatalf("expected base offset 1 after consumed prune, got %d", base)
	}
	msgs, nextOffset, err := b.ReadFromConsumeQueueWithOffsets("orders", 0, 0, 10, "")
	if err != nil {
		t.Fatalf("read rebased consumequeue: %v", err)
	}
	if len(msgs) != 1 || msgs[0].ID != second.ID {
		t.Fatalf("expected second message to remain visible, got %+v", msgs)
	}
	if msgs[0].Offset != 1 || nextOffset != 2 {
		t.Fatalf("expected logical offsets [1,2), got offset=%d next=%d", msgs[0].Offset, nextOffset)
	}
	completed := b.ListCompleted("group-a", "orders", 10)
	if len(completed) != 0 {
		t.Fatalf("expected pruned completed history to be removed from current view, got %+v", completed)
	}
}

func TestSweepConsumedSegmentsSkipsNormalTopicsForEarliestStart(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()
	b.newGroupStartPosition = "earliest"

	if err := b.CreateTopic("orders", TopicTypeNormal, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	store.SetCompactThreshold(1)
	if err := store.AppendSync("orders", 0, storage.Message{ID: uuid.NewString(), Body: "first", Tag: "tag-a", Timestamp: time.Now()}); err != nil {
		t.Fatalf("append first message: %v", err)
	}
	store.SetCompactThreshold(10 * 1024 * 1024)
	if err := store.AppendSync("orders", 0, storage.Message{ID: uuid.NewString(), Body: "second", Tag: "tag-a", Timestamp: time.Now()}); err != nil {
		t.Fatalf("append second message: %v", err)
	}

	if err := b.CommitOffset("group-a", "orders", 0, 1); err != nil {
		t.Fatalf("commit offset: %v", err)
	}
	deleted, err := b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("sweep consumed segments in earliest mode: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no consumed segment prune in earliest mode, got %d", deleted)
	}
	if base := b.ConsumeQueueBaseOffset("orders", 0); base != 0 {
		t.Fatalf("expected base offset 0 in earliest mode, got %d", base)
	}
}

func TestResolveInitialOffsetUsesCompatibleTopicProgress(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if err := b.CreateTopic("orders", TopicTypeNormal, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := b.EnsureSubscription("group-a", "orders", "tag-a"); err != nil {
		t.Fatalf("ensure subscription group-a: %v", err)
	}
	if err := b.EnsureSubscription("group-b", "orders", "*"); err != nil {
		t.Fatalf("ensure subscription group-b: %v", err)
	}
	if err := b.EnsureSubscription("group-c", "orders", "tag-b"); err != nil {
		t.Fatalf("ensure subscription group-c: %v", err)
	}
	if err := b.EnsureSubscription("group-new", "orders", "tag-a"); err != nil {
		t.Fatalf("ensure subscription group-new: %v", err)
	}
	if err := b.CommitOffset("group-a", "orders", 0, 7); err != nil {
		t.Fatalf("commit offset group-a: %v", err)
	}
	if err := b.CommitOffset("group-b", "orders", 0, 5); err != nil {
		t.Fatalf("commit offset group-b: %v", err)
	}
	if err := b.CommitOffset("group-c", "orders", 0, 2); err != nil {
		t.Fatalf("commit offset group-c: %v", err)
	}

	offset, err := b.resolveInitialOffset("group-new", "orders", 0)
	if err != nil {
		t.Fatalf("resolve initial offset: %v", err)
	}
	if offset != 5 {
		t.Fatalf("expected compatible topic progress offset 5, got %d", offset)
	}
}

func TestResolveInitialOffsetFallsBackToBaseOffsetForIncompatibleSubscriptions(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	if err := b.CreateTopic("orders", TopicTypeNormal, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := b.EnsureSubscription("group-a", "orders", "tag-a"); err != nil {
		t.Fatalf("ensure subscription group-a: %v", err)
	}
	if err := b.EnsureSubscription("group-new", "orders", "tag-b"); err != nil {
		t.Fatalf("ensure subscription group-new: %v", err)
	}
	if err := b.CommitOffset("group-a", "orders", 0, 9); err != nil {
		t.Fatalf("commit offset group-a: %v", err)
	}

	offset, err := b.resolveInitialOffset("group-new", "orders", 0)
	if err != nil {
		t.Fatalf("resolve initial offset: %v", err)
	}
	if offset != 0 {
		t.Fatalf("expected base offset fallback 0, got %d", offset)
	}
}

func TestSweepConsumedSegmentsUsesRetryOwnerOffsetAndSettledEvents(t *testing.T) {
	baseDir := t.TempDir()
	dataDir := filepath.Join(baseDir, "data")

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond, time.Hour)
	defer func() {
		_ = store.Close()
	}()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer func() {
		_ = b.Close()
	}()

	retryTopic, err := b.ensureRetryTopic("group-a", "orders")
	if err != nil {
		t.Fatalf("ensure retry topic: %v", err)
	}
	first := storage.Message{ID: uuid.NewString(), Body: "first", Tag: "tag-a", Timestamp: time.Now()}
	second := storage.Message{ID: uuid.NewString(), Body: "second", Tag: "tag-a", Timestamp: time.Now()}
	store.SetCompactThreshold(1)
	if err := store.AppendSync(retryTopic, 0, first); err != nil {
		t.Fatalf("append first retry message: %v", err)
	}
	store.SetCompactThreshold(10 * 1024 * 1024)
	if err := store.AppendSync(retryTopic, 0, second); err != nil {
		t.Fatalf("append second retry message: %v", err)
	}

	deleted, err := b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("initial retry consumed prune: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no retry consumed prune without owner offset, got %d", deleted)
	}

	if err := b.CommitOffset("group-a", retryTopic, 0, 1); err != nil {
		t.Fatalf("commit retry offset: %v", err)
	}
	deleted, err = b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("retry consumed prune: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("expected no retry consumed prune before retry event is settled, got %d", deleted)
	}

	if err := b.BeginProcessing("group-a", "orders", retryTopic, 0, 0, 1, queue.Message{
		ID:        first.ID,
		Body:      first.Body,
		Tag:       first.Tag,
		Timestamp: first.Timestamp,
	}); err != nil {
		t.Fatalf("begin retry processing: %v", err)
	}
	if ok, err := b.CompleteProcessing(first.ID, "group-a", "orders"); err != nil {
		t.Fatalf("complete retry processing: %v", err)
	} else if !ok {
		t.Fatalf("expected retry processing to complete")
	}

	deleted, err = b.SweepConsumedSegments()
	if err != nil {
		t.Fatalf("retry consumed prune after settled event: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 retry consumed segment prune, got %d", deleted)
	}
	if base := b.ConsumeQueueBaseOffset(retryTopic, 0); base != 1 {
		t.Fatalf("expected retry base offset 1 after consumed prune, got %d", base)
	}
}
