package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/SpectatorNan/messageQ/go/mq/storage"
	"github.com/google/uuid"
)

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
