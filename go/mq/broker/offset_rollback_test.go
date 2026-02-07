//go:build ignore
// +build ignore

package broker

import (
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/logger"
	"messageQ/mq/storage"
)

// TestOffsetRollbackIsolation tests that retry messages don't affect other consumer groups
func TestOffsetRollbackIsolation(t *testing.T) {
	dataDir := t.TempDir()
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	// 创建 broker，1 秒超时（方便测试）
	b := NewBrokerWithPersistence(store, 1, dataDir)
	b.processingTimeout = 2 * time.Second
	defer b.Close()

	topicName := "test-isolation"
	group1 := "group1"
	group2 := "group2"

	// 创建 topic
	err := b.CreateTopic(topicName, TopicTypeNormal, 1)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	// 生产 5 条消息
	messageIDs := make([]string, 5)
	for i := 0; i < 5; i++ {
		msg := b.Enqueue(topicName, "test message", "test")
		messageIDs[i] = msg.ID
		logger.Info("Produced message", zap.Int("index", i), zap.String("id", msg.ID))
	}

	time.Sleep(100 * time.Millisecond)

	// group1 消费 5 条消息，不 ack
	t.Log("Group1 consuming 5 messages without ack...")
	for i := 0; i < 5; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group1 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatalf("Group1 expected message %d, got none", i)
		}
		
		// BeginProcessing 但不 ack
		b.BeginProcessing(group1, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		logger.Info("Group1 consumed", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group1 已经没有更多消息
	msgs, _, _, _ := b.ConsumeWithLock(group1, topicName, 0, "", 1)
	if len(msgs) != 0 {
		t.Errorf("Group1 should have no more messages, got %d", len(msgs))
	}

	// group2 消费，应该只看到 5 条原始消息
	t.Log("Group2 consuming...")
	group2Count := 0
	group2IDs := make(map[string]bool)
	for i := 0; i < 10; i++ { // 尝试消费 10 次，但应该只能消费到 5 条
		msgs, offset, next, err := b.ConsumeWithLock(group2, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group2 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			break // 没有更多消息
		}
		
		group2Count++
		group2IDs[msgs[0].ID] = true
		// 立即 ack
		b.BeginProcessing(group2, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group2, topicName)
		logger.Info("Group2 consumed", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group2 只看到 5 条消息（不是 10 条）
	if group2Count != 5 {
		t.Errorf("Group2 expected to consume 5 messages, got %d", group2Count)
	}

	// 验证 group2 看到的都是原始消息 ID
	for _, id := range messageIDs {
		if !group2IDs[id] {
			t.Errorf("Group2 missing message ID: %s", id)
		}
	}

	t.Log("Waiting for group1 messages to timeout (3 seconds)...")
	time.Sleep(3 * time.Second)

	// 等待 offset 回退（2 秒延迟）
	t.Log("Waiting for offset rollback (3 seconds)...")
	time.Sleep(3 * time.Second)

	// group1 应该能重新消费到这 5 条消息
	t.Log("Group1 consuming retried messages...")
	retriedCount := 0
	for i := 0; i < 5; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group1 retry consume failed: %v", err)
		}
		if len(msgs) == 0 {
			t.Logf("Group1 no message at attempt %d", i)
			break
		}
		
		retriedCount++
		// 这次 ack
		b.BeginProcessing(group1, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group1, topicName)
		logger.Info("Group1 retried", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	if retriedCount != 5 {
		t.Errorf("Group1 expected to retry 5 messages, got %d", retriedCount)
	}

	// group2 再次消费，应该没有新消息
	t.Log("Group2 consuming again (should be empty)...")
	msgs, _, _, _ = b.ConsumeWithLock(group2, topicName, 0, "", 1)
	if len(msgs) != 0 {
		t.Errorf("Group2 should have no new messages after group1 retry, got %d", len(msgs))
	}

	t.Log("✅ Offset rollback isolation test passed")
}
