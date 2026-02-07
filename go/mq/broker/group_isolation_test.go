//go:build ignore
// +build ignore

package broker

import (
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"messageQ/mq/logger"
	"messageQ/mq/queue"
	"messageQ/mq/storage"
)

// toQueueMessage converts storage.Message to queue.Message
func toQueueMessage(sm storage.Message) queue.Message {
	return queue.Message{
		ID:        sm.ID,
		Body:      sm.Body,
		Tag:       sm.Tag,
		Retry:     sm.Retry,
		Timestamp: sm.Timestamp,
	}
}

// TestGroupIsolationWithAck 测试消费者组隔离 - ack 场景
func TestGroupIsolationWithAck(t *testing.T) {
	dataDir := t.TempDir()
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	b := NewBrokerWithPersistence(store, 1, dataDir)
	defer b.Close()

	topicName := "test-ack-isolation"
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

	// group1 消费 5 条消息并 ack
	t.Log("Group1 consuming 5 messages with ack...")
	g1ConsumedIDs := make([]string, 0)
	for i := 0; i < 5; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group1 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatalf("Group1 expected message %d, got none", i)
		}
		
		g1ConsumedIDs = append(g1ConsumedIDs, msgs[0].ID)
		// BeginProcessing 并立即 ack
		b.BeginProcessing(group1, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group1, topicName)
		logger.Info("Group1 consumed and acked", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group1 已经没有更多消息
	t.Log("Verifying group1 has no more messages...")
	msgs, _, _, _ := b.ConsumeWithLock(group1, topicName, 0, "", 1)
	if len(msgs) != 0 {
		t.Errorf("Group1 should have no more messages after ack, got %d", len(msgs))
	}

	// 尝试再次消费 10 次，确保不会重复消费
	t.Log("Verifying group1 cannot re-consume acked messages...")
	for i := 0; i < 10; i++ {
		msgs, _, _, _ := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if len(msgs) != 0 {
			t.Errorf("Group1 attempt %d: should have no messages, got %d", i, len(msgs))
			t.Errorf("Unexpected message ID: %s", msgs[0].ID)
		}
	}

	// group2 消费，应该能看到所有 5 条原始消息
	t.Log("Group2 consuming (should see all 5 messages)...")
	g2ConsumedIDs := make([]string, 0)
	for i := 0; i < 10; i++ { // 尝试消费 10 次
		msgs, offset, next, err := b.ConsumeWithLock(group2, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group2 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			break // 没有更多消息
		}
		
		g2ConsumedIDs = append(g2ConsumedIDs, msgs[0].ID)
		// 立即 ack
		b.BeginProcessing(group2, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group2, topicName)
		logger.Info("Group2 consumed and acked", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group2 消费到 5 条消息
	if len(g2ConsumedIDs) != 5 {
		t.Errorf("Group2 expected to consume 5 messages, got %d", len(g2ConsumedIDs))
	}

	// 验证 group1 和 group2 消费到的是同样的消息
	if len(g1ConsumedIDs) != len(g2ConsumedIDs) {
		t.Errorf("Group1 and Group2 consumed different number of messages: %d vs %d", 
			len(g1ConsumedIDs), len(g2ConsumedIDs))
	}

	for i := 0; i < len(g1ConsumedIDs) && i < len(g2ConsumedIDs); i++ {
		if g1ConsumedIDs[i] != g2ConsumedIDs[i] {
			t.Errorf("Message %d mismatch: g1=%s, g2=%s", i, g1ConsumedIDs[i], g2ConsumedIDs[i])
		}
	}

	t.Log("✅ Group isolation with ack test passed")
}

// TestGroupIsolationWithTimeout 测试消费者组隔离 - 超时重试场景
func TestGroupIsolationWithTimeout(t *testing.T) {
	dataDir := t.TempDir()
	defer os.RemoveAll(dataDir)

	store := storage.NewWALStorage(dataDir, 10*time.Millisecond)
	defer store.Close()

	// 创建 broker，2 秒超时
	b := NewBrokerWithPersistence(store, 1, dataDir)
	b.processingTimeout = 2 * time.Second
	defer b.Close()

	topicName := "test-timeout-isolation"
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

	// group1 消费 5 条消息，不 ack（模拟超时）
	t.Log("Group1 consuming 5 messages without ack...")
	g1FirstConsumedIDs := make([]string, 0)
	for i := 0; i < 5; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group1 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			t.Fatalf("Group1 expected message %d, got none", i)
		}
		
		g1FirstConsumedIDs = append(g1FirstConsumedIDs, msgs[0].ID)
		// BeginProcessing 但不 ack
		b.BeginProcessing(group1, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		logger.Info("Group1 consumed without ack", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group1 暂时没有更多消息
	msgs, _, _, _ := b.ConsumeWithLock(group1, topicName, 0, "", 1)
	if len(msgs) != 0 {
		t.Errorf("Group1 should have no more messages before timeout, got %d", len(msgs))
	}

	// group2 消费，应该只看到 5 条原始消息（不是 10 条）
	t.Log("Group2 consuming (should see only 5 original messages)...")
	g2ConsumedIDs := make([]string, 0)
	for i := 0; i < 10; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group2, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group2 consume failed: %v", err)
		}
		if len(msgs) == 0 {
			break
		}
		
		g2ConsumedIDs = append(g2ConsumedIDs, msgs[0].ID)
		b.BeginProcessing(group2, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group2, topicName)
		logger.Info("Group2 consumed and acked", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group2 只消费到 5 条消息（不是 10 条）- 这是核心验证点
	if len(g2ConsumedIDs) != 5 {
		t.Errorf("Group2 expected 5 messages, got %d (should not see retry duplicates)", len(g2ConsumedIDs))
		t.Errorf("This proves that retry mechanism does not duplicate messages for other groups")
	} else {
		t.Log("✅ Key validation: Group2 sees only 5 messages, no duplicates from group1 retry")
	}

	// 验证消息 ID 匹配
	for i := 0; i < len(messageIDs); i++ {
		if i < len(g2ConsumedIDs) && g2ConsumedIDs[i] != messageIDs[i] {
			t.Errorf("Group2 message %d ID mismatch: expected %s, got %s", 
				i, messageIDs[i], g2ConsumedIDs[i])
		}
	}

	// 等待超时（timeout 是 2 秒，checkProcessingTimeouts 每 5 秒检查一次）
	// 需要等待：消息处理超时（2秒）+ ticker触发（最多5秒）+ buffer
	t.Log("Waiting for timeout check (10 seconds)...")
	time.Sleep(10 * time.Second)

	// 检查 stats
	stats := b.Stats()
	t.Logf("Stats after timeout: %+v", stats[group1])

	// 等待 offset 回退
	t.Log("Waiting for offset rollback (3 seconds)...")
	time.Sleep(3 * time.Second)

	// 检查 offset
	offset, ok, err := b.GetOffset(group1, topicName, 0)
	t.Logf("Group1 offset after rollback: offset=%d, ok=%v, err=%v", offset, ok, err)

	// group1 应该能重新消费到这 5 条消息
	t.Log("Group1 consuming retried messages...")
	g1RetryConsumedIDs := make([]string, 0)
	for i := 0; i < 5; i++ {
		msgs, offset, next, err := b.ConsumeWithLock(group1, topicName, 0, "", 1)
		if err != nil {
			t.Fatalf("Group1 retry consume failed: %v", err)
		}
		if len(msgs) == 0 {
			t.Logf("Group1 no message at retry attempt %d", i)
			break
		}
		
		g1RetryConsumedIDs = append(g1RetryConsumedIDs, msgs[0].ID)
		// 这次 ack
		b.BeginProcessing(group1, topicName, 0, offset, next, toQueueMessage(msgs[0]))
		b.CompleteProcessing(msgs[0].ID, group1, topicName)
		logger.Info("Group1 retried and acked", zap.Int("index", i), zap.String("id", msgs[0].ID))
	}

	// 验证 group1 重试消费到的是同样的 5 条消息
	if len(g1RetryConsumedIDs) != 5 {
		t.Errorf("Group1 retry expected 5 messages, got %d", len(g1RetryConsumedIDs))
	}

	for i := 0; i < len(g1FirstConsumedIDs) && i < len(g1RetryConsumedIDs); i++ {
		if g1FirstConsumedIDs[i] != g1RetryConsumedIDs[i] {
			t.Errorf("Group1 retry message %d mismatch: first=%s, retry=%s", 
				i, g1FirstConsumedIDs[i], g1RetryConsumedIDs[i])
		}
	}

	// group2 再次消费，应该没有新消息
	t.Log("Group2 consuming again (should be empty)...")
	msgs, _, _, _ = b.ConsumeWithLock(group2, topicName, 0, "", 1)
	if len(msgs) != 0 {
		t.Errorf("Group2 should have no new messages after group1 retry, got %d", len(msgs))
	}

	t.Log("✅ Group isolation with timeout test passed")
}
