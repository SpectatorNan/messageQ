# 消息处理超时机制

## 问题描述

之前系统存在两个问题：

1. **消费超时没有 ack**：消费者拿到消息后，如果既没有 ack 也没有 nack，继续消费会返回 not found
2. **Stats 显示异常**：消息一直显示在 processing 状态，没有超时回到等待消费状态

## 解决方案

添加了消息处理超时自动重试机制：

### 核心特性

1. **默认超时时间**：30 秒（可配置）
2. **检查间隔**：每 5 秒检查一次
3. **自动重试**：超时的消息自动触发 RetryProcessing
4. **状态同步**：Stats 正确反映消息状态变化

### 实现细节

#### 1. Broker 结构体新增字段

```go
type Broker struct {
    // ... 其他字段
    processingTimeout time.Duration  // 消息处理超时时间
    stopChan          chan struct{}  // 停止后台 goroutine 的信号
}
```

#### 2. 超时检查 Goroutine

在 broker 初始化时自动启动：

```go
func NewBrokerWithPersistence(...) *Broker {
    b := &Broker{
        // ...
        processingTimeout: defaultProcessingTimeout,  // 30 秒
        stopChan:          make(chan struct{}),
    }
    
    // 启动处理超时检查
    go b.checkProcessingTimeouts()
    
    return b
}
```

#### 3. 超时检查逻辑

```go
func (b *Broker) checkProcessingTimeouts() {
    ticker := time.NewTicker(5 * time.Second)  // 每 5 秒检查一次
    defer ticker.Stop()
    
    for {
        select {
        case <-b.stopChan:
            return
        case <-ticker.C:
            b.lock.Lock()
            now := time.Now()
            var timeoutMsgIDs []string
            
            // 查找超时的消息
            for msgID, entry := range b.processing {
                if entry.State == stateProcessing && 
                   now.Sub(entry.UpdatedAt) > b.processingTimeout {
                    timeoutMsgIDs = append(timeoutMsgIDs, msgID)
                    logger.Warn("Message processing timeout", ...)
                }
            }
            b.lock.Unlock()
            
            // 对超时的消息执行重试
            for _, msgID := range timeoutMsgIDs {
                b.RetryProcessing(msgID)
            }
        }
    }
}
```

#### 4. 优雅停止

在 Close 方法中停止超时检查：

```go
func (b *Broker) Close() error {
    close(b.stopChan)  // 停止超时检查
    if b.delayScheduler != nil {
        b.delayScheduler.Stop()
    }
    return nil
}
```

## 工作流程

### 正常流程（有 Ack）

```
1. Consumer 调用 ConsumeWithLock() 获取消息
   ├─ BeginProcessing() 记录为 processing 状态
   └─ Stats: Processing +1

2. Consumer 处理消息

3. Consumer 调用 Ack (CompleteProcessing)
   ├─ 更新状态为 completed
   ├─ 从 processing map 中删除
   └─ Stats: Processing -1, Completed +1
```

### 超时流程（无 Ack）

```
1. Consumer 调用 ConsumeWithLock() 获取消息
   ├─ BeginProcessing() 记录为 processing 状态
   ├─ 记录 UpdatedAt = now
   └─ Stats: Processing +1

2. Consumer 处理消息... (超过 30 秒)

3. 后台超时检查触发 (每 5 秒检查一次)
   ├─ 发现消息超时: now - UpdatedAt > 30s
   └─ 调用 RetryProcessing(msgID)

4. RetryProcessing 处理
   ├─ 更新重试计数
   ├─ 从 processing map 中删除
   ├─ Stats: Processing -1, Retry +1
   └─ 将消息重新加入延迟队列（指数退避）

5. 消息重新可消费
   └─ Consumer 再次调用 ConsumeWithLock() 可以获取到该消息
```

## 重试策略

使用指数退避算法：

```go
func CalculateRetryBackoff(retryCount int) time.Duration {
    // 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 512s
    backoffSeconds := 1 << uint(retryCount-1)
    if backoffSeconds > 512 {
        backoffSeconds = 512
    }
    return time.Duration(backoffSeconds) * time.Second
}
```

## 配置说明

### 默认值

```go
const defaultProcessingTimeout = 30 * time.Second  // 处理超时时间
const defaultMaxRetry = 3                          // 最大重试次数
```

### 检查间隔

```go
ticker := time.NewTicker(5 * time.Second)  // 每 5 秒检查一次
```

## 测试

### 超时测试

```go
t.Run("MessageTimeout", func(t *testing.T) {
    // 1. 生产消息
    msg := b.Enqueue(topicName, "test message", "test-tag")
    
    // 2. 消费但不 ack
    msgs, offset, nextOffset, _ := b.ConsumeWithLock(group, topicName, 0, "", 1)
    b.BeginProcessing(group, topicName, 0, offset, nextOffset, msgs[0])
    
    // 3. 等待超时
    time.Sleep(35 * time.Second)
    
    // 4. 验证状态
    stats := b.Stats()
    assert.Equal(t, 0, stats[group].Processing)  // 不再处理中
    assert.Equal(t, 1, stats[group].Retry)       // 已重试
    
    // 5. 再次消费
    msgs2, _, _, _ := b.ConsumeWithLock(group, topicName, 0, "", 1)
    assert.Equal(t, msg.ID, msgs2[0].ID)         // 同一条消息
    assert.Equal(t, 1, msgs2[0].Retry)           // 重试次数为 1
})
```

### Ack 测试

```go
t.Run("MessageAckBeforeTimeout", func(t *testing.T) {
    // 1. 生产和消费
    msg := b.Enqueue(topicName, "test message 2", "test-tag")
    msgs, offset, nextOffset, _ := b.ConsumeWithLock(group, topicName, 0, "", 1)
    b.BeginProcessing(group, topicName, 0, offset, nextOffset, msgs[0])
    
    // 2. 立即 ack
    b.CompleteProcessing(msgs[0].ID)
    
    // 3. 等待一段时间
    time.Sleep(10 * time.Second)
    
    // 4. 验证状态
    stats := b.Stats()
    assert.Equal(t, 0, stats[group].Processing)  // 不在处理中
    assert.Equal(t, 1, stats[group].Completed)   // 已完成
    assert.Equal(t, 0, stats[group].Retry)       // 没有重试
})
```

## 影响范围

### 新增功能

- ✅ 消息处理超时自动重试
- ✅ Stats 正确反映消息状态
- ✅ 超时消息可以重新消费

### 兼容性

- ✅ 不影响现有 API
- ✅ 不影响正常的 ack/nack 流程
- ✅ 向后兼容

## 监控与日志

### 超时日志

```
WARN  Message processing timeout
  message_id=019c2373-4cf8-7724-8838-de1589736ec1
  group=test-group
  topic=test-topic
  elapsed=35s
```

### Stats API

```bash
GET /api/v1/stats

{
  "code": "ok",
  "data": {
    "test-group": {
      "processing": 0,
      "completed": 5,
      "retry": 1
    }
  }
}
```

## 总结

通过添加消息处理超时机制，解决了以下问题：

1. **自动重试**：消费者崩溃或处理超时，消息自动重试，不会丢失
2. **状态准确**：Stats 正确反映消息的真实状态
3. **可观测性**：超时事件有日志记录，方便排查问题
4. **资源释放**：超时消息及时从 processing map 中清理，避免内存泄漏

这是一个健壮的消息队列系统必备的功能！
