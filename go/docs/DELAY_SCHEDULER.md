# 延迟调度功能说明

## 功能概述

实现了基于优先级队列的延迟调度系统，同时支持：
1. **延迟消息** - 生产时指定延迟时间，到期后才能被消费
2. **重试退避** - NACK后自动按指数退避策略延迟重试

## 核心组件

### DelayScheduler ([delay_scheduler.go](../mq/broker/delay_scheduler.go))

基于最小堆的优先级队列调度器：

```go
type DelayScheduler struct {
    store      storage.Storage
    delayQueue DelayQueue  // 最小堆，按ExecuteAt排序
    ticker     *time.Ticker // 100ms检查周期
}
```

**关键特性：**
- ✅ 最小堆实现O(log n)插入和取出
- ✅ 100ms精度的定时检查
- ✅ 自动将到期消息写回原队列
- ✅ 线程安全

### 退避策略

```go
func CalculateRetryBackoff(retryCount int) time.Duration {
    // 指数退避: 1s, 2s, 4s, 8s, 16s, 最大60s
    seconds := math.Pow(2, float64(retryCount))
    if seconds > 60 {
        seconds = 60
    }
    return time.Duration(seconds) * time.Second
}
```

## API 使用

### 1. 生产延迟消息

```bash
# 延迟2秒
curl -X POST http://localhost:8080/topics/orders/messages/delay \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Process order after 2s",
    "tag": "order", 
    "delay_sec": 2
  }'

# 延迟5000毫秒
curl -X POST http://localhost:8080/topics/notifications/messages/delay \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Send notification", 
    "tag": "email",
    "delay_ms": 5000
  }'
```

**响应示例：**
```json
{
  "code": 0,
  "data": {
    "id": "019c21e6-288d-7971-b3a2-f9fa5fe6e8fe",
    "scheduled_at": "2026-02-03T13:07:37.748Z",
    "execute_at": "2026-02-03T13:07:39.748Z",
    "delay_seconds": 2
  }
}
```

### 2. 消费（无需改动）

```bash
# 消费逻辑完全不变
curl "http://localhost:8080/topics/orders/messages?group=order-processor&queue_id=0"
```

### 3. 重试（自动延迟）

```bash
# NACK后自动触发延迟重试
curl -X POST http://localhost:8080/topics/orders/messages/{id}/nack

# 第1次重试：~1s后
# 第2次重试：~2s后  
# 第3次重试：~4s后
# 超过3次 → DLQ
```

### 4. 查看调度器状态

```bash
curl http://localhost:8080/stats
```

**响应示例：**
```json
{
  "code": 0,
  "data": {
    "order-processor": {
      "processing": 2,
      "completed": 15,
      "retry": 1
    },
    "delay_scheduler": {
      "pending_messages": 3,
      "next_execution": "2026-02-03T13:12:16.657+08:00"
    }
  }
}
```

## 编程接口

### 生产延迟消息

```go
import (
    "time"
    "messageQ/mq/broker"
)

b := broker.NewBrokerWithStorage(store, 4)
defer b.Close() // 关闭时自动停止调度器

// 生产5秒延迟的消息
msg := b.EnqueueWithDelay(
    "orders",           // topic
    "process order #123", // body
    "order",            // tag
    5*time.Second,      // delay
)
```

### 自定义退避策略

```go
// 修改 RetryProcessing 中的退避计算
delay := broker.CalculateRetryBackoff(retryCount)

// 或自定义
delay := time.Duration(retryCount * retryCount) * time.Second // 平方退避
```

## 工作流程

### 延迟消息流程

```
生产 → DelayScheduler.Schedule() → 加入优先级队列
                                          ↓
                            定时检查(100ms)到期消息
                                          ↓
                            Append到原topic/queue
                                          ↓
                            消费者正常消费
```

### 重试流程

```
消费 → NACK → RetryProcessing()
                    ↓
        计算退避延迟(2^n秒)
                    ↓
        DelayScheduler.ScheduleWithDelay()
                    ↓
        到期后自动Append回队列
                    ↓
        下次消费时获取(retry字段递增)
```

## 架构优势

### vs. 立即追加 (旧方案)

| 特性 | 旧方案 | 新方案 |
|------|--------|--------|
| 重试延迟 | ❌ 立即重试 | ✅ 指数退避 |
| 存储开销 | 立即写commitlog | 内存暂存，延迟写入 |
| 延迟消息 | ❌ 不支持 | ✅ 原生支持 |
| 消息风暴 | ⚠️ 立即重试可能雪崩 | ✅ 延迟缓解压力 |

### vs. 专门重试队列

| 特性 | 专门重试队列 | 延迟调度 |
|------|-------------|---------|
| 实现复杂度 | 高（需要双队列合并） | 低（单队列+调度器） |
| 消费逻辑 | 需修改（轮询两个队列） | 无需修改 |
| Offset管理 | 复杂（双offset） | 简单（单offset） |
| 延迟功能 | 额外实现 | 统一实现 |

## 性能特征

### 时间复杂度

- **插入延迟消息**: O(log n)
- **取出到期消息**: O(log n) 
- **检查到期**: O(1) - 只看堆顶

### 空间复杂度

- **内存占用**: O(pending_messages) - 只存储未到期消息
- **到期后**: 消息移到commitlog，内存释放

### 调度精度

- **检查周期**: 100ms
- **实际延迟**: target_delay ± 100ms
- **可配置**: 修改`time.NewTicker(100 * time.Millisecond)`

## 配置建议

### 调整检查频率

```go
// 更高精度（10ms，适合实时场景）
ds.ticker = time.NewTicker(10 * time.Millisecond)

// 更低开销（1s，适合长延迟场景）
ds.ticker = time.NewTicker(1 * time.Second)
```

### 调整最大退避

```go
// 在 CalculateRetryBackoff 中修改
if seconds > 300 { // 最大5分钟
    seconds = 300
}
```

## 测试覆盖

### 新增测试

1. **TestDelayedMessage** - 基本延迟消息功能
2. **TestRetryWithBackoff** - 重试退避验证
3. **TestMultipleDelayedMessages** - 多消息调度顺序
4. **TestStatefulRetryOrder** - 更新以支持延迟重试

### 运行测试

```bash
# 运行所有测试
go test ./... -v

# 只测试延迟功能
go test ./example -run TestDelayed -v
go test ./example -run TestRetry -v

# 压力测试
go test ./example -run TestMultiple -v
```

## 监控指标

### 关键指标

```go
stats := broker.GetDelayScheduler().Stats()

// pending_messages: 待调度消息数
// next_execution: 下次调度时间
```

**告警建议：**
- `pending_messages > 10000` → 消费速度过慢
- `next_execution` 长期延后 → 调度器可能阻塞

## 使用场景

### 1. 订单超时自动取消

```go
// 下单时生产30分钟延迟消息
b.EnqueueWithDelay("order-timeout", orderID, "timeout", 30*time.Minute)

// 30分钟后自动消费检查订单状态
```

### 2. 定时任务调度

```go
// 凌晨2点执行任务
now := time.Now()
target := time.Date(now.Year(), now.Month(), now.Day()+1, 2, 0, 0, 0, now.Location())
delay := target.Sub(now)

b.EnqueueWithDelay("daily-task", "generate report", "task", delay)
```

### 3. 限流和削峰

```go
// 批量操作时添加延迟，避免瞬时高峰
for i, item := range items {
    delay := time.Duration(i*100) * time.Millisecond
    b.EnqueueWithDelay("batch-process", item, "batch", delay)
}
```

### 4. 重试雪崩保护

```go
// NACK自动触发指数退避，避免立即重试导致的雪崩效应
// 1s → 2s → 4s → 8s → 16s → 60s → DLQ
```

## 注意事项

⚠️ **进程重启**  
内存中未到期的延迟消息会丢失。如需持久化，可以：
1. 定期序列化DelayQueue到磁盘
2. 使用专门的延迟队列topic持久化

⚠️ **时钟变化**  
系统时间调整会影响调度精度。建议使用NTP同步。

⚠️ **大量延迟消息**  
内存占用随pending_messages线性增长。建议：
- 监控pending_messages指标
- 对长延迟消息考虑外部调度系统

## 未来优化方向

1. **分级时间轮** - 支持更大规模延迟消息
2. **持久化延迟队列** - 支持进程重启恢复
3. **分布式调度** - 支持多节点共享延迟队列
4. **优先级调度** - 支持消息优先级

---

## 快速开始

```bash
# 1. 启动服务
cd /Users/spectator/Documents/GitHub/nan/messageQ/go
go run .

# 2. 生产延迟消息
curl -X POST http://localhost:8080/topics/test/messages/delay \
  -H "Content-Type: application/json" \
  -d '{"body":"hello","tag":"demo","delay_sec":5}'

# 3. 5秒后消费
curl "http://localhost:8080/topics/test/messages?group=g1&queue_id=0"

# 4. 查看统计
curl http://localhost:8080/stats
```

🎉 **完成！延迟调度功能已集成到MQ系统中。**
