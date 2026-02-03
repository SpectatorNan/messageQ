# 延迟队列架构：合并 vs 分离

## 问题：延迟队列和常规队列要分开吗？

这是一个架构设计决策，取决于你的使用场景。我已经提供了**两种实现**供你选择。

---

## 方案A：合并队列（当前实现）

### 架构图

```
生产常规消息 ──────┐
                   ├──→ orders/queue-0 ──→ 消费者
生产延迟消息 ──┐   │
             │   │
    DelayScheduler │
       (内存)     │
             │   │
       到期后 ────┘
```

### 实现文件
- **调度器**: [mq/broker/delay_scheduler.go](mq/broker/delay_scheduler.go)
- **使用**: 延迟消息到期后 `Append(topic, queueID, msg)` 写入原队列

### 代码示例

```go
// 生产延迟消息
b.EnqueueWithDelay("orders", "msg body", "tag", 5*time.Second)
// 5秒后自动写入 orders/queue-0

// 消费（无需改动）
msgs := b.ConsumeWithLock("orders", "g1", 0, 10)
// 自动包含常规消息和到期的延迟消息
```

### 优点 ✅

| 特性 | 说明 |
|------|------|
| **零改动消费逻辑** | 消费者完全无感知，现有代码继续工作 |
| **简单Offset管理** | 单一Offset，无需跟踪多个队列 |
| **低复杂度** | Broker和Consumer实现都很简单 |
| **透明性** | 消费者不需要知道消息是否延迟过 |

### 缺点 ❌

| 特性 | 说明 |
|------|------|
| **无法区分类型** | 无法统计延迟消息占比 |
| **混合限流** | 无法单独限制延迟消息速率 |
| **监控困难** | 延迟队列长度混在总队列中 |
| **顺序问题** | 延迟消息到期后插入可能打乱时序 |

### 适用场景 🎯

- ✅ 延迟消息占比 < 10%（辅助功能）
- ✅ 不需要精细统计和控制
- ✅ 消费者逻辑已经复杂，不想再修改
- ✅ 强调简单性和兼容性

---

## 方案B：分离队列（新实现）

### 架构图

```
生产常规消息 ──→ orders/queue-0 ──┐
                                 ├──→ 合并消费 ──→ 消费者
生产延迟消息 ──┐                  │
             │                  │
    DelayScheduler              │
       (内存)                   │
             │                  │
       到期后 ────→ orders.delay/queue-0 ──┘
```

### 实现文件
- **调度器**: [mq/broker/delay_scheduler_separate.go](mq/broker/delay_scheduler_separate.go) ⭐ 新增
- **合并消费**: [mq/broker/consume_merge.go](mq/broker/consume_merge.go) ⭐ 新增
- **使用**: 延迟消息到期后写入 `topic.delay`

### 代码示例

#### 方式1：消费时合并（推荐）

```go
// 生产延迟消息
b.EnqueueWithDelay("orders", "msg body", "tag", 5*time.Second)
// 5秒后自动写入 orders.delay/queue-0

// 消费：自动合并两个队列
msgs, _ := b.ConsumeWithMerge("orders", "g1", 0, MergedConsumeOptions{
    IncludeDelay:    true,  // 包含延迟队列
    MaxMessages:     10,    // 每个队列最多取10条
    SortByTimestamp: true,  // 按时间戳排序
})
// 返回 orders + orders.delay 的合并结果
```

#### 方式2：手动控制

```go
// 只消费常规队列
regularMsgs := b.ConsumeWithLock("orders", "g1", 0, 10)

// 只消费延迟队列
delayMsgs := b.ConsumeWithLock("orders.delay", "g1", 0, 10)

// 手动合并
allMsgs := append(regularMsgs, delayMsgs...)
```

### 优点 ✅

| 特性 | 说明 |
|------|------|
| **清晰隔离** | 延迟消息和常规消息物理分离 |
| **精细统计** | 可单独监控延迟队列长度和消费速度 |
| **独立限流** | 可对延迟队列设置单独的流控策略 |
| **灵活消费** | 可选择只消费常规队列或合并消费 |
| **顺序保证** | 常规队列的顺序不受延迟消息影响 |

### 缺点 ❌

| 特性 | 说明 |
|------|------|
| **消费逻辑改动** | 需要使用 `ConsumeWithMerge()` 或手动轮询 |
| **双Offset管理** | 需要跟踪两个队列的Offset |
| **复杂度提升** | Broker需要管理更多队列 |
| **存储开销** | 额外的 `.delay` topic 消耗存储 |

### 适用场景 🎯

- ✅ 延迟消息占比 > 30%（核心功能）
- ✅ 需要精细的监控和统计
- ✅ 需要对延迟消息单独限流
- ✅ 需要严格保证常规消息的顺序
- ✅ 可以接受消费逻辑的改动

---

## 迁移指南

### 从方案A切换到方案B

#### 步骤1：更换调度器

```go
// 原来
broker := NewBrokerWithStorage(store, 4)

// 改为
broker := NewBrokerWithSeparateDelayScheduler(store, 4)
```

#### 步骤2：实现构造函数

```go
// 在 broker.go 中添加
func NewBrokerWithSeparateDelayScheduler(store storage.Storage, queueCount int) *Broker {
    b := &Broker{
        queues:            make(map[string][]*queue.Queue),
        store:             store,
        queueCount:        queueCount,
        rrEnq:             make(map[string]int),
        rrDeq:             make(map[string]int),
        inflight:          make(map[string]int),
        processing:        make(map[string]processingEntry),
        stats:             make(map[string]*groupStats),
        consumeOffsetLock: make(map[string]*sync.Mutex),
    }
    // 使用分离调度器
    b.delayScheduler = NewSeparateDelayScheduler(store)
    return b
}
```

#### 步骤3：更新消费逻辑

```go
// 原来
func ConsumeHandler(c *gin.Context) {
    msgs, _ := broker.ConsumeWithLock(topic, group, queueID, 10)
    c.JSON(200, msgs)
}

// 改为
func ConsumeHandler(c *gin.Context) {
    includeDelay := c.DefaultQuery("include_delay", "true") == "true"
    
    msgs, _ := broker.ConsumeWithMerge(topic, group, queueID, MergedConsumeOptions{
        IncludeDelay:    includeDelay,
        MaxMessages:     10,
        SortByTimestamp: true,
    })
    c.JSON(200, msgs)
}
```

### 从方案B切换回方案A

只需改回原始调度器即可，延迟队列中的消息会保留（可手动清理）。

---

## 性能对比

| 指标 | 方案A（合并） | 方案B（分离） |
|------|--------------|--------------|
| **生产延迟消息** | O(log n) 内存插入 | O(log n) 内存插入 |
| **到期写入** | 1次 Append | 1次 Append（不同topic） |
| **消费性能** | 1次查询 | 2次查询 + 合并 |
| **Offset操作** | 1个Offset | 2个Offset |
| **内存占用** | 相同（只在内存暂存未到期消息） | 相同 |
| **磁盘占用** | 低（单topic） | 高（双topic） |

### 压测建议

```go
// 测试合并队列性能
func BenchmarkMergedConsume(b *testing.B) {
    for i := 0; i < b.N; i++ {
        broker.ConsumeWithLock("orders", "g1", 0, 100)
    }
}

// 测试分离队列性能
func BenchmarkSeparateConsume(b *testing.B) {
    for i := 0; i < b.N; i++ {
        broker.ConsumeWithMerge("orders", "g1", 0, MergedConsumeOptions{
            IncludeDelay: true,
            MaxMessages:  100,
        })
    }
}
```

---

## 混合方案（最佳实践）⭐

可以根据topic配置不同策略：

```go
type TopicConfig struct {
    Name                string
    UseSeparateDelayQueue bool  // 是否使用分离延迟队列
}

var topicConfigs = map[string]TopicConfig{
    "orders":       {UseSeparateDelayQueue: true},  // 订单：分离
    "notifications": {UseSeparateDelayQueue: false}, // 通知：合并
    "logs":         {UseSeparateDelayQueue: false}, // 日志：合并
}

func (b *Broker) EnqueueWithDelay(topic, body, tag string, delay time.Duration) {
    config := topicConfigs[topic]
    
    if config.UseSeparateDelayQueue {
        // 使用分离调度器
        b.separateDelayScheduler.ScheduleWithDelay(...)
    } else {
        // 使用合并调度器
        b.delayScheduler.ScheduleWithDelay(...)
    }
}
```

---

## 监控对比

### 方案A监控指标

```bash
curl http://localhost:8080/stats
```

```json
{
  "group1": {
    "processing": 5,
    "completed": 100,
    "retry": 2
  },
  "delay_scheduler": {
    "pending_messages": 10,  // 无法区分哪些是延迟消息
    "next_execution": "..."
  }
}
```

### 方案B监控指标

```bash
# 常规队列
curl http://localhost:8080/topics/orders/stats
{
  "queue_0": {"length": 100, "offset": 50},
  "queue_1": {"length": 120, "offset": 80}
}

# 延迟队列
curl http://localhost:8080/topics/orders.delay/stats
{
  "queue_0": {"length": 15, "offset": 5},  // 清晰看到延迟消息数量
  "queue_1": {"length": 20, "offset": 10}
}

# 调度器
curl http://localhost:8080/stats
{
  "delay_scheduler": {
    "pending_messages": 35,  // 内存中未到期的
    "orders.delay_written": 35,  // 已到期写入延迟队列的
    "next_execution": "..."
  }
}
```

---

## 决策树 🌳

```
是否需要区分延迟消息和常规消息？
│
├─ 否 → 方案A（合并队列）
│       - 简单、兼容性好
│       - 适合延迟消息占比低的场景
│
└─ 是 → 是否需要单独限流/监控？
        │
        ├─ 否 → 方案A（合并队列）
        │       - 虽然区分不了，但也不需要
        │
        └─ 是 → 是否可以改动消费逻辑？
                │
                ├─ 否 → 方案A（合并队列）
                │       - 被迫选择，考虑未来迁移
                │
                └─ 是 → 方案B（分离队列）⭐
                        - 最佳方案
                        - 清晰、可控、可监控
```

---

## 我的推荐 💡

### 场景判断

1. **小项目/POC/快速验证** → **方案A**  
   理由：快速上线，简单可靠

2. **生产环境/延迟消息是核心功能** → **方案B**  
   理由：长期维护更清晰，监控更精细

3. **不确定** → **先用方案A，设计上预留迁移能力**  
   理由：过早优化是万恶之源

### 实施建议

```go
// 在 Broker 中同时支持两种调度器
type Broker struct {
    // ... 其他字段
    
    delayScheduler         *DelayScheduler         // 合并方案
    separateDelayScheduler *SeparateDelayScheduler // 分离方案
    useSeparateDelay       bool                    // 配置开关
}

// 通过环境变量或配置文件控制
func NewBroker(store storage.Storage) *Broker {
    b := &Broker{...}
    
    if os.Getenv("USE_SEPARATE_DELAY_QUEUE") == "true" {
        b.separateDelayScheduler = NewSeparateDelayScheduler(store)
        b.useSeparateDelay = true
    } else {
        b.delayScheduler = NewDelayScheduler(store)
        b.useSeparateDelay = false
    }
    
    return b
}
```

---

## 总结

| 维度 | 方案A（合并） | 方案B（分离） |
|------|--------------|--------------|
| **实现复杂度** | ⭐⭐ 简单 | ⭐⭐⭐⭐ 中等 |
| **消费逻辑改动** | ✅ 无需改动 | ⚠️ 需要改动 |
| **监控精细度** | ⭐⭐ 粗粒度 | ⭐⭐⭐⭐⭐ 精细 |
| **扩展性** | ⭐⭐⭐ 中等 | ⭐⭐⭐⭐⭐ 优秀 |
| **适合场景** | 辅助功能 | 核心功能 |

**当前推荐：方案A（已实现）** → 简单高效，适合大多数场景  
**可选升级：方案B（已提供）** → 需要精细控制时切换

你可以根据实际业务需求选择或切换方案！🚀
