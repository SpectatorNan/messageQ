# 持久化延迟队列实现说明

## ✅ 已解决的问题

### 1. 延迟队列持久化 ✅
- **问题**：服务重启后，内存中的延迟消息丢失
- **解决**：`PersistentDelayScheduler` 自动持久化到 `data/delay_queue.json`
- **恢复**：服务重启后自动加载延迟消息

### 2. 重试任务持久化 ✅
- **问题**：NACK触发的重试消息在重启后丢失
- **解决**：重试通过延迟调度器实现，自动持久化
- **结果**：服务重启后重试任务继续执行

### 3. Topic类型管理 ✅
- **问题**：缺少RocketMQ风格的topic预创建
- **解决**：新增TopicManager，支持NORMAL/DELAY类型
- **持久化**：Topic配置保存到 `data/topics.json`

---

## 架构设计

### 分离队列方案（当前实现）

```
专门的延迟任务Topic
│
├─ Topic创建时指定类型: NORMAL 或 DELAY
│
├─ DELAY类型Topic
│   ├─ 生产 → DelayScheduler(内存 + 磁盘)
│   ├─ 到期 → 写回同一Topic
│   └─ 消费 → 正常消费流程
│
└─ NORMAL类型Topic
    ├─ 生产 → 直接写入CommitLog
    ├─ 重试 → DelayScheduler(指数退避)
    └─ 消费 → 正常消费流程
```

**优势：**
- ✅ 延迟任务和普通消息物理隔离
- ✅ 可单独监控延迟队列
- ✅ Topic类型一目了然
- ✅ 服务重启后状态完整恢复

---

## API使用

### 1. 创建Topic

```bash
# 创建普通Topic
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "type": "NORMAL",
    "queue_count": 4
  }'

# 创建延迟队列Topic（专门处理延迟任务）
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "delayed-tasks",
    "type": "DELAY",
    "queue_count": 8
  }'
```

**响应：**
```json
{
  "code": "ok",
  "data": {
    "name": "delayed-tasks",
    "type": "DELAY",
    "queue_count": 8
  }
}
```

### 2. 查看Topic配置

```bash
# 查看所有Topic
curl http://localhost:8080/topics

# 查看特定Topic
curl http://localhost:8080/topics/delayed-tasks
```

**响应：**
```json
{
  "code": "ok",
  "data": {
    "name": "delayed-tasks",
    "type": "DELAY",
    "queue_count": 8,
    "created_at": 1738569600
  }
}
```

### 3. 生产延迟消息到DELAY Topic

```bash
curl -X POST http://localhost:8080/topics/delayed-tasks/messages/delay \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Process this task after 30 seconds",
    "tag": "scheduled-job",
    "delay_sec": 30
  }'
```

### 4. 消费（自动处理延迟）

```bash
# 消费延迟Topic（到期的消息会自动出现）
curl "http://localhost:8080/topics/delayed-tasks/messages?group=worker&queue_id=0"
```

### 5. 删除Topic

```bash
curl -X DELETE http://localhost:8080/topics/delayed-tasks
```

---

## 持久化文件

### 文件位置

```
data/
├── topics.json           # Topic配置（类型、队列数等）
├── delay_queue.json      # 延迟消息队列（未到期的消息）
├── commitlog/            # 消息数据（已到期的消息）
│   ├── orders/
│   └── delayed-tasks/
└── consumequeue/         # 消费索引
    ├── orders/
    └── delayed-tasks/
```

### topics.json 示例

```json
[
  {
    "name": "orders",
    "type": "NORMAL",
    "queue_count": 4,
    "created_at": 1738569600
  },
  {
    "name": "delayed-tasks",
    "type": "DELAY",
    "queue_count": 8,
    "created_at": 1738569700
  }
]
```

### delay_queue.json 示例

```json
[
  {
    "message_id": "019c2..."，
    "body": "Task 1",
    "tag": "job",
    "retry": 0,
    "timestamp": 1738569800,
    "topic": "delayed-tasks",
    "queue_id": 0,
    "execute_at": 1738569830  // 30秒后执行
  },
  {
    "message_id": "019c3...",
    "body": "Retry message",
    "tag": "order",
    "retry": 1,
    "timestamp": 1738569805,
    "topic": "orders",
    "queue_id": 2,
    "execute_at": 1738569807  // 2秒后重试（指数退避）
  }
]
```

---

## 重启恢复机制

### 服务重启流程

```
1. 启动Broker
   ↓
2. 加载 topics.json → TopicManager
   ↓
3. 加载 delay_queue.json → DelayScheduler
   ↓
4. 恢复所有未到期的延迟消息到优先级队列
   ↓
5. 后台goroutine继续调度（100ms检查周期）
   ↓
6. 到期消息自动写回对应Topic
```

### 测试验证

```bash
# 运行持久化测试
cd go
go test ./example -run TestDelaySchedulerPersistence -v
go test ./example -run TestRetryPersistence -v
go test ./example -run TestTopicPersistence -v
go test ./example -run TestDelayTopicEndToEnd -v
```

---

## 完整使用示例

### 场景：订单超时自动取消系统

```bash
# Step 1: 创建延迟队列Topic
curl -X POST http://localhost:8080/topics \
  -d '{"name":"order-timeout","type":"DELAY","queue_count":4}'

# Step 2: 下单时生产30分钟延迟消息
curl -X POST http://localhost:8080/topics/order-timeout/messages/delay \
  -d '{
    "body": "{\"order_id\":\"123\",\"action\":\"cancel\"}",
    "tag": "timeout-check",
    "delay_sec": 1800
  }'

# 此时服务重启，延迟消息仍然保留！

# Step 3: 30分钟后，Worker自动消费到期消息
curl "http://localhost:8080/topics/order-timeout/messages?group=order-worker&queue_id=0"

# 返回：
# {
#   "code": "ok",
#   "data": [{
#     "id": "019c...",
#     "body": "{\"order_id\":\"123\",\"action\":\"cancel\"}",
#     "tag": "timeout-check"
#   }]
# }

# Step 4: 处理订单取消逻辑...

# Step 5: ACK确认
curl -X POST http://localhost:8080/topics/order-timeout/messages/019c.../ack
```

---

## 重试机制说明

### 普通Topic的重试持久化

```bash
# 1. 生产消息到普通Topic
curl -X POST http://localhost:8080/topics/orders/messages \
  -d '{"body":"Order data","tag":"order"}'

# 2. 消费
curl "http://localhost:8080/topics/orders/messages?group=processor&queue_id=0"

# 3. 处理失败，NACK触发重试
curl -X POST http://localhost:8080/topics/orders/messages/{id}/nack

# ⚠️ 此时服务重启
# ✅ 重试任务仍然保留在 delay_queue.json

# 4. 重启后，1秒后自动重试（指数退避：1s, 2s, 4s...）
# 消费者自动收到重试消息
```

### 退避策略

```go
// 第1次重试：1秒后
// 第2次重试：2秒后
// 第3次重试：4秒后
// 第4次重试：8秒后
// 第5次重试：16秒后
// 第6+次重试：60秒后（最大值）
// 超过3次进入DLQ
```

---

## 监控

### 查看延迟队列状态

```bash
curl http://localhost:8080/stats
```

**响应：**
```json
{
  "code": "ok",
  "data": {
    "order-processor": {
      "processing": 2,
      "completed": 150,
      "retry": 3
    },
    "delay_scheduler": {
      "pending_messages": 25,
      "persisted": true,
      "next_execution": "2026-02-03T15:30:45+08:00"
    }
  }
}
```

### 关键指标

- `pending_messages`: 内存中等待调度的消息数
- `persisted`: 是否已持久化
- `next_execution`: 下一条消息的执行时间

---

## 与RocketMQ对比

| 特性 | RocketMQ | 本实现 |
|------|----------|--------|
| **Topic预创建** | ✅ 必须预创建 | ✅ 支持预创建 |
| **Topic类型** | NORMAL/DELAY/RETRY | ✅ NORMAL/DELAY |
| **延迟队列持久化** | ✅ 磁盘 | ✅ JSON文件 |
| **重试持久化** | ✅ 专用重试Topic | ✅ 延迟调度器 |
| **服务重启恢复** | ✅ 完整恢复 | ✅ 完整恢复 |
| **延迟精度** | 18个固定级别 | ⚡ 任意延迟 |
| **实现复杂度** | 高 | 低（优先级队列）|

---

## 性能特征

### 内存占用

```
内存占用 = sizeof(DelayedMessage) × pending_messages
         ≈ 200 bytes × pending_messages
```

**示例：**
- 1万条待调度消息 ≈ 2MB内存
- 10万条待调度消息 ≈ 20MB内存

### 调度性能

- **插入复杂度**: O(log n)
- **取出复杂度**: O(log n)
- **检查周期**: 100ms
- **精度**: ±100ms

### 持久化性能

- **持久化时机**: 
  1. 新消息调度时
  2. 消息到期处理后
  3. 服务关闭时
- **持久化方式**: JSON → 临时文件 → 原子Rename
- **性能影响**: 异步，不阻塞调度

---

## 注意事项

### ⚠️ 重要提醒

1. **DELAY Topic只能通过延迟接口生产**
   ```bash
   # ✅ 正确
   POST /topics/delayed-tasks/messages/delay
   
   # ❌ 错误（会被拒绝）
   POST /topics/delayed-tasks/messages
   ```

2. **持久化文件不要手动修改**
   - 格式错误会导致启动失败
   - 修改前请备份

3. **大量延迟消息的内存考虑**
   - 超过10万条建议监控内存
   - 考虑分批调度

4. **时钟同步**
   - 使用NTP确保系统时间准确
   - 时钟跳变会影响调度精度

---

## 升级指南

### 从旧版本迁移

```bash
# 1. 停止服务
kill -TERM <broker_pid>

# 2. 备份数据
cp -r data data.backup

# 3. 更新代码
git pull

# 4. 重新编译
go build

# 5. 启动新版本
./messageQ

# 6. 验证恢复
curl http://localhost:8080/stats
```

### 旧数据兼容

- ✅ CommitLog和ConsumeQueue完全兼容
- ⚠️ 旧版本的内存延迟消息会丢失（升级前会执行完毕）
- ✅ 新版本自动创建 `topics.json` 和 `delay_queue.json`

---

## 测试覆盖

```bash
# 运行所有持久化测试
go test ./example -run Persistence -v

# 测试覆盖：
# ✅ TestDelaySchedulerPersistence - 延迟消息重启恢复
# ✅ TestRetryPersistence - 重试任务重启恢复
# ✅ TestTopicPersistence - Topic配置重启恢复
# ✅ TestDelayTopicEndToEnd - DELAY Topic完整流程
```

---

## 总结

✅ **问题已全部解决**：
1. 延迟队列持久化 → `delay_queue.json`
2. 重试任务持久化 → 通过延迟调度器自动持久化
3. Topic类型管理 → `TopicManager` + `topics.json`

✅ **生产可用**：
- 服务重启后完整恢复
- 延迟任务不丢失
- 重试任务继续执行
- 性能稳定，内存可控

✅ **符合RocketMQ设计理念**：
- Topic预创建
- 类型明确（NORMAL/DELAY）
- 持久化保证
- 优雅关闭

🚀 **Ready for Production!**
