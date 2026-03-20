# API 接口文档

## 概述

所有API响应遵循统一格式：

```json
{
  "code": "ok",
  "message": "success",
  "data": { ... }
}
```

错误响应：
```json
{
  "code": "error_code",
  "message": "error description",
  "data": null
}
```

---

## 消息生产与消费

### 1. 发送普通消息

**POST** `/topics/:topic/messages`

**请求体：**
```json
{
  "body": "message content",
  "tag": "message-tag",
  "correlationId": "order-1001"
}
```

**参数校验：**
- `body`: 必填，不能为空字符串
- `tag`: 必填
- `correlationId`: 可选，UTF-8 字节数不能超过 128
- `topic`: 不能包含空格、制表符、换行符、`/`、`\`，长度不超过255

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "topic": "orders",
    "tag": "electronics",
    "correlationId": "order-1001",
    "body": "Order #1001: iPhone 15 Pro",
    "timestamp": "2026-02-03T16:14:50.123Z",
    "retry": 0
  }
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Order #1001: iPhone 15 Pro",
    "tag": "electronics",
    "correlationId": "order-1001"
  }'
```

---

### 2. 发送延迟消息

**POST** `/topics/:topic/messages`

**请求体：**
```json
{
  "body": "message content",
  "tag": "message-tag",
  "correlationId": "order-1001-delay",
  "delaySec": 10,     // 延迟秒数（二选一）
  "delayMs": 10000    // 延迟毫秒数（二选一）
}
```

**参数校验：**
- `body`: 必填，不能为空字符串
- `tag`: 必填
- `correlationId`: 可选，UTF-8 字节数不能超过 128
- `delaySec` 或 `delayMs`: 必须指定一个，不能同时指定
- 延迟时间范围：0 ~ 30天
- `topic`: 同上

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "topic": "orders",
    "tag": "electronics",
    "correlationId": "order-1001-delay",
    "scheduledAt": "2026-02-03T16:14:50.123Z",
    "executeAt": "2026-02-03T16:15:00.123Z",
    "delaySeconds": 10.0,
    "delayMs": 10000
  }
}
```

**示例：**
```bash
# 延迟10秒
curl -X POST http://localhost:8080/topics/notifications/messages/delay \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Welcome email",
    "tag": "email",
    "correlationId": "welcome-email-1",
    "delaySec": 10
  }'

# 延迟5分钟
curl -X POST http://localhost:8080/topics/notifications/messages/delay \
  -H "Content-Type: application/json" \
  -d '{
    "body": "Password reset link",
    "tag": "security",
    "correlationId": "password-reset-1",
    "delayMs": 300000
  }'
```

---

### 3. 消费消息

**GET** `/topics/:topic/messages?group=:group&queue_id=:queue_id&tag=:tag`

**查询参数：**
- `group`: 必填，消费者组名称
- `queue_id`: 可选，队列ID（默认0）
- `tag`: 可选，消息标签过滤

**参数校验：**
- `group`: 必填，不能包含空格等特殊字符，长度不超过255
- `queue_id`: 必须 >= 0
- `topic`: 同上

**行为说明：**
- 新 consumer group 首次消费普通 topic 时，offset 会初始化到当前 queue tail（`latest`），不会回放历史消息
- 同一 `group + topic` 的订阅 tag 必须一致；一旦某个 group 以某个 tag 建立订阅，后续再用不同 tag 消费会返回 `subscription_conflict`
- `""` 与 `*` 会被视为同一个“全量订阅”标签
- 如果 broker 无法持久化本次 `processing` 状态，消费接口会返回 `internal_error`，并把本次已取出的消息回滚为 retry，而不是静默吞掉该错误
- 如果 broker 无法持久化本次 `retry` / `cancelled` / `expired` 状态，相关 `nack`、`terminate` 或状态查询接口也会返回 `internal_error`，而不是静默报告成功

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "message": {
      "id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
      "body": "Order #1001",
      "tag": "electronics",
      "correlationId": "order-1001",
      "timestamp": "2026-02-03T16:14:50.123Z",
      "retry": 0
    },
    "group": "consumer-group-1",
    "topic": "orders",
    "queue_id": 0,
    "offset": 100,
    "next_offset": 101,
    "state": "processing"
  }
}
```

**示例：**
```bash
curl "http://localhost:8080/topics/orders/messages?group=consumer-g1&queue_id=0"

# 按tag过滤
curl "http://localhost:8080/topics/orders/messages?group=consumer-g1&tag=electronics"
```

---

### 4. 确认消息（ACK）

**POST** `/topics/:topic/messages/:id/ack`

**参数校验：**
- `id`: 必须是有效的UUID格式
- `topic`: 同上

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "message_id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "acked": true,
    "topic": "orders"
  }
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/topics/orders/messages/019c2291-8fbd-7c0a-8e0a-3b262bf11e96/ack
```

---

### 5. 拒绝消息（NACK）

**POST** `/topics/:topic/messages/:id/nack`

**参数校验：**
- `id`: 必须是有效的UUID格式
- `topic`: 同上

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "message_id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "nacked": true,
    "topic": "orders",
    "requeued": true
  }
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/topics/orders/messages/019c2291-8fbd-7c0a-8e0a-3b262bf11e96/nack
```

---

### 5.1 终止消息（Terminate）

**POST** `/api/v1/topics/:topic/messages/:id/terminate`

**参数校验：**
- `id`: 必须是有效的 UUID 格式
- `topic`: 必填，符合 Topic 命名规则

**行为说明：**
- 终止成功后，消息状态记为 `cancelled`
- 终止是 **topic 级全局取消**：不区分消费组，同一条消息对所有消费组都不可再执行
- 终止操作幂等：重复终止同一消息、或消息不存在时，都会返回成功
- `cancelled` 消息会在消费路径、pending/scheduled 列表中被过滤，确保不可再消费

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "messageId": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "terminated": true,
    "topic": "orders",
    "state": "cancelled"
  }
}
```

**示例：**
```bash
curl -X POST \
  http://localhost:8080/api/v1/topics/orders/messages/019c2291-8fbd-7c0a-8e0a-3b262bf11e96/terminate
```

---

### 5.2 批量终止消息

**POST** `/api/v1/topics/:topic/terminate/batch`

**请求体：**
```json
{
  "messageIds": [
    "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
    "019c2291-8fbd-7c0a-8e0a-3b262bf11e97"
  ]
}
```

**参数校验：**
- `topic`: 必填，符合 Topic 命名规则
- `messageIds`: 必填且不能为空
- `messageIds` 中每个元素都必须是有效 UUID

**行为说明：**
- 批量终止同样是 **topic 级全局取消**
- 每个 messageId 都按单条 terminate 的幂等规则处理

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "messageIds": [
      "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
      "019c2291-8fbd-7c0a-8e0a-3b262bf11e97"
    ],
    "terminatedCount": 2,
    "topic": "orders",
    "state": "cancelled"
  }
}
```

**示例：**
```bash
curl -X POST \
  http://localhost:8080/api/v1/topics/orders/terminate/batch \
  -H 'Content-Type: application/json' \
  -d '{
    "messageIds": [
      "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
      "019c2291-8fbd-7c0a-8e0a-3b262bf11e97"
    ]
  }'
```

---

### 5.3 查询消息状态

**GET** `/api/v1/consumers/:group/topics/:topic/messages/status?state=:state&queueId=:queueId&tag=:tag`

**支持状态：**
- `pending`
- `scheduled`
- `processing`
- `completed`
- `retry`
- `dlq`
- `cancelled`
- `expired`

**说明：**
- `pending` / `scheduled` / `processing` / `completed` / `retry` / `dlq` / `cancelled` / `expired` 响应里的消息对象都会返回 `correlationId`
- 查询 `cancelled` 可看到 topic 上已终止消息；该视图不区分消费组，同一 topic 的任意 group 查询结果一致
- 查询 `expired` 可看到超过保留窗口后被系统自动终止的消息；这些消息也可能在后台 retention sweep 删除旧 segment 前被补记为 `expired`
- 对于长期未 ack 的 `processing` 消息，若其原始消息时间已经超过过期窗口，则超时检查会直接将其记为 `expired`，而不是继续无限重试
- 查询 `retry` / `dlq` 返回该 `group+topic` 的**最新状态视图**：broker 会按每条消息的最新 delivery event 做归约，只返回当前最新事件仍为 `retry` 或 `dlq` 的消息，而不是简单回放全部历史；`retry` 事件会返回 `eventAt`（进入重试的时间）和 `scheduledAt`（下一次计划投递时间），并且包含 broker 重启恢复时补发的 retry 事件；`dlq` 事件会返回 `eventAt`（进入死信队列的时间）
- `processing` 文件现在只作为 `processing` delivery event 写入前的 crash-window fallback；一旦 `processing` event 已成功写盘，fallback 文件会被立即删除，后续当前态查询与启动恢复都会优先使用 delivery events
- delivery event log 中带 queue/offset 的记录会随着对应 `commitlog + consumequeue` 前缀一起裁剪：当 retention prune 或 consumed prune 推进了 queue 的 `baseOffset` 后，位于该前缀之前的 `ack/retry/dlq/cancelled/expired` 事件也会被同步删除，因此这些状态查询只对当前保留窗口内的数据提供历史
- 被终止或过期的消息不会出现在 `pending`、`scheduled` 以及实际消费结果中

**`cancelled` 响应示例：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "group": "consumer-g1",
    "topic": "orders",
    "state": "cancelled",
    "messages": [
      {
        "id": "019c2291-8fbd-7c0a-8e0a-3b262bf11e96",
        "body": "Order #1001",
        "tag": "electronics",
        "correlationId": "order-1001",
        "timestamp": "2026-02-03T16:14:50.123Z"
      }
    ]
  }
}
```

---

## Topic 管理

### 6. 创建 Topic

**POST** `/topics`

**请求体：**
```json
{
  "name": "orders",
  "type": "NORMAL",     // NORMAL 或 DELAY
  "queue_count": 8      // 可选，默认4
}
```

**参数校验：**
- `name`: 必填，不能包含空格等特殊字符，长度不超过255
- `type`: 必填，只能是 `NORMAL` 或 `DELAY`
- `queue_count`: 可选，范围 1-128

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "name": "orders",
    "type": "NORMAL",
    "queue_count": 8,
    "created_at": 1738575600
  }
}
```

**示例：**
```bash
# 创建普通Topic
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders",
    "type": "NORMAL",
    "queue_count": 8
  }'

# 创建延迟Topic
curl -X POST http://localhost:8080/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "delayed-tasks",
    "type": "DELAY",
    "queue_count": 4
  }'
```

---

### 7. 获取 Topic 信息

**GET** `/topics/:topic`

**参数校验：**
- `topic`: 必填，符合命名规则

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "name": "orders",
    "type": "NORMAL",
    "queue_count": 8,
    "created_at": 1738575600
  }
}
```

**示例：**
```bash
curl http://localhost:8080/topics/orders
```

---

### 8. 列出所有 Topics

**GET** `/topics`

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "topics": [
      {
        "name": "orders",
        "type": "NORMAL",
        "queue_count": 8,
        "created_at": 1738575600
      },
      {
        "name": "notifications",
        "type": "DELAY",
        "queue_count": 4,
        "created_at": 1738575650
      }
    ],
    "count": 2
  }
}
```

**示例：**
```bash
curl http://localhost:8080/topics
```

---

### 9. 删除 Topic

**DELETE** `/topics/:topic`

**参数校验：**
- `topic`: 必填，符合命名规则

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "topic": "orders",
    "deleted": true
  }
}
```

**示例：**
```bash
curl -X DELETE http://localhost:8080/topics/orders
```

---

## Offset 管理

### 10. 获取消费进度

**GET** `/topics/:topic/offsets/:group?queue_id=:queue_id`

**查询参数：**
- `queue_id`: 可选，队列ID（默认0）

**参数校验：**
- `topic`: 必填，符合命名规则
- `group`: 必填，符合命名规则
- `queue_id`: 必须 >= 0

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "group": "consumer-g1",
    "topic": "orders",
    "queue_id": 0,
    "offset": 100
  }
}
```

如果没有 offset 记录，说明该 group 还未初始化到该 queue；首次消费普通 topic 时会自动从 `latest` 初始化并写入 offset。

当后台 retention sweep 已经裁掉 queue 头部时，返回的 offset 会自动 rebased 到当前仍可读取的最早逻辑 offset，不会落回已经过期删除的 head。

**示例：**
```bash
curl "http://localhost:8080/topics/orders/offsets/consumer-g1?queue_id=0"
```

---

### 11. 提交消费进度

**POST** `/topics/:topic/offsets/:group`

**请求体：**
```json
{
  "queue_id": 0,
  "offset": 100
}
```

**参数校验：**
- `topic`: 必填，符合命名规则
- `group`: 必填，符合命名规则
- `queue_id`: 必须 >= 0
- `offset`: 必填，必须 >= 0

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "group": "consumer-g1",
    "topic": "orders",
    "queue_id": 0,
    "offset": 100,
    "committed": true
  }
}
```

**示例：**
```bash
curl -X POST http://localhost:8080/topics/orders/offsets/consumer-g1 \
  -H "Content-Type: application/json" \
  -d '{
    "queue_id": 0,
    "offset": 100
  }'
```

如果提交的 offset 小于当前 queue 的保留起点，服务端会自动将其提升到当前最早可读 offset；响应里的 `offset` 字段返回的是实际提交后的值。

---

## 统计信息

### 12. 获取统计信息

**GET** `/stats`

**响应：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "groups": {
      "consumer-g1": {
        "processing": 5,
        "completed": 1000,
        "failed": 10
      }
    },
    "delay_scheduler": {
      "pending_messages": 150,
      "storage_type": "binary_commitlog",
      "system_topic": "__DELAY_TOPIC__"
    },
    "timestamp": "2026-02-03T16:30:45.123Z"
  }
}
```

**示例：**
```bash
curl http://localhost:8080/stats
```

---

## 错误码列表

| 错误码 | 描述 |
|--------|------|
| `missing_topic` | 缺少topic参数 |
| `internal_error` | 服务端内部错误，例如无法持久化 processing / retry / cancelled / expired 等状态 |
| `invalid_message` | 消息格式无效或body为空 |
| `invalid_id` | 消息ID格式无效（非UUID） |
| `not_found` | 消息未找到或已处理 |
| `invalid_group` | 消费者组名无效 |
| `invalid_offset` | offset值无效 |
| `offset_unsupported` | 不支持offset存储 |
| `subscription_conflict` | 同一消费者组在同一 topic 上使用了不一致的 tag 订阅 |
| `missing_tag` | 缺少tag参数 |
| `busy` | 消息正在处理中 |
| `invalid_delay` | 延迟参数无效 |
| `invalid_topic_type` | Topic类型无效 |
| `topic_exists` | Topic已存在 |
| `invalid_queue_id` | 队列ID无效 |
| `topic_not_found` | Topic不存在 |
| `invalid_topic_name` | Topic名称无效 |

---

## 参数校验规则

### Topic 名称
- 长度：1-255字符
- 不能包含：空格、制表符、换行符、`/`、`\`
- 示例：`orders`, `user-events`, `payment_notifications`

### Group 名称
- 长度：1-255字符
- 不能包含：空格、制表符、换行符、`/`、`\`
- 示例：`consumer-group-1`, `worker_pool`, `email-processor`

### 消息体（Body）
- 必填
- 去除首尾空格后不能为空
- 无长度限制（受系统内存限制）

### 延迟时间
- `delay_sec`: 0 ~ 2592000（30天）
- `delay_ms`: 0 ~ 2592000000（30天）
- 只能指定一个参数

### Queue Count
- 范围：1-128
- 默认：4

---

## 使用建议

### 1. 错误处理
```javascript
const response = await fetch('/topics/orders/messages', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ body: 'Order #1', tag: 'new' })
});

const result = await response.json();

if (result.code === 'ok') {
  console.log('Success:', result.data);
} else {
  console.error('Error:', result.code, result.message);
}
```

### 2. 消费-ACK 模式
```bash
# 1. 消费消息
MSG=$(curl -s "http://localhost:8080/topics/orders/messages?group=g1")
MSG_ID=$(echo $MSG | jq -r '.data.message.id')

# 2. 处理消息
process_message()

# 3. 确认消息
curl -X POST "http://localhost:8080/topics/orders/messages/$MSG_ID/ack"
```

### 3. 延迟任务调度
```python
import requests
import json

# 10秒后发送通知
response = requests.post(
    'http://localhost:8080/topics/notifications/messages/delay',
    json={
        'body': 'Order confirmed',
        'tag': 'order-notification',
        'delay_sec': 10
    }
)

print(response.json())
```
