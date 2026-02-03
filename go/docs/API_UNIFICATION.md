# API 统一优化文档

## 概述

本次优化将 `produce` 和 `produceDelay` 两个 API 合并为一个统一的接口，通过可选参数来支持延迟消息功能。

## 改动内容

### 1. 路由变更

#### 之前
```
POST /api/v1/topics/:topic/messages          # 普通消息
POST /api/v1/topics/:topic/delayed-messages  # 延迟消息
```

#### 现在
```
POST /api/v1/topics/:topic/messages          # 统一接口，支持普通和延迟消息
```

### 2. 请求格式

#### 普通消息（无延迟）
```json
POST /api/v1/topics/:topic/messages
{
  "body": "message content",
  "tag": "message-tag"
}
```

**响应示例：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "id": "019c2311-e621-7a35-9f79-83df93b43bb9",
    "topic": "test-topic",
    "tag": "message-tag",
    "body": "message content",
    "timestamp": "2026-02-03T18:35:01+08:00",
    "retry": 0
  }
}
```

#### 延迟消息（使用 delay_ms）
```json
POST /api/v1/topics/:topic/messages
{
  "body": "delayed message",
  "tag": "delay-tag",
  "delay_ms": 5000  // 延迟 5000 毫秒（5 秒）
}
```

**响应示例：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "id": "019c2311-e621-7db7-8d1f-7f962212c56c",
    "topic": "test-topic",
    "tag": "delay-tag",
    "scheduled_at": "2026-02-03T18:35:01+08:00",
    "execute_at": "2026-02-03T18:35:06+08:00",
    "delay_seconds": 5.0,
    "delay_ms": 5000
  }
}
```

#### 延迟消息（使用 delay_sec）
```json
POST /api/v1/topics/:topic/messages
{
  "body": "delayed message",
  "tag": "delay-tag",
  "delay_sec": 10  // 延迟 10 秒
}
```

**响应示例：**
```json
{
  "code": "ok",
  "message": "success",
  "data": {
    "id": "019c2311-e621-7f1b-b5a1-4f2ea7d23dd9",
    "topic": "test-topic",
    "tag": "delay-tag",
    "scheduled_at": "2026-02-03T18:35:01+08:00",
    "execute_at": "2026-02-03T18:35:11+08:00",
    "delay_seconds": 10.0,
    "delay_ms": 10000
  }
}
```

### 3. 延迟参数说明

| 参数 | 类型 | 必填 | 说明 | 范围 |
|------|------|------|------|------|
| `delay_ms` | int | 否 | 延迟时间（毫秒） | 0 - 2592000000 (30 天) |
| `delay_sec` | int | 否 | 延迟时间（秒） | 1 - 2592000 (30 天) |

**重要规则：**
- `delay_ms` 和 `delay_sec` 不能同时指定，否则返回 400 错误
- 如果都不指定，则作为普通消息处理
- 延迟时间最大不能超过 30 天

### 4. 错误处理

#### 同时指定两个延迟参数
```bash
curl -X POST http://localhost:8080/api/v1/topics/test/messages \
  -H "Content-Type: application/json" \
  -d '{
    "body": "test",
    "delay_ms": 5000,
    "delay_sec": 10
  }'
```

**错误响应：**
```json
{
  "code": "invalid_delay",
  "message": "cannot specify both delay_ms and delay_sec"
}
```

#### 延迟时间超出范围
```bash
curl -X POST http://localhost:8080/api/v1/topics/test/messages \
  -H "Content-Type: application/json" \
  -d '{
    "body": "test",
    "delay_sec": 2592001
  }'
```

**错误响应：**
```json
{
  "code": "invalid_delay",
  "message": "delay_sec must be between 1 and 2592000 (30 days)"
}
```

#### Topic 不存在
```bash
curl -X POST http://localhost:8080/api/v1/topics/non-existent/messages \
  -H "Content-Type: application/json" \
  -d '{
    "body": "test"
  }'
```

**错误响应（404）：**
```json
{
  "code": "topic_not_found",
  "message": "topic not found: non-existent"
}
```

## 设计优势

### 1. RESTful 设计
- **单一资源端点**：消息是同一种资源，无论是否延迟
- **属性驱动**：延迟是消息的一个可选属性，而不是不同的资源类型
- **简化 API 层级**：减少端点数量，降低学习成本

### 2. 更好的扩展性
- **添加新属性更容易**：未来可以继续添加 `priority`、`ttl` 等可选参数
- **向后兼容**：旧的不带延迟参数的请求仍然可以正常工作
- **灵活性**：客户端只需记住一个端点，根据需要添加参数

### 3. 统一的验证逻辑
- **Topic 存在性验证**：统一在一个 handler 中处理
- **参数验证**：集中管理所有消息相关的参数验证
- **错误处理**：一致的错误响应格式

### 4. 更简洁的代码
- **减少重复代码**：不需要维护两个相似的 handler
- **集中业务逻辑**：所有消息生产逻辑在一个地方处理
- **更容易测试**：测试用例更集中，覆盖更全面

## 实现细节

### Handler 逻辑流程

```go
func ProduceHandler(b *broker.Broker) gin.HandlerFunc {
    return func(c *gin.Context) {
        // 1. 验证 topic 是否存在
        topic := c.Param("topic")
        if _, err := b.GetTopicConfig(topic); err != nil {
            FailGin(c, ErrTopicNotFound)
            return
        }

        // 2. 解析请求体
        var payload struct {
            Body     string `json:"body"`
            Tag      string `json:"tag"`
            DelayMs  int64  `json:"delay_ms"`
            DelaySec int    `json:"delay_sec"`
        }
        if err := c.ShouldBindJSON(&payload); err != nil {
            FailGin(c, ErrInvalidMessage)
            return
        }

        // 3. 验证延迟参数
        hasDelay := payload.DelayMs > 0 || payload.DelaySec > 0
        if hasDelay {
            // 验证不能同时指定两个参数
            // 验证范围是否有效
            
            // 4a. 处理延迟消息
            _, err := b.EnqueueWithDelay(topic, payload.Body, payload.Tag, delay)
            // 返回 ProduceDelayResponse
        } else {
            // 4b. 处理普通消息
            _, offset, err := b.Enqueue(topic, payload.Body, payload.Tag)
            // 返回 ProduceResponse
        }
    }
}
```

### 测试覆盖

新增的 `unified_produce_test.go` 覆盖以下场景：

1. ✅ 生产普通消息（不带延迟参数）
2. ✅ 使用 `delay_ms` 生产延迟消息
3. ✅ 使用 `delay_sec` 生产延迟消息
4. ✅ 同时指定两个延迟参数（应返回 400）
5. ✅ 延迟时间超出范围（应返回 400）
6. ✅ 向不存在的 topic 生产消息（应返回 404）

所有现有测试也已更新并通过。

## 迁移指南

### 对于使用旧 API 的客户端

#### 之前的代码
```go
// 普通消息
resp, err := http.Post(
    "http://localhost:8080/api/v1/topics/test-topic/messages",
    "application/json",
    bytes.NewBuffer(normalMsg),
)

// 延迟消息
resp, err := http.Post(
    "http://localhost:8080/api/v1/topics/test-topic/delayed-messages",
    "application/json",
    bytes.NewBuffer(delayedMsg),
)
```

#### 现在的代码
```go
// 普通消息（无需修改）
resp, err := http.Post(
    "http://localhost:8080/api/v1/topics/test-topic/messages",
    "application/json",
    bytes.NewBuffer(normalMsg),
)

// 延迟消息（修改 URL 和添加延迟参数）
delayedMsg := map[string]interface{}{
    "body":      "content",
    "tag":       "tag",
    "delay_sec": 60,  // 添加延迟参数
}
resp, err := http.Post(
    "http://localhost:8080/api/v1/topics/test-topic/messages",  // 使用统一端点
    "application/json",
    bytes.NewBuffer(delayedMsg),
)
```

### curl 示例

```bash
# 普通消息
curl -X POST http://localhost:8080/api/v1/topics/my-topic/messages \
  -H "Content-Type: application/json" \
  -d '{"body": "hello", "tag": "greeting"}'

# 延迟消息（5 秒后）
curl -X POST http://localhost:8080/api/v1/topics/my-topic/messages \
  -H "Content-Type: application/json" \
  -d '{"body": "hello", "tag": "greeting", "delay_sec": 5}'

# 延迟消息（5000 毫秒后）
curl -X POST http://localhost:8080/api/v1/topics/my-topic/messages \
  -H "Content-Type: application/json" \
  -d '{"body": "hello", "tag": "greeting", "delay_ms": 5000}'
```

## 文件变更清单

### 修改的文件
- `mq/api/gin_handlers.go` - 合并 ProduceHandler 和 ProduceDelayedHandler
- `mq/api/router.go` - 移除 `/delayed-messages` 路由
- `mq/api/api_test.go` - 更新测试用例
- `mq/api/topic_validation_test.go` - 更新测试用例

### 新增的文件
- `mq/api/unified_produce_test.go` - 新增统一 API 的测试用例
- `docs/API_UNIFICATION.md` - 本文档

### 可以移除的文件（可选）
- `mq/api/delay_handlers.go` - ProduceDelayedHandler 已经不再使用

## 总结

通过合并 produce 和 produceDelay API，我们实现了：

1. **更简洁的 API**：从 2 个端点减少到 1 个端点
2. **更好的 RESTful 设计**：延迟作为消息的属性而不是单独的资源
3. **更灵活的扩展性**：可以轻松添加更多可选参数
4. **更统一的验证**：所有消息生产相关的验证集中在一处
5. **完整的测试覆盖**：所有场景都有测试用例

这个改动不仅简化了 API 的使用，也让代码更易于维护和扩展。
