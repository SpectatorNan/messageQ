# 重试退避配置说明

## 配置参数

MessageQ 支持配置重试退避（retry backoff）策略，允许自定义消息重试时的延迟算法。

### 配置项

```yaml
broker:
  retry_backoff_base: 1s            # 基础延迟时间
  retry_backoff_multiplier: 2.0     # 退避乘数（指数）
  retry_backoff_max: 60s            # 最大延迟时间
```

### 计算公式

```
delay = base × (multiplier ^ retry_count)
delay = min(delay, max)
```

### 示例场景

#### 1. 默认配置（指数退避）
```yaml
retry_backoff_base: 1s
retry_backoff_multiplier: 2.0
retry_backoff_max: 60s
```

重试延迟序列：
- 第1次重试: 1s × (2^1) = **2s**
- 第2次重试: 1s × (2^2) = **4s**
- 第3次重试: 1s × (2^3) = **8s**
- 第4次重试: 1s × (2^4) = **16s**
- 第5次重试: 1s × (2^5) = **32s**
- 第6次重试: 1s × (2^6) = 64s → **60s** (达到上限)

#### 2. 线性退避
```yaml
retry_backoff_base: 5s
retry_backoff_multiplier: 1.0
retry_backoff_max: 60s
```

重试延迟序列：
- 所有重试: **5s** (线性，不增长)

#### 3. 快速重试（适合开发环境）
```yaml
retry_backoff_base: 100ms
retry_backoff_multiplier: 1.5
retry_backoff_max: 10s
```

重试延迟序列：
- 第1次重试: 100ms × (1.5^1) = **150ms**
- 第2次重试: 100ms × (1.5^2) = **225ms**
- 第3次重试: 100ms × (1.5^3) = **338ms**
- 第4次重试: 100ms × (1.5^4) = **506ms**
- 第5次重试: 100ms × (1.5^5) = **759ms**

#### 4. 激进重试（高可用场景）
```yaml
retry_backoff_base: 2s
retry_backoff_multiplier: 3.0
retry_backoff_max: 300s
```

重试延迟序列：
- 第1次重试: 2s × (3^1) = **6s**
- 第2次重试: 2s × (3^2) = **18s**
- 第3次重试: 2s × (3^3) = **54s**
- 第4次重试: 2s × (3^4) = **162s**
- 第5次重试: 2s × (3^5) = 486s → **300s** (达到上限)

#### 5. 固定延迟（不使用退避）
```yaml
retry_backoff_base: 10s
retry_backoff_multiplier: 1.0
retry_backoff_max: 10s
```

重试延迟序列：
- 所有重试: **10s** (固定延迟)

## 使用建议

### 选择合适的配置

| 场景 | base | multiplier | max | 说明 |
|------|------|------------|-----|------|
| **生产环境（推荐）** | 1s | 2.0 | 60s | 平衡重试速度和系统压力 |
| **开发/测试环境** | 100ms | 1.5 | 5s | 快速重试便于调试 |
| **高可用系统** | 500ms | 1.8 | 30s | 快速恢复但避免雪崩 |
| **低优先级任务** | 5s | 2.5 | 300s | 减少系统压力 |
| **瞬时故障恢复** | 2s | 3.0 | 120s | 快速放弃或快速恢复 |

### 注意事项

1. **避免过小的 base 值**
   - 太小的基础延迟可能导致频繁重试，增加系统压力
   - 建议最小值不低于 100ms

2. **multiplier > 1.0 才是退避**
   - multiplier = 1.0 时为固定延迟
   - multiplier < 1.0 时延迟会递减（不推荐）
   - 推荐范围：1.5 - 3.0

3. **合理设置 max**
   - max 应与业务超时相匹配
   - 如果 max 太大，失败消息会占用资源很久
   - 如果 max 太小，可能没有足够时间恢复

4. **与 max_retry 配合使用**
   ```yaml
   broker:
     max_retry: 5              # 最多重试5次
     retry_backoff_base: 1s
     retry_backoff_multiplier: 2.0
     retry_backoff_max: 60s
   ```
   - 第5次重试后会进入死信队列（DLQ）
   - 总计延迟: 2s + 4s + 8s + 16s + 32s = 62s

## 环境变量覆盖

支持通过环境变量动态配置：

```bash
export MQ_BROKER_RETRY_BACKOFF_BASE=2s
export MQ_BROKER_RETRY_BACKOFF_MULTIPLIER=1.5
export MQ_BROKER_RETRY_BACKOFF_MAX=120s
```

## 监控建议

建议监控以下指标：
- 重试消息的平均延迟
- 达到 max 延迟的消息数量
- 进入死信队列的消息数量
- 重试成功率

这些指标可以帮助你调整退避参数以获得最佳性能。
