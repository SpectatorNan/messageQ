
# 1.可靠性核心模型
真实MQ至少包含5个状态：
```api
READY  ->  INFLIGHT  ->  ACK
           ↓
         TIMEOUT -> RETRY -> DLQ

```

含义：

- READY：等待消费
- INFLIGHT：已投递，未确认
- ACK：成功删除
- TIMEOUT：消费者挂了
- RETRY：重新入队
- DLQ：超过最大重试次数

# 2.Queue 内核结构升级
```go
type Message struct {
	ID        int64     `json:"id"`
	Body      string    `json:"body"`
	Retry     int       `json:"retry"`
	Timestamp time.Time `json:"timestamp"`
}
type InflightMsg struct {
    Msg      Message
    Deadline time.Time
}
type Queue struct {
    mu        sync.Mutex
    cond      *sync.Cond
    data      []Message
    inflight  map[int64]InflightMsg
    nextID    int64

    ackTimeout time.Duration
    maxRetry   int
}

```