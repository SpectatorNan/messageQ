package main

import "messageQ/mq/queue"

// 保留 Message 别名，Resp 等 API 相关类型已迁移到 mq/api/resp.go

// 使用 mq 包中的 Message 类型作为主应用的消息类型别名，避免重复定义并保持一致性
type Message = queue.Message
