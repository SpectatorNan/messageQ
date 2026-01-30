package main

import "messageQ/mq/queue"

// Adapter: alias the top-level MessageQueue to the implementation in mq/queue
type MessageQueue = queue.Queue

func NewMessageQueue() *MessageQueue {
	q := queue.NewQueue()
	return q
}
