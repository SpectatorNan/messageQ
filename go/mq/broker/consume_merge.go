package broker

import (
	"github.com/SpectatorNan/messageQ/go/mq/storage"
	"sort"
)

// MergedConsumeOptions controls how to merge regular and delay queues
type MergedConsumeOptions struct {
	// IncludeDelay whether to include delay topic
	IncludeDelay bool
	// MaxMessages max messages to return from each queue
	MaxMessages int
	// SortByTimestamp whether to sort merged messages by timestamp
	SortByTimestamp bool
}

// ConsumeWithMerge consumes from both regular topic and delay topic, then merges
// Example: Consume("orders", "g1", 0) → reads from "orders" + "orders.delay"
func (b *Broker) ConsumeWithMerge(group, topic string, queueID int, tag string, opts MergedConsumeOptions) ([]storage.Message, error) {
	if !opts.IncludeDelay {
		// Fast path: only consume regular queue
		msgs, _, _, err := b.ConsumeWithLock(group, topic, queueID, tag, opts.MaxMessages)
		return msgs, err
	}

	// Consume from regular topic
	regularMsgs, _, _, err := b.ConsumeWithLock(group, topic, queueID, tag, opts.MaxMessages)
	if err != nil {
		return nil, err
	}

	// Consume from delay topic
	delayTopic := GetDelayTopicName(topic)
	delayMsgs, _, _, err := b.ConsumeWithLock(group, delayTopic, queueID, tag, opts.MaxMessages)
	if err != nil {
		// Delay topic may not exist, ignore error
		delayMsgs = nil
	}

	// Merge messages
	merged := append(regularMsgs, delayMsgs...)

	// Optional: sort by timestamp for strict ordering
	if opts.SortByTimestamp && len(merged) > 1 {
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].Timestamp.Before(merged[j].Timestamp)
		})
	}

	return merged, nil
}

// Example usage in API handler:
//
// func ConsumeHandler(c *gin.Context) {
//     topic := c.Param("topic")
//     group := c.Query("group")
//     queueID, _ := strconv.Atoi(c.Query("queue_id"))
//
//     // Option 1: Only regular queue (current behavior)
//     msgs, _, _, _ := broker.ConsumeWithLock(group, topic, queueID, "", 10)
//
//     // Option 2: Merge regular + delay queues
//     msgs, _ := broker.ConsumeWithMerge(group, topic, queueID, "", MergedConsumeOptions{
//         IncludeDelay: true,
//         MaxMessages: 10,
//         SortByTimestamp: true,
//     })
//
//     c.JSON(200, msgs)
// }
