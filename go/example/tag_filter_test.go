package example

import (
	"testing"

	"messageQ/mq/queue"
)

func TestTagFiltering(t *testing.T) {
	q := queue.NewQueue()
	_ = q.Enqueue("a1", "a")
	_ = q.Enqueue("b1", "b")
	_ = q.Enqueue("a2", "a")

	m := q.DequeueTag("b")
	if m.Tag != "b" {
		t.Fatalf("expected tag b, got %q", m.Tag)
	}
	m2 := q.DequeueTag("a")
	if m2.Tag != "a" {
		t.Fatalf("expected tag a, got %q", m2.Tag)
	}
}
