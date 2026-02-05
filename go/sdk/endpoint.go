package client

import "fmt"

type Endpoint struct {
	baseUrl string
}

func newEndpoint(baseUrl string) *Endpoint {
	return &Endpoint{
		baseUrl: baseUrl,
	}
}

// access key
func (e *Endpoint) CreateAccessKey() string {
	return fmt.Sprintf("%s/api/v1/admin/aks", e.baseUrl)
}
func (e *Endpoint) ListAccessKeys() string {
	return fmt.Sprintf("%s/api/v1/admin/aks", e.baseUrl)
}
func (e *Endpoint) DeleteAccessKey(id string) string {
	return fmt.Sprintf("%s/api/v1/admin/aks/%s", e.baseUrl, id)
}

// stats
func (e *Endpoint) GetStats() string {
	return fmt.Sprintf("%s/api/v1/stats", e.baseUrl)
}

// topics
func (e *Endpoint) CreateTopic() string {
	return fmt.Sprintf("%s/api/v1/topics", e.baseUrl)
}
func (e *Endpoint) ListTopics() string {
	return fmt.Sprintf("%s/api/v1/topics", e.baseUrl)
}
func (e *Endpoint) GetTopic(topic string) string {
	return fmt.Sprintf("%s/api/v1/topics/%s", e.baseUrl, topic)
}
func (e *Endpoint) DeleteTopic(topic string) string {
	return fmt.Sprintf("%s/api/v1/topics/%s", e.baseUrl, topic)
}

// messages
func (e *Endpoint) ProduceMessage(topic string) string {
	return fmt.Sprintf("%s/api/v1/topics/%s/messages", e.baseUrl, topic)
}
func (e *Endpoint) ConsumeMessages(topic, group string) string {
	return fmt.Sprintf("%s/api/v1/consumers/%s/topics/%s/messages", e.baseUrl, group, topic)
}
func (e *Endpoint) GetOffsets(topic, group string) string {
	return fmt.Sprintf("%s/api/v1/consumers/%s/topics/%s/offsets", e.baseUrl, group, topic)
}
func (e *Endpoint) CommitOffsets(topic, group string) string {
	return fmt.Sprintf("%s/api/v1/consumers/%s/topics/%s/offsets", e.baseUrl, group, topic)
}
func (e *Endpoint) AckMessage(id string) string {
	return fmt.Sprintf("%s/api/v1/messages/%s/ack", e.baseUrl, id)
}
func (e *Endpoint) NackMessage(id string) string {
	return fmt.Sprintf("%s/api/v1/messages/%s/nack", e.baseUrl, id)
}
