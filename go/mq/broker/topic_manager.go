package broker

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TopicType defines the type of a topic
type TopicType string

const (
	TopicTypeNormal TopicType = "NORMAL" // Regular messages
	TopicTypeDelay  TopicType = "DELAY"  // Delay messages only
)

// TopicConfig represents the configuration of a topic
type TopicConfig struct {
	Name       string    `json:"name"`
	Type       TopicType `json:"type"`
	QueueCount int       `json:"queue_count"`
	CreatedAt  int64     `json:"created_at"`
}

// TopicManager manages topic metadata and persistence
type TopicManager struct {
	topics     map[string]*TopicConfig
	mu         sync.RWMutex
	configPath string
}

// NewTopicManager creates a new topic manager
func NewTopicManager(dataDir string) (*TopicManager, error) {
	tm := &TopicManager{
		topics:     make(map[string]*TopicConfig),
		configPath: filepath.Join(dataDir, "topics.json"),
	}

	// Load existing topics
	if err := tm.load(); err != nil {
		// If file doesn't exist, it's ok
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	return tm, nil
}

// CreateTopic creates a new topic with specified type
func (tm *TopicManager) CreateTopic(name string, topicType TopicType, queueCount int) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	config := &TopicConfig{
		Name:       name,
		Type:       topicType,
		QueueCount: queueCount,
		CreatedAt:  now().Unix(),
	}

	tm.topics[name] = config
	return tm.save()
}

// GetTopicConfig returns the configuration of a topic
func (tm *TopicManager) GetTopicConfig(name string) (*TopicConfig, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	config, exists := tm.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return config, nil
}

// ListTopics returns all topics
func (tm *TopicManager) ListTopics() []*TopicConfig {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	topics := make([]*TopicConfig, 0, len(tm.topics))
	for _, config := range tm.topics {
		topics = append(topics, config)
	}
	return topics
}

// DeleteTopic deletes a topic
func (tm *TopicManager) DeleteTopic(name string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.topics[name]; !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	delete(tm.topics, name)
	return tm.save()
}

// IsDelayTopic checks if a topic is a delay topic
func (tm *TopicManager) IsDelayTopic(name string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	config, exists := tm.topics[name]
	if !exists {
		return false
	}

	return config.Type == TopicTypeDelay
}

// load reads topic configurations from disk
func (tm *TopicManager) load() error {
	data, err := os.ReadFile(tm.configPath)
	if err != nil {
		return err
	}

	var configs []*TopicConfig
	if err := json.Unmarshal(data, &configs); err != nil {
		return err
	}

	for _, config := range configs {
		tm.topics[config.Name] = config
	}

	return nil
}

// save writes topic configurations to disk
func (tm *TopicManager) save() error {
	configs := make([]*TopicConfig, 0, len(tm.topics))
	for _, config := range tm.topics {
		configs = append(configs, config)
	}

	data, err := json.MarshalIndent(configs, "", "  ")
	if err != nil {
		return err
	}

	// Create directory if not exists
	dir := filepath.Dir(tm.configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	return os.WriteFile(tm.configPath, data, 0644)
}
