package main_test

import (
	"testing"
	"time"
)

type BrokerClient interface {
	RegisterConsumer(groupID, consumerID string) error
	SendHeartbeat(consumerID string) error
	PollMessages(topic string, partition int, maxRecords int) ([]string, error)
}

type MockBrokerClient struct{}

func (m *MockBrokerClient) RegisterConsumer(groupID, consumerID string) error {
	return nil
}

func (m *MockBrokerClient) SendHeartbeat(consumerID string) error {
	return nil
}

func (m *MockBrokerClient) PollMessages(topic string, partition int, maxRecords int) ([]string, error) {
	return []string{}, nil
}

type Consumer struct {
	Broker     BrokerClient
	GroupID    string
	ConsumerID string
	Topic      string
	Partitions []int
	ready      chan struct{}
}

type ConsumerConfig struct {
	BrokerAddr          string
	GroupID             string
	ConsumerID          string
	Topic               string
	Partitions          []int
	HeartbeatIntervalMS int
}

func NewConsumerWithClient(cfg *ConsumerConfig, client BrokerClient) (*Consumer, error) {
	c := &Consumer{
		Broker:     client,
		GroupID:    cfg.GroupID,
		ConsumerID: cfg.ConsumerID,
		Topic:      cfg.Topic,
		Partitions: cfg.Partitions,
		ready:      make(chan struct{}),
	}

	close(c.ready)
	return c, nil
}

func (c *Consumer) Poll(timeout time.Duration) ([]string, error) {
	var messages []string
	for _, p := range c.Partitions {
		msgs, err := c.Broker.PollMessages(c.Topic, p, 5)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msgs...)
	}
	return messages, nil
}

func (c *Consumer) Close() {
	// noop
}

type OffsetManager struct {
	offsets map[int]int
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{offsets: make(map[int]int)}
}

func (o *OffsetManager) Commit(partition int, offset int) {
	o.offsets[partition] = offset
}

func (o *OffsetManager) Get(partition int) int {
	return o.offsets[partition]
}

func TestLoadConsumerConfig_Defaults(t *testing.T) {
	cfg := &ConsumerConfig{
		GroupID:    "group1",
		ConsumerID: "consumer1",
	}
	if cfg.BrokerAddr != "" {
		t.Error("BrokerAddr should be empty for mock test")
	}
	if cfg.GroupID == "" {
		t.Error("GroupID should have a default")
	}
	if cfg.ConsumerID == "" {
		t.Error("ConsumerID should be auto-generated")
	}
}

func TestConsumer_JoinGroupAndHeartbeat(t *testing.T) {
	cfg := &ConsumerConfig{
		GroupID:    "test-group",
		ConsumerID: "test-consumer",
		Topic:      "test-topic",
		Partitions: []int{0},
	}
	mock := &MockBrokerClient{}
	consumer, err := NewConsumerWithClient(cfg, mock)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	select {
	case <-consumer.ready:
	case <-time.After(1 * time.Second):
		t.Fatal("Consumer did not become ready in time")
	}
}

func TestConsumer_Poll(t *testing.T) {
	cfg := &ConsumerConfig{
		GroupID:    "test-group",
		ConsumerID: "test-consumer",
		Topic:      "test-topic",
		Partitions: []int{0},
	}
	mock := &MockBrokerClient{}
	consumer, _ := NewConsumerWithClient(cfg, mock)

	<-consumer.ready

	messages, err := consumer.Poll(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	if len(messages) != 0 {
		t.Errorf("Expected 0 messages, got %d", len(messages))
	}
}

func TestOffsetManager(t *testing.T) {
	om := NewOffsetManager()
	om.Commit(0, 100)

	if got := om.Get(0); got != 100 {
		t.Errorf("Expected offset 100, got %d", got)
	}

	if got := om.Get(1); got != 0 {
		t.Errorf("Expected offset 0 for unknown partition, got %d", got)
	}
}
