package main_test

import (
	"encoding/binary"
	"testing"
)

type BrokerClient interface {
	Send(msg []byte) ([]byte, error)
	CreateTopic(topic string, partitions int) error
}

type MockBrokerClient struct{}

func (m *MockBrokerClient) Send(msg []byte) ([]byte, error) {
	return []byte("ACK"), nil
}

func (m *MockBrokerClient) CreateTopic(topic string, partitions int) error {
	return nil
}

type Publisher struct {
	Broker     BrokerClient
	Topic      string
	Partitions int
}

func NewPublisherWithClient(topic string, partitions int, client BrokerClient) *Publisher {
	return &Publisher{
		Broker:     client,
		Topic:      topic,
		Partitions: partitions,
	}
}

func (p *Publisher) CreateTopic() error {
	return p.Broker.CreateTopic(p.Topic, p.Partitions)
}

func (p *Publisher) PublishMessage(msg string) error {
	_, err := p.Broker.Send([]byte(msg))
	return err
}

func EncodeMessage(topic, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)

	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

func TestPublisher_CreateTopic(t *testing.T) {
	mock := &MockBrokerClient{}
	pub := NewPublisherWithClient("test-topic", 2, mock)

	if err := pub.CreateTopic(); err != nil {
		t.Errorf("CreateTopic failed: %v", err)
	}
}

func TestPublisher_PublishMessage(t *testing.T) {
	mock := &MockBrokerClient{}
	pub := NewPublisherWithClient("test-topic", 2, mock)

	if err := pub.PublishMessage("Hello test message"); err != nil {
		t.Errorf("PublishMessage failed: %v", err)
	}
}

func TestEncodeMessage(t *testing.T) {
	topic := "my-topic"
	payload := "hello"
	data := EncodeMessage(topic, payload)

	if len(data) < 2 {
		t.Fatalf("Encoded data too short: %d bytes", len(data))
	}

	topicLen := int(binary.BigEndian.Uint16(data[:2]))
	if topicLen != len(topic) {
		t.Errorf("Topic length mismatch: expected %d, got %d", len(topic), topicLen)
	}

	decodedTopic := string(data[2 : 2+topicLen])
	decodedPayload := string(data[2+topicLen:])
	if decodedTopic != topic {
		t.Errorf("Topic mismatch: expected %q, got %q", topic, decodedTopic)
	}
	if decodedPayload != payload {
		t.Errorf("Payload mismatch: expected %q, got %q", payload, decodedPayload)
	}
}
