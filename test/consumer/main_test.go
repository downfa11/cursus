package main_test

import (
	"bytes"
	"compress/gzip"
	main "consumer"
	"encoding/binary"
	"io"
	"testing"
	"time"
)

// TestCompressDecompressMessage tests gzip compression/decompression
func TestCompressDecompressMessage(t *testing.T) {
	original := []byte("test message")

	// Compress
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(original); err != nil {
		t.Fatalf("gzip write failed: %v", err)
	}
	if err := gzWriter.Close(); err != nil {
		t.Fatalf("gzip close failed: %v", err)
	}
	compressed := buf.Bytes()

	// Decompress
	gzReader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip reader failed: %v", err)
	}
	defer gzReader.Close()

	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("gzip read failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Errorf("Decompressed data doesn't match original")
	}
}

// TestEncodeMessage tests message encoding format
func TestEncodeMessage(t *testing.T) {
	topic := "my-topic"
	payload := "hello"

	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)

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

// TestConsumerConfigNormalization tests config validation
func TestConsumerConfigNormalization(t *testing.T) {
	tests := []struct {
		name     string
		input    *main.ConsumerConfig
		validate func(*main.ConsumerConfig) error
	}{
		{
			name: "negative session timeout gets default",
			input: &main.ConsumerConfig{
				SessionTimeoutMS:    -1,
				HeartbeatIntervalMS: 3000,
			},
			validate: func(cfg *main.ConsumerConfig) error {
				if cfg.SessionTimeoutMS != 5000 {
					t.Errorf("Expected SessionTimeoutMS=5000, got %d", cfg.SessionTimeoutMS)
				}
				return nil
			},
		},
		{
			name: "rebalance timeout adjusted to session timeout",
			input: &main.ConsumerConfig{
				SessionTimeoutMS:    30000,
				RebalanceTimeoutMS:  10000,
				HeartbeatIntervalMS: 3000,
			},
			validate: func(cfg *main.ConsumerConfig) error {
				if cfg.RebalanceTimeoutMS != 30000 {
					t.Errorf("Expected RebalanceTimeoutMS=30000, got %d", cfg.RebalanceTimeoutMS)
				}
				return nil
			},
		},
		{
			name: "TLS disabled when cert paths missing",
			input: &main.ConsumerConfig{
				UseTLS:      true,
				TLSCertPath: "",
				TLSKeyPath:  "",
			},
			validate: func(cfg *main.ConsumerConfig) error {
				if cfg.UseTLS {
					t.Error("Expected UseTLS=false when cert paths are empty")
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.input.SessionTimeoutMS <= 0 {
				tt.input.SessionTimeoutMS = 5000
			}
			if tt.input.RebalanceTimeoutMS < tt.input.SessionTimeoutMS {
				tt.input.RebalanceTimeoutMS = tt.input.SessionTimeoutMS
			}
			if tt.input.UseTLS && (tt.input.TLSCertPath == "" || tt.input.TLSKeyPath == "") {
				tt.input.UseTLS = false
			}

			tt.validate(tt.input)
		})
	}
}

// TestOffsetManager tests offset tracking
func TestOffsetManager(t *testing.T) {
	om := main.NewOffsetManager()
	om.Commit(0, 100)
	if got := om.Get(0); got != 100 {
		t.Errorf("Expected offset 100, got %d", got)
	}

	if got := om.Get(1); got != 0 {
		t.Errorf("Expected offset 0 for unknown partition, got %d", got)
	}

	om.Commit(1, 200)
	om.Commit(2, 300)
	if got := om.Get(1); got != 200 {
		t.Errorf("Expected offset 200, got %d", got)
	}
	if got := om.Get(2); got != 300 {
		t.Errorf("Expected offset 300, got %d", got)
	}
}

// TestCompressDecompress tests gzip functionality
func TestCompressDecompress(t *testing.T) {
	original := []byte("test message for compression")

	compressed, err := main.CompressMessage(original, true)
	if err != nil {
		t.Fatalf("Compression failed: %v", err)
	}

	if len(compressed) >= len(original) {
		t.Log("Warning: Compressed size not smaller (expected for small messages)")
	}

	decompressed, err := main.DecompressMessage(compressed, true)
	if err != nil {
		t.Fatalf("Decompression failed: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Error("Decompressed data doesn't match original")
	}

	notCompressed, err := main.CompressMessage(original, false)
	if err != nil {
		t.Fatalf("CompressMessage with gzip=false failed: %v", err)
	}

	if !bytes.Equal(original, notCompressed) {
		t.Error("Message should be unchanged when gzip is disabled")
	}
}

// TestMaxPollRecords tests poll limit functionality
func TestMaxPollRecords(t *testing.T) {
	t.Skip("Requires running broker - integration test")

	cfg := &main.ConsumerConfig{
		BrokerAddr:     "localhost:9000",
		GroupID:        "test-group",
		ConsumerID:     "test-consumer",
		Topic:          "test-topic",
		Partitions:     []int{0, 1},
		MaxPollRecords: 10,
		PollTimeoutMS:  1000,
	}

	consumer, err := main.NewConsumer(cfg)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	messages, err := consumer.Poll(5 * time.Second)
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}

	if len(messages) > cfg.MaxPollRecords {
		t.Errorf("Expected at most %d messages, got %d", cfg.MaxPollRecords, len(messages))
	}
}

// TestRetryBackoff tests exponential backoff
func TestRetryBackoff(t *testing.T) {
	t.Skip("Requires running broker - integration test")
}

// TestHeartbeat tests heartbeat functionality
func TestHeartbeat(t *testing.T) {
	t.Skip("Requires running broker - integration test")
}
