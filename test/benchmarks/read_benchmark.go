package benchmarks

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/downfa11-org/cursus/test/consumer"
	"github.com/downfa11-org/cursus/test/consumer/config"
	"github.com/downfa11-org/cursus/test/publisher"
	"github.com/downfa11-org/cursus/test/publisher/config"
)

type ReadResult struct {
	Timestamp      time.Time
	ElapsedSeconds float64
	ReadsPerSecond float64
	BytesPerSecond float64
	TotalReads     int64
	TotalBytes     int64
}

func RunReadBenchmark(numThreads int, numMessages int, messageSize int) error {
	topic := "read-benchmark-topic"
	if err := setupTestData(topic, numMessages, messageSize); err != nil {
		return fmt.Errorf("failed to setup test data: %w", err)
	}

	var wg sync.WaitGroup
	results := make(chan ReadResult, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			cfg := &config.ConsumerConfig{
				BrokerAddrs:      []string{"localhost:9000"},
				Topic:            topic,
				GroupID:          fmt.Sprintf("read-group-%d", threadID),
				ConsumerID:       fmt.Sprintf("consumer-%d", threadID),
				EnableBenchmark:  true,
				BatchSize:        100,
				Mode:             "polling",
				PollInterval:     10 * time.Millisecond,
				EnableAutoCommit: false,
			}

			start := time.Now()
			cons, err := consumer.NewConsumer(cfg)
			if err != nil {
				results <- ReadResult{Timestamp: time.Now()}
				return
			}
			defer cons.Close()

			cons.Start()
			time.Sleep(5 * time.Second)
			elapsed := time.Since(start)

			totalReads := int64(numMessages / numThreads)
			readsPerSec := float64(totalReads) / elapsed.Seconds()
			bytesPerSec := float64(totalReads*messageSize) / elapsed.Seconds()

			results <- ReadResult{
				Timestamp:      time.Now(),
				ElapsedSeconds: elapsed.Seconds(),
				ReadsPerSecond: readsPerSec,
				BytesPerSecond: bytesPerSec,
				TotalReads:     totalReads,
				TotalBytes:     int64(totalReads * messageSize),
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return saveReadResults(results)
}

func setupTestData(topic string, numMessages int, messageSize int) error {
	cfg := &publisherConfig.PublisherConfig{
		BrokerAddrs:     []string{"localhost:9000"},
		Topic:           topic,
		EnableBenchmark: false,
		NumMessages:     numMessages,
		MessageSize:     messageSize,
		BatchSize:       1000,
		LingerMS:        0,
		Partitions:      4,
	}

	pub, err := publisher.NewPublisher(cfg)
	if err != nil {
		return err
	}
	defer pub.Close()

	for i := 0; i < numMessages; i++ {
		msg := generateMessage(messageSize, i)
		if err := pub.PublishMessage(msg); err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}
	}

	pub.Flush()
	return nil
}

func saveReadResults(results <-chan ReadResult) error {
	file, err := os.Create("test/data/read_throughput.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"timestamp", "elapsed_seconds", "reads_per_second",
		"bytes_per_second", "total_reads", "total_bytes",
	})

	for result := range results {
		writer.Write([]string{
			result.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.3f", result.ElapsedSeconds),
			fmt.Sprintf("%.2f", result.ReadsPerSecond),
			fmt.Sprintf("%.2f", result.BytesPerSecond),
			fmt.Sprintf("%d", result.TotalReads),
			fmt.Sprintf("%d", result.TotalBytes),
		})
	}

	return nil
}
