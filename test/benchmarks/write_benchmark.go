package benchmarks

import (
	"encoding/csv"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/downfa11-org/cursus/test/publisher"
	"github.com/downfa11-org/cursus/test/publisher/config"
)

type WriteResult struct {
	Timestamp       time.Time
	ElapsedSeconds  float64
	WritesPerSecond float64
	BytesPerSecond  float64
	TotalWrites     int64
	TotalBytes      int64
}

func RunWriteBenchmark(numThreads int, duration time.Duration, messageSize int) error {
	var wg sync.WaitGroup
	results := make(chan WriteResult, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			cfg := &config.PublisherConfig{
				BrokerAddrs:     []string{"localhost:9000"},
				Topic:           fmt.Sprintf("bench-topic-%d", threadID),
				EnableBenchmark: true,
				NumMessages:     100000,
				MessageSize:     messageSize,
				BatchSize:       500,
				LingerMS:        0,
				Partitions:      1,
			}

			start := time.Now()
			pub, err := publisher.NewPublisher(cfg)
			if err != nil {
				results <- WriteResult{Timestamp: time.Now()}
				return
			}
			defer pub.Close()

			for j := 0; j < cfg.NumMessages; j++ {
				msg := generateMessage(messageSize, j)
				pub.PublishMessage(msg)
			}

			elapsed := time.Since(start)
			writesPerSec := float64(cfg.NumMessages) / elapsed.Seconds()
			bytesPerSec := float64(cfg.NumMessages*messageSize) / elapsed.Seconds()

			results <- WriteResult{
				Timestamp:       time.Now(),
				ElapsedSeconds:  elapsed.Seconds(),
				WritesPerSecond: writesPerSec,
				BytesPerSecond:  bytesPerSec,
				TotalWrites:     int64(cfg.NumMessages),
				TotalBytes:      int64(cfg.NumMessages * messageSize),
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return saveWriteResults(results)
}

func saveWriteResults(results <-chan WriteResult) error {
	file, err := os.Create("test/data/benchmark_throughput.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"timestamp", "elapsed_seconds", "writes_per_second",
		"bytes_per_second", "total_writes", "total_bytes",
	})

	for result := range results {
		writer.Write([]string{
			result.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.3f", result.ElapsedSeconds),
			fmt.Sprintf("%.2f", result.WritesPerSecond),
			fmt.Sprintf("%.2f", result.BytesPerSecond),
			fmt.Sprintf("%d", result.TotalWrites),
			fmt.Sprintf("%d", result.TotalBytes),
		})
	}

	return nil
}
