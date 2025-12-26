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

type BatchResult struct {
	Timestamp        time.Time
	ElapsedSeconds   float64
	BatchesPerSecond float64
	EntriesPerSecond float64
	BytesPerSecond   float64
	TotalBatches     int64
	TotalEntries     int64
	TotalBytes       int64
}

func RunBatchBenchmark(numThreads int, duration time.Duration, batchSize int, messageSize int) error {
	var wg sync.WaitGroup
	results := make(chan BatchResult, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			cfg := &config.PublisherConfig{
				BrokerAddrs:     []string{"localhost:9000"},
				Topic:           fmt.Sprintf("batch-topic-%d", threadID),
				EnableBenchmark: true,
				NumMessages:     100000,
				MessageSize:     messageSize,
				BatchSize:       batchSize,
				LingerMS:        10,
				Partitions:      1,
			}

			start := time.Now()
			pub, err := publisher.NewPublisher(cfg)
			if err != nil {
				results <- BatchResult{Timestamp: time.Now()}
				return
			}
			defer pub.Close()

			for j := 0; j < cfg.NumMessages; j += batchSize {
				end := j + batchSize
				if end > cfg.NumMessages {
					end = cfg.NumMessages
				}

				for k := j; k < end; k++ {
					msg := generateMessage(messageSize, k)
					pub.PublishMessage(msg)
				}
			}

			elapsed := time.Since(start)
			totalBatches := int64((cfg.NumMessages + batchSize - 1) / batchSize)
			batchesPerSec := float64(totalBatches) / elapsed.Seconds()
			entriesPerSec := float64(cfg.NumMessages) / elapsed.Seconds()
			bytesPerSec := float64(cfg.NumMessages*messageSize) / elapsed.Seconds()

			results <- BatchResult{
				Timestamp:        time.Now(),
				ElapsedSeconds:   elapsed.Seconds(),
				BatchesPerSecond: batchesPerSec,
				EntriesPerSecond: entriesPerSec,
				BytesPerSecond:   bytesPerSec,
				TotalBatches:     totalBatches,
				TotalEntries:     int64(cfg.NumMessages),
				TotalBytes:       int64(cfg.NumMessages * messageSize),
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return saveBatchResults(results)
}

func saveBatchResults(results <-chan BatchResult) error {
	file, err := os.Create("test/data/batch_throughput.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"timestamp", "elapsed_seconds", "batches_per_second",
		"entries_per_second", "bytes_per_second", "total_batches", "total_entries", "total_bytes",
	})

	for result := range results {
		writer.Write([]string{
			result.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.3f", result.ElapsedSeconds),
			fmt.Sprintf("%.2f", result.BatchesPerSecond),
			fmt.Sprintf("%.2f", result.EntriesPerSecond),
			fmt.Sprintf("%.2f", result.BytesPerSecond),
			fmt.Sprintf("%d", result.TotalBatches),
			fmt.Sprintf("%d", result.TotalEntries),
			fmt.Sprintf("%d", result.TotalBytes),
		})
	}

	return nil
}
