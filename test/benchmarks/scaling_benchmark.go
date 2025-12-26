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

type ScalingResult struct {
	ThreadCount     int
	ElapsedSeconds  float64
	WritesPerSecond float64
	BytesPerSecond  float64
	TotalWrites     int64
	TotalBytes      int64
}

func RunScalingBenchmark(maxThreads int, messageSize int) error {
	file, err := os.Create("test/data/scaling_results.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"threads", "elapsed_seconds", "writes_per_second",
		"bytes_per_second", "total_writes", "total_bytes",
	})

	for threads := 1; threads <= maxThreads; threads++ {
		result := runSingleScalingTest(threads, messageSize)

		writer.Write([]string{
			fmt.Sprintf("%d", result.ThreadCount),
			fmt.Sprintf("%.3f", result.ElapsedSeconds),
			fmt.Sprintf("%.2f", result.WritesPerSecond),
			fmt.Sprintf("%.2f", result.BytesPerSecond),
			fmt.Sprintf("%d", result.TotalWrites),
			fmt.Sprintf("%d", result.TotalBytes),
		})

		writer.Flush()
		fmt.Printf("Completed scaling test with %d threads: %.2f writes/sec\n", threads, result.WritesPerSecond)
	}

	return nil
}

func runSingleScalingTest(numThreads int, messageSize int) ScalingResult {
	var wg sync.WaitGroup
	results := make(chan WriteResult, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()

			cfg := &config.PublisherConfig{
				BrokerAddrs:     []string{"localhost:9000"},
				Topic:           fmt.Sprintf("scaling-topic-%d", threadID),
				EnableBenchmark: true,
				NumMessages:     10000,
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

	var totalWrites int64
	var totalBytes int64
	var totalElapsed float64
	var totalThroughput float64

	count := 0
	for result := range results {
		if result.TotalWrites > 0 {
			totalWrites += result.TotalWrites
			totalBytes += result.TotalBytes
			totalElapsed += result.ElapsedSeconds
			totalThroughput += result.WritesPerSecond
			count++
		}
	}

	avgThroughput := totalThroughput / float64(count)
	avgElapsed := totalElapsed / float64(count)

	return ScalingResult{
		ThreadCount:     numThreads,
		ElapsedSeconds:  avgElapsed,
		WritesPerSecond: avgThroughput,
		BytesPerSecond:  float64(totalBytes) / avgElapsed,
		TotalWrites:     totalWrites,
		TotalBytes:      totalBytes,
	}
}
