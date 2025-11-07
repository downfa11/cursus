package bench

import (
	"fmt"
	"sync"
	"time"
)

type BenchmarkRunner struct {
	Addr                string
	NumProducers        int
	NumConsumers        int
	MessagesPerProducer int
	Partitions          int
	EnableGzip          bool
	Topic               string
}

func NewBenchmarkRunner(addr, topicName string, partitions, producers, consumers, messages int, gzip bool) *BenchmarkRunner {
	return &BenchmarkRunner{
		Addr:                addr,
		NumProducers:        producers,
		NumConsumers:        consumers,
		MessagesPerProducer: messages,
		EnableGzip:          gzip,
		Topic:               topicName,
		Partitions:          partitions,
	}
}

func (b *BenchmarkRunner) Run() {
	totalMessages := b.NumProducers * b.MessagesPerProducer
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < b.NumProducers; i++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			client := &BenchClient{
				Addr:        b.Addr,
				EnableGzip:  b.EnableGzip,
				NumMessages: b.MessagesPerProducer,
				Topic:       b.Topic,
				Partitions:  b.Partitions,
			}
			if err := client.Run(); err != nil {
				fmt.Printf("Producer %d error: %v\n", pid, err)
			}
		}(i)
	}
	wg.Wait()

	duration := time.Since(start)
	throughput := float64(totalMessages) / duration.Seconds()

	fmt.Printf("\n🧪 BENCHMARK RESULT [disk] 🧪\n")
	fmt.Printf("-------------------------------------\n")
	fmt.Printf(" Producers     : %d\n", b.NumProducers)
	fmt.Printf(" Consumers     : %d\n", b.NumConsumers)
	fmt.Printf(" Partitions    : %d\n", b.Partitions)
	fmt.Printf(" Total Messages: %d\n", totalMessages)
	fmt.Printf(" Duration      : %v\n", duration)
	fmt.Printf(" Throughput    : %.2f msg/sec\n", throughput)
	fmt.Printf("-------------------------------------\n")
}
