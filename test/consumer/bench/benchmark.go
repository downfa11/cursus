package bench

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/downfa11-org/go-broker/util"
)

const sep = "========================================"

type ConsumerMetrics struct {
	MsgCount        int64
	ProcessedCount  int64
	StartTime       time.Time
	PartitionCounts map[int]*int64
	TargetMessages  int64

	mu sync.RWMutex
}

func NewConsumerMetrics(target int64) *ConsumerMetrics {
	return &ConsumerMetrics{
		StartTime:       time.Now(),
		PartitionCounts: make(map[int]*int64),
		TargetMessages:  target,
	}
}

func (cm *ConsumerMetrics) RecordMessage(partition int) {
	cm.mu.RLock()
	ptr, ok := cm.PartitionCounts[partition]
	cm.mu.RUnlock()

	if !ok {
		cm.mu.Lock()
		ptr, ok = cm.PartitionCounts[partition]
		if !ok {
			p := new(int64)
			*p = 0
			cm.PartitionCounts[partition] = p
			ptr = p
		}
		cm.mu.Unlock()
	}

	atomic.AddInt64(ptr, 1)
	atomic.AddInt64(&cm.MsgCount, 1)
}

func (cm *ConsumerMetrics) RecordProcessed(count int64) {
	atomic.AddInt64(&cm.ProcessedCount, count)
}

func (m *ConsumerMetrics) IsDone() bool {
	return atomic.LoadInt64(&m.MsgCount) >= m.TargetMessages
}

func (m *ConsumerMetrics) PrintSummaryTo(w io.Writer) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	duration := time.Since(m.StartTime)
	total := atomic.LoadInt64(&m.MsgCount)
	processed := atomic.LoadInt64(&m.ProcessedCount)

	seconds := duration.Seconds()
	if seconds <= 0 {
		seconds = 0.001
	}

	tps := float64(total) / duration.Seconds()
	processedTps := float64(processed) / duration.Seconds()

	fmt.Fprint(w, "\r\n")
	fmt.Fprintln(w, sep)
	fmt.Fprintln(w, "📊 CONSUMER BENCHMARK SUMMARY")
	fmt.Fprintf(w, "%-28s : %d\n", "Partitions Observed", len(m.PartitionCounts))
	fmt.Fprintf(w, "%-28s : %d\n", "Total Target", m.TargetMessages)
	fmt.Fprintf(w, "%-28s : %d\n", "Total messages consumed", total)
	fmt.Fprintf(w, "%-28s : %d\n", "Actually processed", processed)
	fmt.Fprintf(w, "%-28s : %d\n", "Duplicate/Extra", total-processed)
	fmt.Fprintf(w, "%-28s : %.3fs\n", "Consume elapsed Time", seconds)
	fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Consume Throughput", tps)
	fmt.Fprintf(w, "%-28s : %.2f msg/s\n", "Processed Throughput", processedTps)
	fmt.Fprint(w, "\r\n")

	fmt.Fprintln(w, "Partition Breakdown:")
	keys := make([]int, 0, len(m.PartitionCounts))
	for k := range m.PartitionCounts {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, pid := range keys {
		count := atomic.LoadInt64(m.PartitionCounts[pid])
		fmt.Fprintf(w, "  #%-2d  messages=%-10d  share=%.2f%%\n", pid, count, (float64(count)/float64(total))*100)
	}
	fmt.Fprintln(w, sep)
}

func (m *ConsumerMetrics) PrintSummary() {
	util.Info("🎉 Benchmark completed successfully!")
	m.PrintSummaryTo(os.Stdout)
}
