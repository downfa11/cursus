package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	MessagesProcessed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "broker_messages_processed_total",
		Help: "Total number of messages processed by the benchmark",
	})

	MessagesPerSec = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_messages_per_second",
		Help: "Current throughput in messages per second",
	})

	LatencyHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "broker_message_latency_seconds",
		Help:    "Histogram of message latency during processing",
		Buckets: prometheus.DefBuckets,
	})

	QueueSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "broker_queue_size",
		Help: "Current queue size in the topic manager",
	})

	CleanupCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "broker_cleanup_count_total",
		Help: "Total number of deduped message IDs cleaned up from memory",
	})
)

func init() {
	prometheus.MustRegister(MessagesProcessed, MessagesPerSec, LatencyHist, QueueSize, CleanupCount)
}

func StartMetricsServer(port int) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		addr := fmt.Sprintf(":%d", port)
		fmt.Println("[METRICS] Prometheus exporter listening on", addr)
		_ = http.ListenAndServe(addr, nil)
	}()
}

// PushMetric updates Prometheus metrics for each processed message.
func PushMetric(topic string, elapsedSeconds float64) {
	MessagesProcessed.Inc()
	LatencyHist.Observe(elapsedSeconds)
	MessagesPerSec.Set(1.0 / elapsedSeconds)
}
