package benchmarks

import (
	"os"
	"strconv"
	"time"
)

type BenchmarkConfig struct {
	Duration      time.Duration
	Threads       int
	MessageSize   int
	BatchSize     int
	FsyncInterval time.Duration
	BrokerAddr    string
}

func LoadBenchmarkConfig() *BenchmarkConfig {
	cfg := &BenchmarkConfig{
		BrokerAddr: "localhost:9000",
	}

	if duration := os.Getenv("GOBROKER_DURATION"); duration != "" {
		if d, err := time.ParseDuration(duration); err == nil {
			cfg.Duration = d
		}
	}
	if cfg.Duration == 0 {
		cfg.Duration = 2 * time.Minute
	}

	if threads := os.Getenv("GOBROKER_THREADS"); threads != "" {
		if t, err := strconv.Atoi(threads); err == nil {
			cfg.Threads = t
		}
	}
	if cfg.Threads == 0 {
		cfg.Threads = 10
	}

	if size := os.Getenv("GOBROKER_MESSAGE_SIZE"); size != "" {
		if s, err := strconv.Atoi(size); err == nil {
			cfg.MessageSize = s
		}
	}
	if cfg.MessageSize == 0 {
		cfg.MessageSize = 1024
	}

	if batch := os.Getenv("GOBROKER_BATCH_SIZE"); batch != "" {
		if b, err := strconv.Atoi(batch); err == nil {
			cfg.BatchSize = b
		}
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 500
	}

	if fsync := os.Getenv("GOBROKER_FSYNC"); fsync != "" {
		if f, err := time.ParseDuration(fsync); err == nil {
			cfg.FsyncInterval = f
		}
	}

	return cfg
}
