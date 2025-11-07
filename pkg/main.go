package main

import (
	"fmt"
	"go-broker/pkg/config"
	"go-broker/pkg/disk"
	"go-broker/pkg/server"
	"go-broker/pkg/topic"
	"log"
)

func main() {
	// --- Configuration ---
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	fmt.Printf("🚀 Starting broker on port %d\n", cfg.BrokerPort)
	fmt.Printf("🧠 Benchmark: %v | 📊 Exporter: %v\n", cfg.EnableBenchmark, cfg.EnableExporter)

	// --- Initialization ---
	dm := disk.NewDiskManager(cfg.LogDir, cfg.BufferSize)
	tm := topic.NewTopicManager(cfg, dm)

	if err := server.RunServer(cfg, tm, dm); err != nil {
		log.Fatalf("❌ Broker failed: %v", err)
	}
}
