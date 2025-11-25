package main

import (
	"encoding/json"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/coordinator"
	"github.com/downfa11-org/go-broker/pkg/disk"
	"github.com/downfa11-org/go-broker/pkg/offset"
	"github.com/downfa11-org/go-broker/pkg/server"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/util"
)

func main() {
	// Configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		util.Fatal("❌ Failed to load config: %v", err)
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		util.Error("Failed to marshal config: %v", err)
	} else {
		util.Info("Configuration:\n%s", string(data))
	}

	util.Info("🚀 Starting broker on port %d\n", cfg.BrokerPort)
	util.Info("🧠 Benchmark: %v | 📊 Exporter: %v\n", cfg.EnableBenchmark, cfg.EnableExporter)

	// Initialization
	dm := disk.NewDiskManager(cfg)
	cd := coordinator.NewCoordinator(cfg)
	tm := topic.NewTopicManager(cfg, dm, cd)
	om := offset.NewOffsetManager()

	// Static consumer groups
	for _, gcfg := range cfg.StaticConsumerGroups {
		for _, topicName := range gcfg.Topics {
			t := tm.GetTopic(topicName)
			if t == nil && cfg.AutoCreateTopics {
				t = tm.CreateTopic(topicName, 4)
			}
			if t != nil {
				if err := tm.RegisterConsumerGroup(topicName, gcfg.Name, gcfg.ConsumerCount); err != nil {
					util.Error("⚠️ Failed to register static consumer group %q on topic %q: %v", gcfg.Name, topicName, err)
				}
			}
		}
	}

	go cd.Start()

	if err := server.RunServer(cfg, tm, dm, om, cd); err != nil {
		util.Fatal("❌ Broker failed: %v", err)
	}
}
