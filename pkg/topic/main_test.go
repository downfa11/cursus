package topic

import (
	"go-broker/pkg/config"
	"go-broker/pkg/disk"
	"go-broker/pkg/types"
	"testing"
	"time"
)

func TestTopicManager(t *testing.T) {
	cfg := &config.Config{CleanupInterval: 1}
	dm := disk.NewDiskManager(cfg.LogDir, cfg.BufferSize)
	tm := NewTopicManager(cfg, dm)

	t1 := tm.CreateTopic("topic1", 4)
	t2 := tm.GetTopic("topic1")
	if t1 != t2 {
		t.Errorf("CreateTopic/GetTopic returned different instances")
	}

	tm.Publish("topic1", types.Message{Payload: "hello"})
	time.Sleep(50 * time.Millisecond)

	total := 0
	for _, p := range t1.Partitions {
		total += len(p.ch)
	}
	if total != 1 {
		t.Errorf("expected total size 1 after first publish, got %d", total)
	}

	tm.Publish("topic1", types.Message{Payload: "hello"})
	time.Sleep(20 * time.Millisecond)

	total = 0
	for _, p := range t1.Partitions {
		total += len(p.ch)
	}
	if total != 1 {
		t.Errorf("duplicate message inserted, total=%d", total)
	}

	time.Sleep(2 * time.Second)
	tm.CleanupDedup()

	for _, p := range t1.Partitions {
		for len(p.ch) > 0 {
			<-p.ch
		}
	}
	for i, p := range t1.Partitions {
		if len(p.ch) != 0 {
			t.Errorf("partition[%d] cleanup failed, size=%d", i, len(p.ch))
		}
	}
}
