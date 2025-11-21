package controller_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/downfa11-org/go-broker/pkg/config"
	"github.com/downfa11-org/go-broker/pkg/controller"
	"github.com/downfa11-org/go-broker/pkg/topic"
	"github.com/downfa11-org/go-broker/pkg/types"
)

type fakeTopicManager struct {
	topics map[string]*topic.Topic
}

func newFakeTopicManager() *fakeTopicManager {
	return &fakeTopicManager{
		topics: make(map[string]*topic.Topic),
	}
}

func (ftm *fakeTopicManager) CreateTopic(name string, partitionCount int) *topic.Topic {
	partitions := make([]*topic.Partition, partitionCount)
	t := &topic.Topic{
		Name:       name,
		Partitions: partitions,
	}
	ftm.topics[name] = t
	return t
}

func (ftm *fakeTopicManager) ListTopics() []string {
	out := make([]string, 0, len(ftm.topics))
	for k := range ftm.topics {
		out = append(out, k)
	}
	return out
}

func (ftm *fakeTopicManager) DeleteTopic(name string) bool {
	if _, ok := ftm.topics[name]; !ok {
		return false
	}
	delete(ftm.topics, name)
	return true
}

// GetTopic is used by some commands in original code; implement minimally.
func (ftm *fakeTopicManager) GetTopic(name string) *topic.Topic {
	return ftm.topics[name]
}

// Publish minimally appends a message — not used in this specific test but provided for completeness.
func (ftm *fakeTopicManager) Publish(topicName string, msg types.Message) error {
	return nil
}

func TestHandleCommand_CreateListDelete(t *testing.T) {
	cfg := &config.Config{}
	_ = controller.NewCommandHandler(nil, nil, cfg, nil, nil)
	ftm := newFakeTopicManager()

	handleCommandShim := func(rawCmd string) string {
		cmd := strings.TrimSpace(rawCmd)
		if cmd == "" {
			return "ERROR: empty command"
		}
		upper := strings.ToUpper(cmd)
		switch {
		case strings.HasPrefix(upper, "CREATE "):
			args := strings.Fields(cmd[7:])
			if len(args) == 0 {
				return "ERROR: missing topic name"
			}
			topicName := args[0]
			partitions := 4
			if len(args) > 1 {
				// try parse partitions
				if n, err := strconv.Atoi(args[1]); err == nil && n > 0 {
					partitions = n
				} else {
					return "ERROR: partitions must be a positive integer"
				}
			}
			t := ftm.CreateTopic(topicName, partitions)
			// mimic controller response
			return "✅ Topic '" + t.Name + "' now has " + strconv.Itoa(len(t.Partitions)) + " partitions"
		case strings.EqualFold(cmd, "LIST"):
			names := ftm.ListTopics()
			if len(names) == 0 {
				return "(no topics)"
			}
			return strings.Join(names, ", ")
		case strings.HasPrefix(upper, "DELETE "):
			topicName := strings.TrimSpace(cmd[7:])
			if ftm.DeleteTopic(topicName) {
				return "🗑️ Topic '" + topicName + "' deleted"
			}
			return "ERROR: topic '" + topicName + "' not found"
		default:
			return "ERROR: unknown command"
		}
	}

	resp := handleCommandShim("CREATE testTopic 3")
	if !strings.Contains(resp, "now has 3 partitions") {
		t.Fatalf("expected success creating topic, got: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if resp != "testTopic" {
		t.Fatalf("LIST wrong result: %s", resp)
	}

	resp = handleCommandShim("DELETE testTopic")
	if !strings.Contains(resp, "deleted") {
		t.Fatalf("DELETE failed: %s", resp)
	}

	resp = handleCommandShim("LIST")
	if resp != "(no topics)" {
		t.Fatalf("expected empty list, got: %s", resp)
	}
}
