package offset_test

import (
	"testing"

	"github.com/downfa11-org/go-broker/pkg/offset"
)

func TestOffsetManager(t *testing.T) {
	om := offset.NewOffsetManager()

	groupID := "group1"
	topic := "topicA"
	partition := 0

	// GetOffset tried → error
	if _, err := om.GetOffset(groupID, topic, partition); err == nil {
		t.Fatalf("expected error for non-existent offset, got nil")
	}

	// After CommitOffset, GetOffset
	if err := om.CommitOffset(groupID, topic, partition, 42); err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}

	offsetValue, err := om.GetOffset(groupID, topic, partition)
	if err != nil {
		t.Fatalf("GetOffset failed after commit: %v", err)
	}

	if offsetValue != 42 {
		t.Fatalf("expected offset 42, got %d", offsetValue)
	}

	if err := om.CommitOffset(groupID, "topicB", 1, 100); err != nil {
		t.Fatalf("CommitOffset failed: %v", err)
	}

	offsetValue, err = om.GetOffset(groupID, "topicB", 1)
	if err != nil || offsetValue != 100 {
		t.Fatalf("expected offset 100, got %d, err=%v", offsetValue, err)
	}
}
