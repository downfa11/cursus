package offset

import (
	"fmt"
	"sync"
)

type OffsetManager struct {
	mu      sync.RWMutex
	offsets map[string]map[string]map[int]uint64 // groupID -> topic -> partition -> offset
}

func NewOffsetManager() *OffsetManager {
	return &OffsetManager{
		offsets: make(map[string]map[string]map[int]uint64),
	}
}

// GetOffset returns the stored offset for a given group/topic/partition.
func (om *OffsetManager) GetOffset(groupID, topic string, partition int) (uint64, error) {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if topics, ok := om.offsets[groupID]; ok {
		if partitions, ok := topics[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				return offset, nil
			}
		}
	}
	return 0, fmt.Errorf("no offset found for group '%s', topic '%s', partition %d", groupID, topic, partition)
}

// CommitOffset saves the offset for a given group/topic/partition.
func (om *OffsetManager) CommitOffset(groupID, topic string, partition int, offset uint64) error {
	om.mu.Lock()
	defer om.mu.Unlock()

	if _, ok := om.offsets[groupID]; !ok {
		om.offsets[groupID] = make(map[string]map[int]uint64)
	}
	if _, ok := om.offsets[groupID][topic]; !ok {
		om.offsets[groupID][topic] = make(map[int]uint64)
	}
	om.offsets[groupID][topic][partition] = offset
	return nil
}
