package coordinator

import (
	"fmt"
	"sync"
	"time"

	"github.com/downfa11-org/go-broker/pkg/config"
)

type Coordinator struct {
	groups map[string]*GroupMetadata
	mu     sync.RWMutex
	cfg    *config.Config
}

type GroupMetadata struct {
	TopicName     string
	Members       map[string]*MemberMetadata
	Generation    int
	Partitions    []int
	LastRebalance time.Time
}

type MemberMetadata struct {
	ID            string
	LastHeartbeat time.Time
	Assignments   []int
}

func NewCoordinator(cfg *config.Config) *Coordinator {
	return &Coordinator{
		groups: make(map[string]*GroupMetadata),
		cfg:    cfg,
	}
}

func (c *Coordinator) Start() {
	go c.monitorHeartbeats()
}

func (c *Coordinator) RegisterGroup(topicName, groupName string, partitionCount int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	partitions := make([]int, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions[i] = i
	}

	c.groups[groupName] = &GroupMetadata{
		TopicName:  topicName,
		Members:    make(map[string]*MemberMetadata),
		Partitions: partitions,
	}
	return nil
}

func (c *Coordinator) AddConsumer(groupName, consumerID string) ([]int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		return nil, fmt.Errorf("group not found")
	}

	group.Members[consumerID] = &MemberMetadata{
		ID:            consumerID,
		LastHeartbeat: time.Now(),
	}

	c.rebalanceRange(groupName)

	return group.Members[consumerID].Assignments, nil
}

func (c *Coordinator) GetAssignments(groupName string) map[string][]int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	group := c.groups[groupName]
	if group == nil {
		return nil
	}

	result := make(map[string][]int)
	for id, member := range group.Members {
		result[id] = member.Assignments
	}
	return result
}

func (c *Coordinator) RemoveConsumer(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("group not found")
	}

	delete(group.Members, consumerID)
	c.rebalanceRange(groupName)
	return nil
}

func (c *Coordinator) RecordHeartbeat(groupName, consumerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	group := c.groups[groupName]
	if group == nil {
		return fmt.Errorf("group not found")
	}

	member := group.Members[consumerID]
	if member == nil {
		return fmt.Errorf("consumer not found")
	}

	member.LastHeartbeat = time.Now()
	return nil
}
