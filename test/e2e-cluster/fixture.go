package e2e_cluster

import (
	"os/exec"
	"testing"
	"time"

	"github.com/downfa11-org/go-broker/test/e2e"
)

// ClusterTestContext extends e2e.TestContext for cluster testing
type ClusterTestContext struct {
	*e2e.TestContext
	clusterSize       int
	minInSyncReplicas int
	nodeAddresses     []string
}

// GivenCluster creates a new cluster test context
func GivenCluster(t *testing.T) *ClusterTestContext {
	return &ClusterTestContext{
		TestContext:       e2e.Given(t),
		clusterSize:       3,
		minInSyncReplicas: 2,
		nodeAddresses:     []string{"localhost:9080", "localhost:9081", "localhost:9082"},
	}
}

func GivenClusterRestart(t *testing.T) *ClusterTestContext {
	cmd := exec.Command("docker-compose", "-f", "docker-compose.yml", "up", "-d")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to start docker-compose: %v\nOutput: %s", err, string(output))
	}

	t.Cleanup(func() {
		exec.Command("docker-compose", "-f", "docker-compose.yml", "down", "-v").Run()
	})

	time.Sleep(15 * time.Second)
	return GivenCluster(t)
}

func (c *ClusterTestContext) WithTopic(topic string) *ClusterTestContext {
	c.TestContext.WithTopic(topic)
	return c
}

func (c *ClusterTestContext) WithPartitions(partitions int) *ClusterTestContext {
	c.TestContext.WithPartitions(partitions)
	return c
}

func (c *ClusterTestContext) WithNumMessages(num int) *ClusterTestContext {
	c.TestContext.WithNumMessages(num)
	return c
}

func (c *ClusterTestContext) WithAcks(acks string) *ClusterTestContext {
	c.TestContext.WithAcks(acks)
	return c
}

func (c *ClusterTestContext) WithConsumerGroup(group string) *ClusterTestContext {
	c.TestContext.WithConsumerGroup(group)
	return c
}

func (c *ClusterTestContext) WithClusterSize(size int) *ClusterTestContext {
	c.clusterSize = size
	return c
}

func (c *ClusterTestContext) WithMinInSyncReplicas(min int) *ClusterTestContext {
	c.minInSyncReplicas = min
	return c
}

func (c *ClusterTestContext) WithNodeAddresses(addrs []string) *ClusterTestContext {
	c.nodeAddresses = addrs
	return c
}
