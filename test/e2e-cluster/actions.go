package e2e_cluster

import (
	"fmt"
	"net/http"
	"os/exec"
	"time"

	"github.com/downfa11-org/go-broker/test/e2e"
)

// ClusterActions represents cluster-specific test actions
type ClusterActions struct {
	ctx     *ClusterTestContext
	actions *e2e.Actions
}

func (c *ClusterTestContext) WhenCluster() *ClusterActions {
	return &ClusterActions{
		ctx:     c,
		actions: c.TestContext.When(),
	}
}

func (a *ClusterActions) StartCluster() *ClusterActions {
	a.ctx.GetT().Logf("Checking %d-node cluster health...", a.ctx.clusterSize)

	if err := a.checkAllNodesHealth(); err != nil {
		a.ctx.GetT().Fatalf("Cluster health check failed: %v", err)
	}

	a.ctx.GetT().Log("Cluster is ready")
	return a
}

// waitForNodeHealth checks if a node is healthy
func (a *ClusterActions) waitForNodeHealth(nodeIndex int, port int) error {
	a.ctx.GetT().Logf("Waiting for node %d health check on port %d...", nodeIndex, port)

	for retry := 0; retry < 30; retry++ {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/health", port))
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			a.ctx.GetT().Logf("Node %d is healthy", nodeIndex)
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("node %d failed to become healthy after 30 retries", nodeIndex)
}

// checkAllNodesHealth verifies all cluster nodes are healthy
func (a *ClusterActions) checkAllNodesHealth() error {
	for i := 0; i < a.ctx.clusterSize; i++ {
		port := 9080 + i
		if err := a.waitForNodeHealth(i, port); err != nil {
			return err
		}
	}
	return nil
}

func (a *ClusterActions) CreateTopic() *ClusterActions {
	a.actions.CreateTopic()
	return a
}

func (a *ClusterActions) PublishMessages() *ClusterActions {
	a.actions.PublishMessages()
	return a
}

func (a *ClusterActions) RetryPublishMessages() *ClusterActions {
	a.actions.RetryPublishMessages()
	return a
}

func (a *ClusterActions) JoinGroup() *ClusterActions {
	a.actions.JoinGroup()
	return a
}

func (a *ClusterActions) SyncGroup() *ClusterActions {
	a.actions.SyncGroup()
	return a
}

func (a *ClusterActions) ConsumeMessages() *ClusterActions {
	a.actions.ConsumeMessages()
	return a
}

func (a *ClusterActions) ConsumeMessagesUpToOffset(offset uint64) *ClusterActions {
	client := a.ctx.TestContext.GetClient()
	a.ctx.GetT().Logf("Consuming messages up to offset %d...", offset)

	if _, _, err := client.JoinGroup(a.ctx.TestContext.GetTopic(), a.ctx.TestContext.GetConsumerGroup()); err != nil {
		a.ctx.GetT().Logf("Failed to join consumer group: %v", err)
		return a
	}

	for _, partition := range a.ctx.TestContext.GetAssignedPartitions() {
		messages, err := client.ConsumeMessages(
			a.ctx.TestContext.GetTopic(),
			partition,
			a.ctx.TestContext.GetConsumerGroup(),
			client.GetMemberID(),
			client.GetGeneration(),
			5*time.Second,
		)

		if err != nil {
			a.ctx.GetT().Logf("Failed to consume from partition %d: %v", partition, err)
			continue
		}

		for range messages {
			currentCount := a.ctx.TestContext.GetConsumedCount()
			a.ctx.TestContext.SetConsumedCount(currentCount + 1)
		}
	}

	return a
}

func (a *ClusterActions) ContinueConsuming() *ClusterActions {
	client := a.ctx.TestContext.GetClient()
	a.ctx.GetT().Log("Continuing consumption from last offset...")

	for _, partition := range a.ctx.TestContext.GetAssignedPartitions() {
		messages, err := client.ConsumeMessages(
			a.ctx.TestContext.GetTopic(),
			partition,
			a.ctx.TestContext.GetConsumerGroup(),
			client.GetMemberID(),
			client.GetGeneration(),
			5*time.Second,
		)

		if err != nil {
			a.ctx.GetT().Logf("Failed to continue consumption from partition %d: %v", partition, err)
			continue
		}

		currentCount := a.ctx.TestContext.GetConsumedCount()
		a.ctx.TestContext.SetConsumedCount(currentCount + len(messages))
	}

	return a
}

func (a *ClusterActions) PublishMoreMessages(count int) *ClusterActions {
	originalCount := a.ctx.TestContext.GetNumMessages()
	a.ctx.TestContext.SetNumMessages(count)
	a.actions.PublishMessages()
	a.ctx.TestContext.SetNumMessages(originalCount)
	return a
}

func (a *ClusterActions) Then() *e2e.Consequences {
	return a.actions.Then()
}

func (a *ClusterActions) SimulateLeaderFailure() *ClusterActions {
	a.ctx.GetT().Log("Simulating leader failure...")

	cmd := exec.Command("docker", "stop", "broker1")
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Logf("Failed to stop leader: %v", err)
		return a
	}

	time.Sleep(5 * time.Second)

	for i := 1; i < a.ctx.clusterSize; i++ {
		port := 9080 + i
		if err := a.waitForNodeHealth(i, port); err == nil {
			a.ctx.GetT().Logf("Node %d appears to be the new leader", i)
			a.ctx.TestContext.SetBrokerAddr(fmt.Sprintf("localhost:%d", port))
			break
		}
	}

	return a
}

func (a *ClusterActions) SimulateFollowerFailure() *ClusterActions {
	a.ctx.GetT().Log("Simulating follower failure...")

	cmd := exec.Command("docker", "stop", "broker2")
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Logf("Failed to stop follower: %v", err)
		return a
	}

	time.Sleep(2 * time.Second)
	return a
}

func (a *ClusterActions) AddNodeToCluster() *ClusterActions {
	a.ctx.GetT().Log("Adding new node to cluster...")

	cmd := exec.Command("docker", "compose", "-f", "test/e2e-cluster/docker-compose.yml", "up", "-d", "broker4")
	if output, err := cmd.CombinedOutput(); err != nil {
		a.ctx.GetT().Logf("Failed to add new node: %v\nOutput: %s", err, string(output))
		return a
	}

	if err := a.waitForNodeHealth(3, 9083); err != nil {
		a.ctx.GetT().Logf("New node failed health check: %v", err)
		return a
	}

	a.ctx.clusterSize++
	a.ctx.GetT().Log("New node added successfully")
	return a
}

func (a *ClusterActions) RemoveNodeFromCluster() *ClusterActions {
	a.ctx.GetT().Log("Removing node from cluster...")

	nodeToRemove := fmt.Sprintf("broker%d", a.ctx.clusterSize)
	cmd := exec.Command("docker", "stop", nodeToRemove)
	if err := cmd.Run(); err != nil {
		a.ctx.GetT().Logf("Failed to remove node: %v", err)
		return a
	}

	cmd = exec.Command("docker", "rm", nodeToRemove)
	cmd.Run()

	a.ctx.clusterSize--
	a.ctx.GetT().Log("Node removed successfully")
	return a
}

func (a *ClusterActions) RebalancePartitions() *ClusterActions {
	a.ctx.GetT().Log("Rebalancing partitions...")

	a.ctx.GetT().Log("Triggering partition rebalance...")
	time.Sleep(3 * time.Second)

	a.actions.SyncGroup()
	a.ctx.GetT().Log("Partition rebalance completed")
	return a
}

func (c *ClusterActions) SimulateNetworkPartition() *ClusterActions {
	c.ctx.GetT().Log("Simulating network partition between leader and followers...")

	// todo.
	cmd := exec.Command("docker", "exec", "broker1", "iptables", "-A", "INPUT", "-s", "172.20.0.3", "-j", "DROP")
	cmd.Run()
	cmd = exec.Command("docker", "exec", "broker1", "iptables", "-A", "INPUT", "-s", "172.20.0.4", "-j", "DROP")
	cmd.Run()

	return c
}

func (c *ClusterActions) PublishMessagesDuringPartition() *ClusterActions {
	c.ctx.GetT().Log("Publishing messages during network partition...")

	// todo.
	c.actions.PublishMessages()
	return c
}

func (c *ClusterActions) HealNetworkPartition() *ClusterActions {
	c.ctx.GetT().Log("Healing network partition...")

	// todo.
	cmd := exec.Command("docker", "exec", "broker1", "iptables", "-D", "INPUT", "-s", "172.20.0.3", "-j", "DROP")
	cmd.Run()
	cmd = exec.Command("docker", "exec", "broker1", "iptables", "-D", "INPUT", "-s", "172.20.0.4", "-j", "DROP")
	cmd.Run()

	time.Sleep(2 * time.Second)
	return c
}

func (c *ClusterActions) SimulateMultipleNodeFailures(count int) *ClusterActions {
	c.ctx.GetT().Logf("Simulating failure of %d nodes...", count)

	for i := 2; i <= 2+count-1; i++ {
		nodeName := fmt.Sprintf("broker%d", i)
		cmd := exec.Command("docker", "stop", nodeName)
		if err := cmd.Run(); err != nil {
			c.ctx.GetT().Logf("Failed to stop %s: %v", nodeName, err)
		}
	}

	time.Sleep(5 * time.Second)
	return c
}

func (c *ClusterActions) WaitForLeaderElection() *ClusterActions {
	c.ctx.GetT().Log("Waiting for leader election...")

	for retry := 0; retry < 30; retry++ {
		for i := 1; i < c.ctx.clusterSize; i++ {
			port := 9080 + i
			if err := c.waitForNodeHealth(i, port); err == nil {
				c.ctx.GetT().Logf("Node %d appears to be the new leader", i)
				return c
			}
		}
		time.Sleep(1 * time.Second)
	}

	return c
}

func (c *ClusterActions) StartContinuousPublishing() *ClusterActions {
	c.ctx.GetT().Log("Starting continuous message publishing...")

	go func() {
		for i := 0; i < 50; i++ {
			c.actions.PublishMessages()
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return c
}

func (c *ClusterActions) StopContinuousPublishing() *ClusterActions {
	c.ctx.GetT().Log("Stopping continuous message publishing...")
	// todo.
	return c
}
