package e2e_cluster

import (
	"testing"

	"github.com/downfa11-org/go-broker/test/e2e"
)

// TestDataReplication tests data replication across cluster
func TestDataReplication(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("replication-test").
		WithPartitions(3).
		WithNumMessages(100).
		WithAcks("all").
		WithClusterSize(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		Then().
		Expect(MessagesReplicatedToAllNodes()).
		And(OffsetsInSync())
}

// TestISRWithAllAcks tests ISR behavior with acks=all
func TestISRWithAllAcks(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("isr-test").
		WithPartitions(1).
		WithNumMessages(50).
		WithAcks("all").
		WithMinInSyncReplicas(2)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateFollowerFailure().
		Then().
		Expect(MessagesPublishedWithQuorum()).
		And(ISRMaintained())
}

// TestExactlyOnceInCluster tests exactly-once semantics in cluster
func TestExactlyOnceInCluster(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("exactly-once-cluster").
		WithPartitions(3).
		WithNumMessages(100).
		WithAcks("all").
		WithClusterSize(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateLeaderFailure().
		RetryPublishMessages().
		Then().
		Expect(NoDuplicateMessages()).
		And(e2e.MessagesConsumed(100))
}

// TestClusterReconfiguration tests node addition/removal
func TestClusterReconfiguration(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("reconfig-test").
		WithPartitions(2).
		WithNumMessages(50).
		WithClusterSize(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		AddNodeToCluster().
		RebalancePartitions().
		PublishMoreMessages(50).
		RemoveNodeFromCluster().
		Then().
		Expect(ClusterStable()).
		And(e2e.MessagesConsumed(100))
}

// TestReplicationAsTimeline tests replication as timeline continuity
func TestReplicationAsTimeline(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("timeline-test").
		WithPartitions(1).
		WithNumMessages(100)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		JoinGroup().
		ConsumeMessagesUpToOffset(50).
		SimulateLeaderFailure().
		ContinueConsuming().
		Then().
		Expect(ConsumptionContinuityFromOffset(50)).
		And(NoDataLoss())
}

// TestNetworkPartition tests leader-follower network partition scenario
func TestNetworkPartition(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("network-partition-test").
		WithPartitions(3).
		WithNumMessages(100).
		WithAcks("all").
		WithClusterSize(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateNetworkPartition().
		PublishMessagesDuringPartition().
		HealNetworkPartition().
		Then().
		Expect(MessagesReplicatedAfterPartitionHeal()).
		And(NoDataLossDuringPartition())
}

// TestSimultaneousNodeFailures tests multiple nodes failing simultaneously
func TestSimultaneousNodeFailures(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("multi-failure-test").
		WithPartitions(3).
		WithNumMessages(150).
		WithAcks("all").
		WithClusterSize(5).
		WithMinInSyncReplicas(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		SimulateMultipleNodeFailures(2).
		WaitForLeaderElection().
		PublishMoreMessages(50).
		Then().
		Expect(ClusterMaintainsQuorum()).
		And(MessagesPublishedWithQuorum()).
		And(e2e.MessagesConsumed(150))
}

// TestRebalancingMessageLoss tests message publishing/consuming during rebalancing
func TestRebalancingMessageLoss(t *testing.T) {
	ctx := GivenCluster(t).
		WithTopic("rebalance-loss-test").
		WithPartitions(4).
		WithNumMessages(100).
		WithAcks("all").
		WithClusterSize(3)
	defer ctx.Cleanup()

	ctx.WhenCluster().
		StartCluster().
		CreateTopic().
		PublishMessages().
		StartContinuousPublishing().
		AddNodeToCluster().
		RebalancePartitions().
		StopContinuousPublishing().
		ContinueConsuming().
		Then().
		Expect(NoMessagesLostDuringRebalance()).
		And(e2e.MessagesConsumed(100)).
		And(ClusterStable())
}
