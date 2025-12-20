package e2e_cluster

import (
	"testing"
)

// x
// TestDataReplication tests data replication across cluster
func TestDataReplication(t *testing.T) {
	ctx := GivenClusterRestart(t).
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
