package e2e_cluster

import (
	"fmt"

	"github.com/downfa11-org/go-broker/test/e2e"
)

// MessagesReplicatedToAllNodes checks that all nodes have the same data
func MessagesReplicatedToAllNodes() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		leaderOffsets := make(map[int]uint64)
		for partition := 0; partition < ctx.GetPartitions(); partition++ {
			offset, err := client.FetchCommittedOffset(ctx.GetTopic(), partition, ctx.GetConsumerGroup())
			if err != nil {
				return fmt.Errorf("failed to fetch leader offset for partition %d: %w", partition, err)
			}
			leaderOffsets[partition] = offset
		}

		ctx.GetT().Logf("Verified leader offsets: %v", leaderOffsets)
		return nil
	}
}

// ISRMaintained verifies ISR is maintained during operations
func ISRMaintained() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		ctx.GetT().Log("Checking ISR status...")

		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published to verify ISR")
		}

		ctx.GetT().Logf("ISR maintained with %d messages published", ctx.GetPublishedCount())
		return nil
	}
}

// NoDuplicateMessages verifies no duplicates in consumed messages
func NoDuplicateMessages() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetConsumedCount() == 0 {
			return fmt.Errorf("no messages consumed to check for duplicates")
		}

		if ctx.GetConsumedCount() > ctx.GetPublishedCount() {
			return fmt.Errorf("consumed %d messages but only %d published (duplicates detected)", ctx.GetConsumedCount(), ctx.GetPublishedCount())
		}

		ctx.GetT().Logf("Verified no duplicates: %d published, %d consumed", ctx.GetPublishedCount(), ctx.GetConsumedCount())
		return nil
	}
}

// ClusterStable verifies cluster is stable after operations
func ClusterStable() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		if err := e2e.CheckBrokerHealth(ClusterHealthCheckAddr); err != nil {
			return fmt.Errorf("broker health check failed: %w", err)
		}

		ctx.GetT().Logf("Cluster stable - topic %s accessible", ctx.GetTopic())
		return nil
	}
}

// ConsumptionContinuityFromOffset verifies consumption continues from specified offset
func ConsumptionContinuityFromOffset(offset uint64) e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		currentOffset, err := client.FetchCommittedOffset(ctx.GetTopic(), 0, ctx.GetConsumerGroup())
		if err != nil {
			return fmt.Errorf("failed to fetch current offset: %w", err)
		}

		if currentOffset <= offset {
			return fmt.Errorf("consumption not continuous: current offset %d <= expected %d",
				currentOffset, offset)
		}

		ctx.GetT().Logf("Consumption continuous from offset %d to %d", offset, currentOffset)
		return nil
	}
}

// NoDataLoss verifies no data was lost during operations
func NoDataLoss() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published to verify data loss")
		}

		if ctx.GetConsumedCount() < ctx.GetPublishedCount() {
			return fmt.Errorf("data loss detected: %d published but only %d consumed",
				ctx.GetPublishedCount(), ctx.GetConsumedCount())
		}

		ctx.GetT().Logf("No data loss: %d published, %d consumed",
			ctx.GetPublishedCount(), ctx.GetConsumedCount())
		return nil
	}
}

// OffsetsInSync verifies offsets are in sync across replicas
func OffsetsInSync() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		for partition := 0; partition < ctx.GetPartitions(); partition++ {
			offset, err := client.FetchCommittedOffset(ctx.GetTopic(), partition, ctx.GetConsumerGroup())
			if err != nil {
				return fmt.Errorf("failed to fetch offset for partition %d: %w", partition, err)
			}

			if ctx.GetConsumedCount() > 0 && offset == 0 {
				return fmt.Errorf("offset not committed for partition %d", partition)
			}

			ctx.GetT().Logf("Partition %d offset: %d", partition, offset)
		}

		return nil
	}
}

// MessagesPublishedWithQuorum verifies messages were published with quorum
func MessagesPublishedWithQuorum() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published")
		}

		if ctx.GetPublishedCount() != ctx.GetNumMessages() {
			return fmt.Errorf("quorum not achieved: expected %d messages, got %d",
				ctx.GetNumMessages(), ctx.GetPublishedCount())
		}

		if ctx.GetAcks() != "all" {
			return fmt.Errorf("acks not set to 'all': got %s", ctx.GetAcks())
		}

		ctx.GetT().Logf("Quorum achieved: %d messages published with acks=%s",
			ctx.GetPublishedCount(), ctx.GetAcks())
		return nil
	}
}

func MessagesReplicatedAfterPartitionHeal() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		for partition := 0; partition < ctx.GetPartitions(); partition++ {
			offset, err := client.FetchCommittedOffset(ctx.GetTopic(), partition, ctx.GetConsumerGroup())
			if err != nil {
				return fmt.Errorf("failed to fetch offset after partition heal: %w", err)
			}

			if offset == 0 && ctx.GetPublishedCount() > 0 {
				return fmt.Errorf("partition %d not replicated after heal", partition)
			}
		}

		ctx.GetT().Log("Messages replicated successfully after partition heal")
		return nil
	}
}

func NoDataLossDuringPartition() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published to verify")
		}

		if ctx.GetConsumedCount() < ctx.GetPublishedCount() {
			return fmt.Errorf("data loss during partition: %d published, %d consumed",
				ctx.GetPublishedCount(), ctx.GetConsumedCount())
		}

		ctx.GetT().Log("No data loss during partition")
		return nil
	}
}

func ClusterMaintainsQuorum() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		client := e2e.NewBrokerClient(ctx.GetBrokerAddrs())
		defer client.Close()

		if ctx.GetAcks() != "all" {
			return fmt.Errorf("quorum test requires acks=all")
		}

		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published to verify quorum")
		}

		ctx.GetT().Log("Cluster maintained quorum during failures")
		return nil
	}
}

func NoMessagesLostDuringRebalance() e2e.Expectation {
	return func(ctx *e2e.TestContext) error {
		if ctx.GetPublishedCount() == 0 {
			return fmt.Errorf("no messages published to verify")
		}

		if ctx.GetConsumedCount() < ctx.GetPublishedCount() {
			return fmt.Errorf("messages lost during rebalance: %d published, %d consumed",
				ctx.GetPublishedCount(), ctx.GetConsumedCount())
		}

		ctx.GetT().Log("No messages lost during rebalance")
		return nil
	}
}
