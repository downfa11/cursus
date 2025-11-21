package e2e

import (
	"testing"
)

// TestConsumerGroupJoin verifies consumer group join functionality
func TestConsumerGroupJoin(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("consumer-group-test").
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(ConsumerJoinedGroup()).
		And(MessagesConsumed(10))
}

// TestConsumerOffsetCommit verifies offset commit functionality
func TestConsumerOffsetCommit(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("offset-test").
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(OffsetsCommitted())
}

// TestConsumerHeartbeat verifies heartbeat mechanism
func TestConsumerHeartbeat(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("heartbeat-test").
		When().
		StartBroker().
		ConsumeMessages().
		Then().
		Expect(HeartbeatsSent())
}
