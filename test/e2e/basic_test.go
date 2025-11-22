package e2e

import (
	"testing"
)

func TestBasicPubSub(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.WithTopic("test-topic").
		WithPartitions(4).
		When().
		StartBroker().
		PublishMessages().
		ConsumeMessages().
		Then().
		Expect(BrokerIsHealthy()).
		And(MessagesPublishedSince(10, ctx.startTime)).
		And(MessagesConsumed(10))
}

func TestConfigValidation(t *testing.T) {
	ctx := Given(t)
	defer ctx.Cleanup()

	ctx.
		WithTopic("config-test").
		WithPartitions(2).
		WithNumMessages(5).
		When().
		StartBroker().
		PublishMessages().
		Then().
		Expect(MessagesPublishedSince(5, ctx.startTime))
}
