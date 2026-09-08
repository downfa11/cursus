package controller

import (
	"fmt"

	"github.com/cursus-io/cursus/pkg/coordinator"
	"github.com/cursus-io/cursus/pkg/types"
)

// writeConsumerOffsetRecord routes an acknowledged append through the normal
// partition leader path. It is deliberately separate from metadata Raft: the
// replicated __consumer_offsets partition log is the durability boundary.
func (ch *CommandHandler) writeConsumerOffsetRecord(record coordinator.ConsumerMetadataRecord) error {
	if ch.TopicManager == nil {
		return fmt.Errorf("consumer offset topic manager is unavailable")
	}
	payload, key, err := coordinator.EncodeConsumerMetadataRecord(record)
	if err != nil {
		return err
	}
	topic := ch.TopicManager.GetTopic("__consumer_offsets")
	if topic == nil {
		return fmt.Errorf("consumer offset topic is unavailable")
	}
	msg := types.Message{Payload: string(payload), Key: key}
	partition := topic.GetPartitionForMessage(msg)
	cmd := fmt.Sprintf("PUBLISH topic=__consumer_offsets partition=%d acks=1 producerId=consumer-offset-coordinator key=%s message=%s", partition, key, payload)
	resp := ch.handlePublish(cmd, NewInternalClientContext("default-group", 0))
	if len(resp) < 2 || (resp[:2] != "OK" && resp[0] != '{') {
		return fmt.Errorf("consumer offset append failed: %s", resp)
	}
	return nil
}
