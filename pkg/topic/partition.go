package topic

import (
	"fmt"
	"time"

	"github.com/downfa11-org/go-broker/pkg/types"
	"github.com/downfa11-org/go-broker/util"
)

type AbsoluteOffsetProvider interface {
	GetAbsoluteOffset() uint64
}

func (p *Partition) runBroadcaster() {
	util.Debug("🚀 partition %d: Broadcaster worker started", p.id)
	for msg := range p.broadcastCh {
		p.broadcastToStreams(msg)
	}
	util.Debug("🛑 partition %d: Broadcaster worker stopped", p.id)
}

// Enqueue pushes a message into the partition queue.
func (p *Partition) Enqueue(msg types.Message) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		util.Warn("⚠️ Partition closed, dropping message [partition-%d]", p.id)
		return
	}

	if appender, ok := p.dh.(DiskAppender); ok {
		util.Debug("Calling AppendMessage for disk persistence [partition-%d]", p.id)
		serialized, err := util.SerializeMessage(msg)
		if err != nil {
			util.Warn("⚠️ Failed to serialize message for disk persistence [partition-%d]: %v", p.id, err)
			return
		}
		appender.AppendMessage(p.topic, p.id, string(serialized))
		p.NotifyNewMessage()
	} else {
		util.Warn("⚠️ DiskHandler does not implement AppendMessage [partition-%d]\n", p.id)
	}

	p.enqueueToBroadcast(msg)
}

func (p *Partition) EnqueueSync(msg types.Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	if appender, ok := p.dh.(interface {
		AppendMessageSync(topic string, partition int, payload string) error
	}); ok {
		serialized, err := util.SerializeMessage(msg)
		if err != nil {
			util.Warn("⚠️ Failed to serialize message for disk persistence [partition-%d]: %v", p.id, err)
			return err
		}
		if err := appender.AppendMessageSync(p.topic, p.id, string(serialized)); err != nil {
			return fmt.Errorf("disk write failed: %w", err)
		}
		p.NotifyNewMessage()
	} else {
		return fmt.Errorf("disk handler does not support sync write")
	}

	p.enqueueToBroadcast(msg)
	return nil
}

// EnqueueBatchSync pushes multiple messages into the partition queue synchronously.
func (p *Partition) EnqueueBatchSync(msgs []types.Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	appender, ok := p.dh.(DiskAppender)
	if !ok {
		return fmt.Errorf("disk handler does not implement sync write")
	}

	for _, msg := range msgs {
		serialized, err := util.SerializeMessage(msg)
		if err != nil {
			util.Warn("⚠️ Failed to serialize message for disk persistence [partition-%d]: %v", p.id, err)
			return err
		}

		if err := appender.AppendMessageSync(p.topic, p.id, string(serialized)); err != nil {
			return fmt.Errorf("disk write failed for partition %d: %w", p.id, err)
		}
		p.enqueueToBroadcast(msg)
	}
	p.NotifyNewMessage()
	return nil
}

// EnqueueBatch pushes multiple messages into the partition queue asynchronously.
func (p *Partition) EnqueueBatch(msgs []types.Message) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return fmt.Errorf("partition %d is closed", p.id)
	}

	if appender, ok := p.dh.(DiskAppender); ok {
		for _, msg := range msgs {
			serialized, err := util.SerializeMessage(msg)
			if err != nil {
				util.Warn("⚠️ Failed to serialize message for disk persistence [partition-%d]: %v", p.id, err)
				return err
			}
			appender.AppendMessage(p.topic, p.id, string(serialized))
			p.enqueueToBroadcast(msg)
		}
		p.NotifyNewMessage()
	} else {
		return fmt.Errorf("disk handler does not implement async write")
	}

	return nil
}

func (p *Partition) enqueueToBroadcast(msg types.Message) {
	select {
	case p.broadcastCh <- msg:
	default:
		util.Warn("⚠️ partition %d: Broadcast channel full, dropping real-time delivery", p.id)
	}
}

func (p *Partition) broadcastToStreams(msg types.Message) {
	if p.streamManager == nil {
		return
	}

	streams := p.streamManager.GetStreamsForPartition(p.topic, p.id)
	for _, stream := range streams {
		conn := stream.Conn()
		if conn == nil {
			continue
		}

		if err := conn.SetWriteDeadline(time.Now().Add(1 * time.Second)); err != nil {
			util.Error("⚠️ SetWriteDeadline error: %v", err)
			continue
		}

		if err := util.WriteWithLength(conn, []byte(msg.Payload)); err != nil {
			util.Warn("Failed to broadcast to stream for topic '%s' partition %d: %v", p.topic, p.id, err)
			continue
		}

		stream.IncrementOffset()
		stream.SetLastActive(time.Now())

	}
}

func (p *Partition) NotifyNewMessage() {
	select {
	case p.newMessageCh <- struct{}{}:
	default:
	}
}

// Close shuts down the partition.
func (p *Partition) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.broadcastCh)
}
