package stream

import (
	"fmt"
	"net"
	"sync"
	"time"
)

type StreamManager struct {
	streams   map[string]*StreamConnection // key: "topic:partition:group"
	mu        sync.RWMutex
	maxConn   int
	timeout   time.Duration
	heartbeat time.Duration
}

type StreamConnection struct {
	conn       net.Conn
	topic      string
	partition  int
	group      string
	offset     uint64
	lastActive time.Time
	stopCh     chan struct{}
}

func NewStreamManager(maxConn int, timeout, heartbeat time.Duration) *StreamManager {
	return &StreamManager{
		streams:   make(map[string]*StreamConnection),
		maxConn:   maxConn,
		timeout:   timeout,
		heartbeat: heartbeat,
	}
}

// NewStreamConnection creates a new stream connection
func NewStreamConnection(conn net.Conn, topic string, partition int, group string, offset uint64) *StreamConnection {
	return &StreamConnection{
		conn:       conn,
		topic:      topic,
		partition:  partition,
		group:      group,
		offset:     offset,
		stopCh:     make(chan struct{}),
		lastActive: time.Now(),
	}
}

func (sm *StreamManager) AddStream(key string, stream *StreamConnection) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if len(sm.streams) >= sm.maxConn {
		return fmt.Errorf("maximum connections (%d) reached", sm.maxConn)
	}

	sm.streams[key] = stream
	go sm.monitorConnection(key, stream)
	return nil
}

func (sm *StreamManager) RemoveStream(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if stream, ok := sm.streams[key]; ok {
		close(stream.stopCh)
		delete(sm.streams, key)
	}
}

func (sm *StreamManager) monitorConnection(key string, stream *StreamConnection) {
	ticker := time.NewTicker(sm.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-stream.stopCh:
			continue
		case <-ticker.C:
			if time.Since(stream.lastActive) > sm.timeout {
				stream.stopCh <- struct{}{}
				sm.RemoveStream(key)
				return
			}
		}
	}
}

func (sm *StreamManager) GetStreamsForPartition(topic string, partitionID int) []*StreamConnection {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var streams []*StreamConnection
	for _, stream := range sm.streams {
		if stream.topic == topic && stream.partition == partitionID {
			streams = append(streams, stream)
		}
	}
	return streams
}

func (sc *StreamConnection) Topic() string             { return sc.topic }
func (sc *StreamConnection) Partition() int            { return sc.partition }
func (sc *StreamConnection) Group() string             { return sc.group }
func (sc *StreamConnection) Offset() uint64            { return sc.offset }
func (sc *StreamConnection) Conn() net.Conn            { return sc.conn }
func (sc *StreamConnection) SetOffset(o uint64)        { sc.offset = o }
func (sc *StreamConnection) StopCh() <-chan struct{}   { return sc.stopCh }
func (sc *StreamConnection) SetLastActive(t time.Time) { sc.lastActive = t }
func (sc *StreamConnection) IncrementOffset()          { sc.offset++ }
