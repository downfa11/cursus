package types

// Message represents a single message
type Message struct {
	ID      uint64
	Payload string
	Offset  uint64
	Key     string // optional: partition routing key
}

func (m Message) String() string {
	return m.Payload
}

// AppendResult represents the result of appending a message to storage
type AppendResult struct {
	SegmentIndex int
	Offset       int
}
