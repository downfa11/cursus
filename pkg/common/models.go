package common

type Message struct {
	ID      uint64
	Payload string
	Offset  uint64
}

type AppendResult struct {
	SegmentIndex int
	Offset       int
}
