package types

const (
	IndexEntrySize = 16 // offset(8) + position(8)
)

type IndexEntry struct {
	Offset   uint64
	Position uint64
}

// SegmentFile manages the metadata and lifecycle of a specific log segment.
type SegmentFile struct {
	Path     string
	RefCount int32
	Deleted  int32 // 1 if the segment is marked for deletion (soft-delete)
}
