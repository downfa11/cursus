package disk_test

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/downfa11-org/cursus/pkg/config"
	"github.com/downfa11-org/cursus/pkg/disk"
	"github.com/downfa11-org/cursus/pkg/types"
)

func setupDiskHandlerWithIndex(t *testing.T) *disk.DiskHandler {
	tmpDir := t.TempDir()
	cfg := &config.Config{
		LogDir:             tmpDir,
		DiskFlushBatchSize: 2,
		LingerMS:           100,
		DiskWriteTimeoutMS: 500,
		SegmentRollTimeMS:  500,
		SegmentSize:        1024,
		IndexIntervalBytes: 10,
	}

	dh, err := disk.NewDiskHandler(cfg, "testTopic", 0)
	if err != nil {
		t.Fatalf("failed to create DiskHandler: %v", err)
	}

	return dh
}

func TestOpenAndCloseIndexFiles(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer dh.Close()

	if err := dh.OpenIndexFiles(); err != nil {
		t.Fatalf("failed to open index files: %v", err)
	}

	indexPath := dh.GetIndexPath(0)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Errorf("index file not created at %s", indexPath)
	}

	if err := dh.CloseIndexFiles(); err != nil {
		t.Fatalf("failed to close index files: %v", err)
	}
}

func TestFindOffsetPosition(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer dh.Close()

	if err := dh.OpenIndexFiles(); err != nil {
		t.Fatalf("failed to open index files: %v", err)
	}

	testEntries := []struct {
		offset   uint64
		position uint64
	}{
		{0, 100},
		{10, 200},
		{20, 300},
	}

	for _, entry := range testEntries {
		indexEntry := types.IndexEntry{
			Position: entry.position,
			Offset:   entry.offset,
		}

		if err := binary.Write(dh.GetIndexFile(), binary.BigEndian, indexEntry); err != nil {
			t.Fatalf("failed to write index entry: %v", err)
		}
	}

	if err := dh.GetIndexFile().Sync(); err != nil {
		t.Fatalf("failed to sync index file: %v", err)
	}

	pos, err := dh.FindOffsetPosition(10)
	if err != nil {
		t.Fatalf("failed to find offset 10: %v", err)
	}
	if pos != 200 {
		t.Errorf("expected position 200 for offset 10, got %d", pos)
	}

	pos, err = dh.FindOffsetPosition(15)
	if err != nil {
		t.Fatalf("failed to find offset 15: %v", err)
	}
	if pos != 200 {
		t.Errorf("expected position 200 for offset 15 (closest previous), got %d", pos)
	}

	_, err = dh.FindOffsetPosition(5)
	if err != nil {
		t.Errorf("unexpected error for offset 5: %v", err)
	}
}

func TestIndexFileErrorHandling(t *testing.T) {
	dh := setupDiskHandlerWithIndex(t)
	defer dh.Close()

	originalBaseName := dh.BaseName
	dh.BaseName = "/invalid/path/that/does/not/exist"

	err := dh.OpenIndexFiles()
	if err == nil {
		t.Errorf("expected error when opening index files in invalid path")
	}

	dh.BaseName = originalBaseName
}
