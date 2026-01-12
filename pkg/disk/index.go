package disk

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/downfa11-org/cursus/pkg/types"
	"github.com/downfa11-org/cursus/util"
	"golang.org/x/exp/mmap"
)

func (d *DiskHandler) openIndexFiles() error {
	indexPath := d.GetIndexPath(d.CurrentSegment)
	f, err := os.OpenFile(indexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		if os.IsPermission(err) {
			util.Debug("Index file %s is read-only, opening in read-only mode", indexPath)
			f, err = os.Open(indexPath)
			if err != nil {
				return fmt.Errorf("failed to open read-only index: %w", err)
			}
		} else {
			return fmt.Errorf("failed to open index file: %w", err)
		}
	}

	var writer *bufio.Writer
	info, err := f.Stat()
	if err == nil && info.Mode().Perm()&0200 != 0 {
		writer = bufio.NewWriter(f)
	}

	d.indexMu.Lock()
	d.indexFile = f
	d.indexWriter = writer
	err = d.refreshIndexMapper(indexPath)
	d.indexMu.Unlock()

	return err
}

func (d *DiskHandler) refreshIndexMapper(path string) error {
	if d.indexMapper != nil {
		d.indexMapper.Close()
	}
	mapper, err := mmap.Open(path)
	if err != nil {
		return err
	}
	d.indexMapper = mapper
	return nil
}

func (d *DiskHandler) closeIndexFiles() error {
	var errs []error

	if d.indexWriter != nil {
		if err := d.indexWriter.Flush(); err != nil {
			return fmt.Errorf("flush index writer failed: %w", err)
		}
	}

	if d.indexMapper != nil {
		if err := d.indexMapper.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexMapper = nil
	}

	if d.indexFile != nil {
		if err := d.indexFile.Close(); err != nil {
			errs = append(errs, err)
		}
		d.indexFile = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("closeIndexFiles errors: %v", errs)
	}
	return nil
}

func getLastOffsetFromIndex(indexPath string, baseOffset uint64) (lastOffset uint64, count int, err error) {
	const entrySize = types.IndexEntrySize
	info, err := os.Stat(indexPath)

	if err != nil || info.Size() < entrySize {
		return 0, 0, fmt.Errorf("index too small or not found")
	}

	f, err := os.Open(indexPath)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	buf := make([]byte, entrySize)
	_, err = f.ReadAt(buf, info.Size()-entrySize)
	if err != nil {
		return 0, 0, err
	}

	lastOffset = binary.BigEndian.Uint64(buf[0:8])
	count = int(lastOffset - baseOffset + 1)
	return lastOffset, count, nil
}

// findSegmentForOffset finds which segment contains the given offset
func (dh *DiskHandler) findSegmentForOffset(offset uint64) (string, uint64, error) {
	dh.mu.Lock()
	currSeg := dh.CurrentSegment
	currPath := dh.GetSegmentPath(currSeg)
	dh.mu.Unlock()

	if offset >= currSeg {
		return currPath, currSeg, nil
	}

	pattern := dh.BaseName + "_segment_*.log"
	files, _ := filepath.Glob(pattern)
	if len(files) == 0 {
		return "", 0, fmt.Errorf("no segments found")
	}

	sort.Strings(files)
	for i := len(files) - 1; i >= 0; i-- {
		base := filepath.Base(files[i])
		parts := strings.Split(strings.TrimSuffix(base, ".log"), "_segment_")
		if len(parts) < 2 {
			continue
		}

		baseOffset, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}

		if offset >= baseOffset {
			return files[i], baseOffset, nil
		}
	}

	return "", 0, fmt.Errorf("offset %d not found in any segment", offset)
}

func (d *DiskHandler) FindOffsetPosition(offset uint64) (uint64, error) {
	if d.indexFile == nil {
		return 0, nil
	}

	stat, err := d.indexFile.Stat()
	if err != nil {
		return 0, fmt.Errorf("index stat failed: %w", err)
	}

	currentFileSize := stat.Size()
	if d.indexMapper == nil || int64(d.indexMapper.Len()) < currentFileSize {
		indexPath := d.GetIndexPath(d.CurrentSegment)
		if err := d.refreshIndexMapper(indexPath); err != nil {
			return 0, fmt.Errorf("failed to refresh index mapper: %w", err)
		}
	}

	if d.indexMapper == nil {
		return 0, fmt.Errorf("index not available")
	}

	const entrySize = types.IndexEntrySize
	if d.indexMapper.Len()%entrySize != 0 {
		return 0, fmt.Errorf("index corruption: size %d is not multiple of %d", d.indexMapper.Len(), entrySize)
	}

	length := d.indexMapper.Len()
	entryCount := length / entrySize
	if entryCount == 0 {
		return 0, nil
	}

	low, high := 0, entryCount-1
	var lastFoundPos uint64 = 0
	buf := make([]byte, entrySize)

	for low <= high {
		mid := (low + high) / 2
		_, err := d.indexMapper.ReadAt(buf, int64(mid*entrySize))
		if err != nil {
			break
		}

		eOffset := binary.BigEndian.Uint64(buf[0:8])
		ePosition := binary.BigEndian.Uint64(buf[8:16])

		if eOffset == offset {
			return ePosition, nil
		} else if eOffset < offset {
			lastFoundPos = ePosition
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return lastFoundPos, nil
}
