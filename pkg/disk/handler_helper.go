package disk

import (
	"fmt"
	"os"

	"github.com/downfa11-org/cursus/util"
)

func (dh *DiskHandler) GetLatestOffset() (uint64, error) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	return dh.AbsoluteOffset, nil
}

// GetIndexFile returns the current index file
func (d *DiskHandler) GetIndexFile() *os.File {
	return d.indexFile
}

func (d *DiskHandler) GetSegmentPath(baseOffset uint64) string {
	return fmt.Sprintf("%s_segment_%020d.log", d.BaseName, baseOffset)
}

func (d *DiskHandler) GetIndexPath(baseOffset uint64) string {
	return fmt.Sprintf("%s_segment_%020d.index", d.BaseName, baseOffset)
}

// OpenIndexFiles public method for testing
func (d *DiskHandler) OpenIndexFiles() error {
	return d.openIndexFiles()
}

// CloseIndexFiles public method for testing
func (d *DiskHandler) CloseIndexFiles() error {
	return d.closeIndexFiles()
}

// Close signals the flushLoop to terminate and cleans up resources.
func (d *DiskHandler) Close() {
	d.closeOnce.Do(func() {
		close(d.done)
		d.shutdown.Wait()
		if d.writer != nil {
			if err := d.writer.Flush(); err != nil {
				util.Error("flush error on close: %v", err)
			}
		}
		if d.file != nil {
			if err := d.file.Close(); err != nil {
				util.Error("file close error: %v", err)
			}
		}
	})
}
