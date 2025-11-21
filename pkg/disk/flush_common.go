package disk

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"
)

// flushLoop continuously processes write batches and handles segment rotation.
func (d *DiskHandler) flushLoop() {
	batch := make([]string, 0, d.batchSize)
	ticker := time.NewTicker(d.linger)
	defer ticker.Stop()

	var segmentTicker *time.Ticker
	if d.segmentRollTime > 0 {
		segmentTicker = time.NewTicker(d.segmentRollTime)
		defer segmentTicker.Stop()
	}

	for {
		select {
		case msg, ok := <-d.writeCh:
			if !ok {
				continue
			}
			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-func() <-chan time.Time {
			if segmentTicker != nil {
				return segmentTicker.C
			}
			return nil
		}():
			// Time-based segment rotation
			d.mu.Lock()
			d.ioMu.Lock()
			if time.Since(d.segmentCreatedAt) >= d.segmentRollTime {
				if err := d.rotateSegment(); err != nil {
					log.Printf("ERROR: time-based segment rotation failed: %v", err)
				}
			}
			d.ioMu.Unlock()
			d.mu.Unlock()

		case <-d.done:
			// Gracefully drain all pending writes before shutdown
			draining := true
			for draining {
				if len(batch) >= d.batchSize {
					d.writeBatch(batch)
					batch = batch[:0]
					continue
				}
				select {
				case msg, ok := <-d.writeCh:
					if !ok {
						draining = false
						continue
					}
					batch = append(batch, msg)
				default:
					draining = false
				}
			}

			if len(batch) > 0 {
				d.writeBatch(batch)
			}

			if d.file != nil {
				if err := d.writer.Flush(); err != nil {
					log.Printf("ERROR: flush failed in shutdown: %v", err)
				}
				if err := d.file.Sync(); err != nil {
					log.Printf("ERROR: sync failed during shutdown: %v", err)
				}
				d.file.Close()
			}
			return
		}
	}
}

// writeBatch writes a batch of messages into the current segment file.
func (d *DiskHandler) writeBatch(batch []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			log.Printf("FATAL: failed to open segment: %v", err)
			return
		}
	}

	var lenBuf [4]byte

	for _, msg := range batch {
		data := []byte(msg)
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

		totalLen := 4 + len(data)
		if d.CurrentOffset+totalLen > d.SegmentSize {
			if err := d.rotateSegment(); err != nil {
				log.Printf("ERROR: rotateSegment failed to rotate segment: %v", err)
				break
			}
		}

		if _, err := d.writer.Write(lenBuf[:]); err != nil {
			log.Printf("ERROR: writeBatch failed writing length: %v", err)
			break
		}
		if _, err := d.writer.Write(data); err != nil {
			log.Printf("ERROR: writeBatch failed writing data: %v", err)
			break
		}

		d.CurrentOffset += totalLen
		d.AbsoluteOffset++
	}

	if err := d.writer.Flush(); err != nil {
		log.Printf("ERROR: flush failed after batch: %v", err)
		return
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: sync failed after batch: %v", err)
		}
	}
}

// WriteDirect writes a single message immediately without batching.
func (d *DiskHandler) WriteDirect(msg string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	var lenBuf [4]byte

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			log.Printf("FATAL: failed to open segment: %v", err)
			return
		}
	}

	data := []byte(msg)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	totalLen := 4 + len(data)

	if d.CurrentOffset+totalLen > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			log.Printf("FATAL: rotateSegment failed: %v", err)
			return
		}
	}

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		log.Printf("ERROR: writeDirect failed writing length: %v", err)
		return
	}
	if _, err := d.writer.Write(data); err != nil {
		log.Printf("ERROR: writeDirect failed writing data: %v", err)
		return
	}

	d.CurrentOffset += totalLen
	d.AbsoluteOffset++

	if err := d.writer.Flush(); err != nil {
		log.Printf("ERROR: flush failed in WriteDirect: %v", err)
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: failed to sync disk file: %v", err)
		}
	}
}

// rotateSegment closes the current segment and opens a new one.
func (d *DiskHandler) rotateSegment() error {
	var errs []error

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			log.Printf("ERROR: flush failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	if d.file != nil {
		if err := d.file.Close(); err != nil {
			log.Printf("ERROR: close failed during rotation: %v", err)
			errs = append(errs, err)
		}
	}

	d.CurrentSegment++
	d.CurrentOffset = 0
	d.segmentCreatedAt = time.Now()

	if err := d.openSegment(); err != nil {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("rotateSegment errors: %v", errs)
	}
	return nil
}

// Flush forces all pending data to be written and synced to disk.
func (d *DiskHandler) Flush() {
	batch := make([]string, 0, len(d.writeCh))

	for {
		select {
		case msg := <-d.writeCh:
			batch = append(batch, msg)
		default:
			goto perform_write
		}
	}

perform_write:
	if len(batch) > 0 {
		d.writeBatch(batch)
		return
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			log.Printf("ERROR: flush failed in Flush: %v", err)
		}
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: failed to sync disk file: %v", err)
		}
	}
}
