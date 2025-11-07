package server

import (
	"bytes"
	"compress/gzip"
	"io"
)

// CompressMessage compresses a message if enabled
func CompressMessage(msg []byte, enable bool) ([]byte, error) {
	if !enable {
		return msg, nil
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(msg); err != nil {
		return nil, err
	}
	gz.Close()
	return buf.Bytes(), nil
}

// DecompressMessage decompresses a message if enabled
func DecompressMessage(msg []byte, enable bool) ([]byte, error) {
	if !enable {
		return msg, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
