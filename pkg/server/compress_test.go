package server_test

import (
	"bytes"
	"go-broker/pkg/server"
	"testing"
)

func TestCompressDecompress(t *testing.T) {
	input := []byte("hello world")
	cmp, err := server.CompressMessage(input, true)
	if err != nil {
		t.Fatal(err)
	}

	out, err := server.DecompressMessage(cmp, true)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(input, out) {
		t.Errorf("Expected %s, got %s", input, out)
	}
}
