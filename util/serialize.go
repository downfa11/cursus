package util

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// EncodeMessage serializes topic and payload into bytes.
func EncodeMessage(topic string, payload string) []byte {
	topicBytes := []byte(topic)
	payloadBytes := []byte(payload)
	data := make([]byte, 2+len(topicBytes)+len(payloadBytes))
	binary.BigEndian.PutUint16(data[:2], uint16(len(topicBytes)))
	copy(data[2:2+len(topicBytes)], topicBytes)
	copy(data[2+len(topicBytes):], payloadBytes)
	return data
}

// DecodeMessage deserializes bytes into topic and payload.
func DecodeMessage(data []byte) (string, string) {
	if len(data) < 2 {
		return "", ""
	}
	topicLen := binary.BigEndian.Uint16(data[:2])
	if int(topicLen)+2 > len(data) {
		return "", ""
	}
	topic := string(data[2 : 2+topicLen])
	payload := string(data[2+int(topicLen):])
	return topic, payload
}

// WriteWithLength writes data with a 4-byte length prefix.
func WriteWithLength(conn net.Conn, data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := conn.Write(lenBuf); err != nil {
		return fmt.Errorf("write length: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("write body: %w", err)
	}
	return nil
}

// ReadWithLength reads data with a 4-byte length prefix.
func ReadWithLength(conn net.Conn) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf)
	buf := make([]byte, length)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return buf, nil
}
