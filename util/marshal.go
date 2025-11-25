package util

import (
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// UnmarshalYAML implements custom YAML unmarshaling for LogLevel
func (l *LogLevel) UnmarshalYAML(value *yaml.Node) error {

	var s string
	if err := value.Decode(&s); err == nil {
		switch strings.ToLower(s) {
		case "debug":
			*l = LogLevelDebug
		case "info":
			*l = LogLevelInfo
		case "warn", "warning":
			*l = LogLevelWarn
		case "error":
			*l = LogLevelError
		default:
			*l = LogLevelInfo
		}
		return nil
	}

	var i int
	if err := value.Decode(&i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	*l = LogLevel(i)
	return nil
}

// UnmarshalJSON implements custom JSON unmarshaling for LogLevel
func (l *LogLevel) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		switch strings.ToLower(s) {
		case "debug":
			*l = LogLevelDebug
		case "info":
			*l = LogLevelInfo
		case "warn", "warning":
			*l = LogLevelWarn
		case "error":
			*l = LogLevelError
		default:
			*l = LogLevelInfo
		}
		return nil
	}

	var i int
	if err := json.Unmarshal(data, &i); err != nil {
		return fmt.Errorf("log_level must be a string (debug/info/warn/error) or integer (0-3)")
	}
	*l = LogLevel(i)
	return nil
}
