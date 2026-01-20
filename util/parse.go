package util

import "strconv"

func ParseInt(str string, fallback int) int {
	if v, err := strconv.Atoi(str); err == nil {
		return v
	}
	return fallback
}

func ParseInt64(str string, fallback int64) int64 {
	if v, err := strconv.ParseInt(str, 10, 64); err == nil {
		return v
	}
	return fallback
}

func ParseBool(str string, fallback bool) bool {
	if v, err := strconv.ParseBool(str); err == nil {
		return v
	}
	return fallback
}
