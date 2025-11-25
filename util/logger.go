package util

import (
	"log"
	"os"
)

var currentLevel LogLevel = LogLevelInfo

func SetLevel(level LogLevel) {
	currentLevel = level
}

func Debug(format string, v ...interface{}) {
	if currentLevel <= LogLevelDebug {
		log.Printf("[DEBUG] "+format, v...)
	}
}

func Info(format string, v ...interface{}) {
	if currentLevel <= LogLevelInfo {
		log.Printf("[INFO] "+format, v...)
	}
}

func Warn(format string, v ...interface{}) {
	if currentLevel <= LogLevelWarn {
		log.Printf("[WARN] "+format, v...)
	}
}

func Error(format string, v ...interface{}) {
	if currentLevel <= LogLevelError {
		log.Printf(" "+format, v...)
	}
}

func Fatal(format string, v ...interface{}) {
	log.Printf("[FATAL] "+format, v...)
	os.Exit(1)
}
