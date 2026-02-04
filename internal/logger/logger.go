package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

type Logger struct {
	logger   *log.Logger
	minLevel Level
}

func New(minLevel Level) *Logger {
	return NewWithWriter(os.Stdout, minLevel)
}

func NewWithWriter(w io.Writer, minLevel Level) *Logger {
	return &Logger{
		logger:   log.New(w, "", log.LstdFlags),
		minLevel: minLevel,
	}
}

func NewFileLogger(filePath string, minLevel Level) (*Logger, error) {
	dirPath := filepath.Dir(filePath)
	err := os.Mkdir(dirPath, 0755)
	if err != nil {
		return nil, fmt.Errorf("Failed to create logger: %w", err)
	}

	logFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("Failed to create logger: %w", err)
	}

	return NewWithWriter(logFile, minLevel), nil
}

func (l *Logger) Fatal(msg string, args ...any) {
	l.logger.Fatalf("[FATAL] "+msg, args...)
}

func (l *Logger) Debug(msg string, args ...any) {
	if l.minLevel <= DEBUG {
		l.logger.Printf("[DEBUG] "+msg, args...)
	}
}

func (l *Logger) Info(msg string, args ...any) {
	if l.minLevel <= INFO {
		l.logger.Printf("[INFO] "+msg, args...)
	}
}

func (l *Logger) Warn(msg string, args ...any) {
	if l.minLevel <= WARN {
		l.logger.Printf("[WARN] "+msg, args...)
	}
}

func (l *Logger) Error(msg string, args ...any) {
	if l.minLevel <= ERROR {
		l.logger.Printf("[ERROR] "+msg, args...)
	}
}

func (l *Logger) SetLevel(level Level) {
	l.minLevel = level
}
