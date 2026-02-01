package logger

import (
	"io"
	"log"
	"os"
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
