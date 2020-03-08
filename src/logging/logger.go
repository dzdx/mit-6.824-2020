package logging

import (
	"fmt"
	"time"
)

type logLevel int

const (
	ErrorLevel logLevel = iota
	WarnLevel
	InfoLevel
	DebugLevel
	TraceLevel
)

func (l logLevel) String() string {
	switch l {
	case ErrorLevel:
		return "error"
	case WarnLevel:
		return "warn"
	case InfoLevel:
		return "info"
	case DebugLevel:
		return "debug"
	case TraceLevel:
		return "trace"
	}
	return "unknown"
}

func (l logLevel) color() string {
	switch l {
	case ErrorLevel:
		return "\033[1;31m%s\033[0m"
	case WarnLevel:
		return "\033[1;33m%s\033[0m"
	case InfoLevel:
		return "\033[1;34m%s\033[0m"
	case DebugLevel:
		return "\033[0;36m%s\033[0m"
	case TraceLevel:
		return "\033[0;37m%s\033[0m"
	}
	return "%s"
}
func NewLogger(level logLevel, colorful bool, fieldsF func() string) *Logger {
	return &Logger{level: level, colorful: colorful, fieldsF: fieldsF}
}

type Logger struct {
	level    logLevel
	colorful bool
	fieldsF  func() string
}

func (l *Logger) logging(level logLevel, format string, a ...interface{}) {
	if level <= l.level {
		fields := l.fieldsF()
		colorFormat := "%s\n"
		if l.colorful {
			colorFormat = level.color() + "\n"
		}
		prefix := fmt.Sprintf("%s [%5s] ", time.Now().Format("15:04:05.000000"), level)
		split := ": "
		fmt.Printf(colorFormat, prefix+fields+split+fmt.Sprintf(format, a...))
	}
}
func (l *Logger) Errorf(format string, a ...interface{}) {
	l.logging(ErrorLevel, format, a...)
}
func (l *Logger) Warnf(format string, a ...interface{}) {
	l.logging(WarnLevel, format, a...)
}
func (l *Logger) Infof(format string, a ...interface{}) {
	l.logging(InfoLevel, format, a...)
}
func (l *Logger) Debugf(format string, a ...interface{}) {
	l.logging(DebugLevel, format, a...)
}
func (l *Logger) Tracef(format string, a ...interface{}) {
	l.logging(TraceLevel, format, a...)
}
