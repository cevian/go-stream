package slog

import (
	"fmt"
	stdlogger "log"
	"os"
)

var (
	defaultLog abstractLog // the main logger object
)

const (
	DEFAULT_STATS_LOG_NAME   = "test"
	DEFAULT_STATS_LOG_LEVEL  = "debug"
	DEFAULT_STATS_LOG_PREFIX = "test"
)

type Level int

const (
	Panic Level = iota
	Error
	Warn
	Info
	Debug
)

type abstractLog interface {
	Logf(level Level, format string, v ...interface{})
}

type StdLogger struct {
	log *stdlogger.Logger
}

func (l *StdLogger) Logf(level Level, format string, v ...interface{}) {
	if level >= Warn {
		l.log.Output(4, fmt.Sprintf(format, v...))
	} else if level == Error {
		l.log.Output(4, fmt.Sprintf(format, v...))
	} else if level == Panic {
		l.log.Output(4, fmt.Sprintf(format, v...))
	}
}

func Logf(level Level, format string, v ...interface{}) {
	if defaultLog != nil {
		defaultLog.Logf(level, format, v...)
	}
}

func Errorf(format string, v ...interface{}) { Logf(Error, format, v...) }
func Warnf(format string, v ...interface{})  { Logf(Warn, format, v...) }
func Infof(format string, v ...interface{})  { Logf(Info, format, v...) }
func Debugf(format string, v ...interface{}) { Logf(Debug, format, v...) }

func Fatalf(format string, v ...interface{}) {
	Logf(Panic, format, v...)

	os.Exit(1)
}

func logSetupFailure(format string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", v...)
	os.Exit(1)
}

func init() {
	InitSimpleLogger() //default logger
}

func InitSimpleLogger() {
	defaultLog = &StdLogger{stdlogger.New(os.Stderr, "", stdlogger.Flags())}
}
