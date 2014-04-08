// +build cgo

package slog

import (
	"github.com/cloudflare/golog/logger"
)

type CFLog struct {
	log       *logger.Logger
	logPrefix string
}

func (l *CFLog) Logf(level Level, format string, v ...interface{}) {
	lev := int(level) + 1
	l.log.Printf(logger.Level(lev), l.logPrefix, format, v...)
}
