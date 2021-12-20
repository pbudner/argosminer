package storage

import (
	"strings"

	"go.uber.org/zap"
)

type diskLogger struct {
	logger *zap.SugaredLogger
}

func defaultLogger(logger *zap.SugaredLogger) *diskLogger {
	return &diskLogger{
		logger: logger,
	}
}

func (l *diskLogger) Errorf(format string, v ...interface{}) {
	l.logger.Errorf(strings.Trim(format, "\n"), v...)
}

func (l *diskLogger) Infof(format string, v ...interface{}) {
	l.logger.Infof(strings.Trim(format, "\n"), v...)
}

func (l *diskLogger) Warningf(format string, v ...interface{}) {
	l.logger.Warnf(strings.Trim(format, "\n"), v...)
}

func (l *diskLogger) Debugf(format string, v ...interface{}) {
	l.logger.Debugf(strings.Trim(format, "\n"), v...)
}
