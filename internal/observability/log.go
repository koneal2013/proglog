package observability

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggingSystem interface {
	Sugar() *zap.SugaredLogger
	Named(string) *zap.Logger
	WithOptions(...zap.Option) *zap.Logger
	With(...zap.Field) *zap.Logger
	Check(zapcore.Level, string) *zapcore.CheckedEntry
	Debug(string, ...zap.Field)
	Info(string, ...zap.Field)
	Warn(string, ...zap.Field)
	Error(string, ...zap.Field)
	DPanic(string, ...zap.Field)
	Panic(string, ...zap.Field)
	Fatal(string, ...zap.Field)
	Sync() error
	Core() zapcore.Core
}

func NewLogger(isDevelopment bool, serviceName string, options ...zap.Option) (LoggingSystem, error) {
	if isDevelopment {
		if logger, err := zap.NewDevelopment(options...); err != nil {
			return nil, err
		} else {
			return logger.Named(serviceName), nil
		}
	} else {
		if logger, err := zap.NewProduction(options...); err != nil {
			return nil, err
		} else {
			return logger.Named(serviceName), nil
		}
	}
}
