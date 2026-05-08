package goframework

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

type Logger struct {
	logger *slog.Logger
}

func newLog() *Logger {
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName, _ = os.Hostname()
	}
	return &Logger{
		logger: otelslog.NewLogger(serviceName),
	}
}

func (l *Logger) Info(ctx context.Context, msg string, args ...any) {
	l.logger.InfoContext(ctx, msg, args...)
}

func (l *Logger) Warn(ctx context.Context, msg string, args ...any) {
	l.logger.WarnContext(ctx, msg, args...)
}

func (l *Logger) Error(ctx context.Context, msg string, args ...any) {
	l.logger.ErrorContext(ctx, msg, args...)
}

func (l *Logger) Debug(ctx context.Context, msg string, args ...any) {
	l.logger.DebugContext(ctx, msg, args...)
}
