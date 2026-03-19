package logger

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New builds a *zap.Logger with the given log level, output format, and a
// static "service" field stamped on every log line.
//
//   - level:   "debug" | "info" | "warn" | "error"  (default: "info")
//   - format:  "json" | "console"                    (default: "json")
//   - service: human-readable service name injected as a field
func New(level, format, service string) (*zap.Logger, error) {
	zapLevel, err := parseLevel(level)
	if err != nil {
		return nil, fmt.Errorf("logger: %w", err)
	}

	var cfg zap.Config
	switch strings.ToLower(format) {
	case "console", "text":
		cfg = zap.NewDevelopmentConfig()
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	default:
		cfg = zap.NewProductionConfig()
	}

	cfg.Level = zap.NewAtomicLevelAt(zapLevel)

	// Always emit timestamps in ISO-8601 / RFC3339 so log aggregators parse them.
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	log, err := cfg.Build(
		zap.AddCallerSkip(0),
		// Pre-stamp the service name so every log line carries it without any
		// extra work at call sites.
		zap.Fields(zap.String("service", service)),
	)
	if err != nil {
		return nil, fmt.Errorf("logger: build: %w", err)
	}
	return log, nil
}

// MustNew is like New but panics on error. Useful in main() where there is no
// error-handling path before the logger exists.
func MustNew(level, format, service string) *zap.Logger {
	log, err := New(level, format, service)
	if err != nil {
		panic(err)
	}
	return log
}

// parseLevel converts a human-readable string to a zapcore.Level.
// An empty string defaults to InfoLevel.
func parseLevel(s string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return zapcore.InfoLevel, nil
	case "debug":
		return zapcore.DebugLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("unknown log level %q; valid values: debug, info, warn, error", s)
	}
}
