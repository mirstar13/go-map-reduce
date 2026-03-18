package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()

	cfg.EncoderConfig.CallerKey = zapcore.OmitKey

	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}

	return cfg.Build()
}
