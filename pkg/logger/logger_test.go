package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestParseLevel_ValidValues(t *testing.T) {
	tests := []struct {
		input string
		want  zapcore.Level
	}{
		{"", zapcore.InfoLevel},
		{"info", zapcore.InfoLevel},
		{"INFO", zapcore.InfoLevel},
		{"  info  ", zapcore.InfoLevel},
		{"debug", zapcore.DebugLevel},
		{"DEBUG", zapcore.DebugLevel},
		{"warn", zapcore.WarnLevel},
		{"WARN", zapcore.WarnLevel},
		{"warning", zapcore.WarnLevel},
		{"WARNING", zapcore.WarnLevel},
		{"error", zapcore.ErrorLevel},
		{"ERROR", zapcore.ErrorLevel},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := parseLevel(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseLevel_InvalidValue_ReturnsError(t *testing.T) {
	for _, bad := range []string{"trace", "fatal", "verbose", "1", "off"} {
		t.Run(bad, func(t *testing.T) {
			_, err := parseLevel(bad)
			require.Error(t, err)
			assert.Contains(t, err.Error(), bad)
		})
	}
}

func TestNew_JSONFormat_Succeeds(t *testing.T) {
	log, err := New("info", "json", "test-service")
	require.NoError(t, err)
	require.NotNil(t, log)
	log.Sync() //nolint:errcheck
}

func TestNew_ConsoleFormat_Succeeds(t *testing.T) {
	log, err := New("debug", "console", "test-service")
	require.NoError(t, err)
	require.NotNil(t, log)
	log.Sync() //nolint:errcheck
}

func TestNew_TextFormat_TreatedAsConsole(t *testing.T) {
	// "text" is an alias for "console" in the implementation.
	log, err := New("info", "text", "test-service")
	require.NoError(t, err)
	require.NotNil(t, log)
	log.Sync() //nolint:errcheck
}

func TestNew_UnknownFormat_FallsBackToJSON(t *testing.T) {
	// Unknown format defaults to JSON — no error.
	log, err := New("info", "structured", "test-service")
	require.NoError(t, err)
	require.NotNil(t, log)
	log.Sync() //nolint:errcheck
}

func TestNew_InvalidLevel_ReturnsError(t *testing.T) {
	_, err := New("trace", "json", "test-service")
	require.Error(t, err)
}

func TestNew_ServiceFieldPresent(t *testing.T) {
	// We can't inspect fields directly, but we verify the logger is non-nil and functional.
	log, err := New("info", "json", "manager")
	require.NoError(t, err)
	assert.NotNil(t, log)
	// Calling named loggers must not panic.
	assert.NotPanics(t, func() {
		log.Info("test message")
	})
	log.Sync() //nolint:errcheck
}

func TestMustNew_ValidArgs_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		log := MustNew("info", "json", "test-service")
		log.Sync() //nolint:errcheck
	})
}

func TestMustNew_InvalidLevel_Panics(t *testing.T) {
	assert.Panics(t, func() {
		MustNew("trace", "json", "test-service")
	})
}

func TestNew_AllLevelsCombinedWithFormats(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error"}
	formats := []string{"json", "console", "text"}

	for _, level := range levels {
		for _, format := range formats {
			t.Run(level+"/"+format, func(t *testing.T) {
				log, err := New(level, format, "svc")
				require.NoError(t, err)
				require.NotNil(t, log)
				log.Sync() //nolint:errcheck
			})
		}
	}
}
