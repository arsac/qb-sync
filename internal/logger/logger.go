package logger

import (
	"log/slog"
	"os"
	"strings"
)

// ParseLevel converts a level string to slog.Level.
// Accepts: debug, info, warn, error (case-insensitive). Defaults to info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func New(name string, level slog.Level) *slog.Logger {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})
	return slog.New(handler).With("task", name)
}
