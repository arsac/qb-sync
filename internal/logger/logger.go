package logger

import (
	"log/slog"
	"os"
)

func New(name string) *slog.Logger {
	handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(handler).With("task", name)
}
