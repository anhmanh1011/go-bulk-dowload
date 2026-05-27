package logging

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Level  string
	Format string
}

func Setup(cfg Config) (*slog.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	case "text", "":
		handler = slog.NewTextHandler(os.Stderr, opts)
	default:
		return nil, fmt.Errorf("unknown log format %q", cfg.Format)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info", "":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}
