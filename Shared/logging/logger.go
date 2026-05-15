package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Init configures the default slog logger (JSON or text).
func Init(service, level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl, AddSource: lvl == slog.LevelDebug}
	var handler slog.Handler
	if strings.ToLower(format) == "text" {
		handler = slog.NewTextHandler(os.Stdout, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	logger := slog.New(handler).With("service", service)
	slog.SetDefault(logger)
	return logger
}

// WithRun returns a logger with workflow run correlation fields.
func WithRun(logger *slog.Logger, workflowID, runID, stageID string) *slog.Logger {
	return logger.With(
		"workflow_id", workflowID,
		"run_id", runID,
		"stage_id", stageID,
	)
}
