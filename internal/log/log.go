// Package log is a small leveled logger used across the collectors.
//
// It replaces the logger previously provided by the New Relic Labs SDK and
// keeps the same printf-style call sites. Output is structured JSON on stderr
// so it can be shipped by any log driver (Docker, CloudWatch, etc.).
package log

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

var (
	level  = new(slog.LevelVar)
	logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
)

func init() {
	SetLevel(os.Getenv("LOG_LEVEL"))
}

// SetLevel sets the minimum level: debug, info, warn or error (default info).
func SetLevel(name string) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "debug":
		level.Set(slog.LevelDebug)
	case "warn", "warning":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}
}

func IsDebugEnabled() bool {
	return level.Level() <= slog.LevelDebug
}

func Debugf(format string, args ...any) {
	if IsDebugEnabled() {
		logger.Debug(fmt.Sprintf(format, args...))
	}
}

func Infof(format string, args ...any) {
	logger.Info(fmt.Sprintf(format, args...))
}

func Warnf(format string, args ...any) {
	logger.Warn(fmt.Sprintf(format, args...))
}

func Errorf(format string, args ...any) {
	logger.Error(fmt.Sprintf(format, args...))
}

// With returns a logger carrying structured attributes (e.g. topic, source).
func With(args ...any) *slog.Logger {
	return logger.With(args...)
}

func Fatalf(err error) {
	logger.Error(fmt.Sprintf("FATAL: can't continue: %v", err))
	os.Exit(1)
}
