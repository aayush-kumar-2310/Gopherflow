package ctxutil

import (
	"context"
	"time"

	"Shared/config"
)

// WithRedisTimeout wraps parent with a Redis operation deadline.
func WithRedisTimeout(parent context.Context, cfg config.Config) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, cfg.RedisTimeout)
}

// WithDBTimeout wraps parent with a database operation deadline.
func WithDBTimeout(parent context.Context, cfg config.Config) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, cfg.DBTimeout)
}

// JobContext returns a context for async pool work that outlives a short Redis poll tick
// but still honors process shutdown via parent.
func JobContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(parent), timeout)
}

// Sleep respects cancellation; returns false if ctx was cancelled.
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
