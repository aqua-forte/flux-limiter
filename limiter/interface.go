package limiter

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLimitExceeded is returned or signaled when a rate limit is exceeded.
	ErrLimitExceeded = errors.New("rate limit exceeded")
	// ErrLimiterClosed is returned when operations are attempted on a closed limiter.
	ErrLimiterClosed = errors.New("limiter is closed")
)

// Result represents the outcome of a rate limit evaluation with HTTP header metadata.
type Result struct {
	Allowed   bool          // Allowed indicates if the request passed the rate limit check
	Remaining int64         // Remaining number of tokens / requests available
	Reset     time.Duration // Duration until the bucket refills or current window resets
	Limit     int64         // Maximum configured capacity / burst limit
}

// Limiter defines the universal contract for traffic rate limiting.
type Limiter interface {
	// Allow checks whether a single event is permitted for the specified key.
	Allow(ctx context.Context, key string) (bool, error)

	// AllowN checks whether n events are permitted for the specified key.
	AllowN(ctx context.Context, key string, n int64) (bool, error)

	// AllowDetailed returns complete rate-limiting decision and telemetry headers.
	AllowDetailed(ctx context.Context, key string) (Result, error)

	// Close gracefully terminates background routines and connections.
	Close() error
}
