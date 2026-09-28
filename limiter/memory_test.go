package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryLimiter_TokenBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("burst and exhaustion", func(t *testing.T) {
		// 10 tokens/sec, burst of 5
		lim := NewMemoryLimiter(10, 5)
		defer lim.Close()

		key := "test-burst-user"

		// First 5 requests must succeed immediately
		for i := 0; i < 5; i++ {
			allowed, err := lim.Allow(ctx, key)
			require.NoError(t, err)
			assert.True(t, allowed, "Request %d should be allowed within burst capacity", i+1)
		}

		// 6th request must be rejected
		allowed, err := lim.Allow(ctx, key)
		require.NoError(t, err)
		assert.False(t, allowed, "6th request must be rejected when burst is depleted")

		// Verify detailed metadata on rejected request
		res, err := lim.AllowDetailed(ctx, key)
		require.NoError(t, err)
		assert.False(t, res.Allowed)
		assert.Equal(t, int64(0), res.Remaining)
		assert.Positive(t, res.Reset)
	})

	t.Run("table driven token replenishment", func(t *testing.T) {
		mockTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		var mu sync.Mutex
		now := func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return mockTime
		}
		advanceTime := func(d time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			mockTime = mockTime.Add(d)
		}

		// Rate: 2 tokens/sec, Burst: 3
		lim := NewMemoryLimiter(2, 3, WithClock(now))
		defer lim.Close()

		key := "replenish-client"

		tests := []struct {
			name            string
			advanceBy       time.Duration
			expectedAllowed bool
			expectedRemain  int64
		}{
			{"initial request 1", 0, true, 2},
			{"initial request 2", 0, true, 1},
			{"initial request 3 (exhaust burst)", 0, true, 0},
			{"request 4 without time advance (rejected)", 0, false, 0},
			{"advance 500ms (1 token refilled)", 500 * time.Millisecond, true, 0},
			{"immediate request without advance (rejected)", 0, false, 0},
			{"advance 1.5s (3 tokens refilled, capped at burst 3)", 1500 * time.Millisecond, true, 2},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.advanceBy > 0 {
					advanceTime(tc.advanceBy)
				}
				res, err := lim.AllowDetailed(ctx, key)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedAllowed, res.Allowed)
				assert.Equal(t, tc.expectedRemain, res.Remaining)
			})
		}
	})

	t.Run("independent client keys", func(t *testing.T) {
		lim := NewMemoryLimiter(5, 2)
		defer lim.Close()

		key1 := "client-alpha"
		key2 := "client-beta"

		// Deplete client-alpha
		allow1, _ := lim.Allow(ctx, key1)
		allow2, _ := lim.Allow(ctx, key1)
		allow3, _ := lim.Allow(ctx, key1)
		assert.True(t, allow1)
		assert.True(t, allow2)
		assert.False(t, allow3)

		// client-beta must still have its full quota
		bAllow1, _ := lim.Allow(ctx, key2)
		bAllow2, _ := lim.Allow(ctx, key2)
		assert.True(t, bAllow1)
		assert.True(t, bAllow2)
	})

	t.Run("context cancellation", func(t *testing.T) {
		lim := NewMemoryLimiter(10, 5)
		defer lim.Close()

		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := lim.Allow(cancelCtx, "some-key")
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("limiter closed error", func(t *testing.T) {
		lim := NewMemoryLimiter(10, 5)
		require.NoError(t, lim.Close())

		_, err := lim.Allow(ctx, "some-key")
		assert.ErrorIs(t, err, ErrLimiterClosed)
	})
}

func TestMemoryLimiter_GracefulCleanup(t *testing.T) {
	mockTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	now := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return mockTime
	}

	lim := NewMemoryLimiter(
		10,
		10,
		WithClock(now),
		WithTTL(50*time.Millisecond),
		WithCleanupInterval(10*time.Millisecond),
	)
	defer lim.Close()

	ctx := context.Background()

	// Create 10 keys
	for i := 0; i < 10; i++ {
		_, err := lim.Allow(ctx, fmt.Sprintf("temp-key-%d", i))
		require.NoError(t, err)
	}

	assert.Equal(t, 10, lim.KeyCount())

	// Advance clock past TTL
	mu.Lock()
	mockTime = mockTime.Add(200 * time.Millisecond)
	mu.Unlock()

	// Trigger pruning manually
	lim.pruneExpired(mockTime)

	assert.Equal(t, 0, lim.KeyCount(), "All expired keys should be pruned")
}

func BenchmarkMemoryLimiter(b *testing.B) {
	lim := NewMemoryLimiter(1_000_000, 1_000_000)
	defer lim.Close()

	ctx := context.Background()
	var keyCounter int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		id := atomic.AddInt64(&keyCounter, 1)
		key := fmt.Sprintf("bench-user-%d", id%100)

		for pb.Next() {
			_, _ = lim.Allow(ctx, key)
		}
	})
}
