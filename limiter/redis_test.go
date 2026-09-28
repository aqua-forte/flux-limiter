package limiter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})
	t.Cleanup(func() {
		_ = client.Close()
	})
	return mr, client
}

func TestRedisLimiter_FixedWindow(t *testing.T) {
	mr, client := setupTestRedis(t)
	ctx := context.Background()

	// 3 requests allowed per 1-second window
	lim := NewRedisLimiter(
		client,
		3,
		1*time.Second,
		WithAlgorithm(AlgorithmFixedWindow),
		WithPrefix("test:fixed:"),
	)
	defer lim.Close()

	key := "user-123"

	// 1st request -> allowed (remaining: 2)
	res, err := lim.AllowDetailed(ctx, key)
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(2), res.Remaining)

	// 2nd request -> allowed (remaining: 1)
	res, err = lim.AllowDetailed(ctx, key)
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(1), res.Remaining)

	// 3rd request -> allowed (remaining: 0)
	res, err = lim.AllowDetailed(ctx, key)
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(0), res.Remaining)

	// 4th request -> blocked!
	res, err = lim.AllowDetailed(ctx, key)
	require.NoError(t, err)
	assert.False(t, res.Allowed)
	assert.Equal(t, int64(0), res.Remaining)

	// Allow method should also return false
	allowed, err := lim.Allow(ctx, key)
	require.NoError(t, err)
	assert.False(t, allowed)

	// Fast-forward miniredis time past the 1-second window
	mr.FastForward(2 * time.Second)

	// Window has reset -> request should be allowed again
	res, err = lim.AllowDetailed(ctx, key)
	require.NoError(t, err)
	assert.True(t, res.Allowed)
	assert.Equal(t, int64(2), res.Remaining)
}

func TestRedisLimiter_SlidingWindow(t *testing.T) {
	mr, client := setupTestRedis(t)
	ctx := context.Background()

	// 2 requests allowed per 1000ms window
	lim := NewRedisLimiter(
		client,
		2,
		1*time.Second,
		WithAlgorithm(AlgorithmSlidingWindow),
		WithPrefix("test:sliding:"),
	)
	defer lim.Close()

	key := "user-456"

	// Request 1
	allowed, err := lim.Allow(ctx, key)
	require.NoError(t, err)
	assert.True(t, allowed)

	// Request 2
	allowed, err = lim.Allow(ctx, key)
	require.NoError(t, err)
	assert.True(t, allowed)

	// Request 3 -> Denied
	allowed, err = lim.Allow(ctx, key)
	require.NoError(t, err)
	assert.False(t, allowed)

	// Fast-forward past window
	mr.FastForward(2 * time.Second)

	// Should be allowed again
	allowed, err = lim.Allow(ctx, key)
	require.NoError(t, err)
	assert.True(t, allowed)
}

func TestRedisLimiter_FailOpenOption(t *testing.T) {
	// Create client with non-existent server
	deadClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:54321", // non-existent
	})
	defer deadClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Fail closed (default)
	limClosed := NewRedisLimiter(deadClient, 10, time.Second, WithFailOpen(false))
	defer limClosed.Close()

	_, err := limClosed.Allow(ctx, "user-err")
	assert.Error(t, err, "Expected error when Redis is down and FailOpen is false")

	// Fail open
	limOpen := NewRedisLimiter(deadClient, 10, time.Second, WithFailOpen(true))
	defer limOpen.Close()

	allowed, err := limOpen.Allow(ctx, "user-err")
	assert.NoError(t, err, "Should not return error when FailOpen is true")
	assert.True(t, allowed, "Should allow request when FailOpen is true")
}
