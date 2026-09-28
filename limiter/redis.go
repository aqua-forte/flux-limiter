package limiter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// Algorithm specifies the rate limiting algorithm used with Redis.
type Algorithm string

const (
	// AlgorithmFixedWindow uses atomic INCR + EXPIRE over discrete time buckets.
	AlgorithmFixedWindow Algorithm = "fixed_window"
	// AlgorithmSlidingWindow uses Redis Sorted Sets (ZSET) for smooth rolling limits.
	AlgorithmSlidingWindow Algorithm = "sliding_window"
)

const (
	defaultRedisPrefix   = "flux:"
	defaultRedisLimit    = int64(100)
	defaultRedisWindow   = 1 * time.Second
	defaultFailOpenState = false
)

// RedisLimiterOption defines functional configuration options for RedisLimiter.
type RedisLimiterOption func(*RedisLimiter)

// WithPrefix sets the Redis key namespace.
func WithPrefix(prefix string) RedisLimiterOption {
	return func(r *RedisLimiter) {
		if prefix != "" {
			r.prefix = prefix
		}
	}
}

// WithAlgorithm selects between Fixed Window or Sliding Window counter.
func WithAlgorithm(algo Algorithm) RedisLimiterOption {
	return func(r *RedisLimiter) {
		r.algorithm = algo
	}
}

// WithFailOpen configures whether Redis errors allow requests to pass through.
func WithFailOpen(failOpen bool) RedisLimiterOption {
	return func(r *RedisLimiter) {
		r.failOpen = failOpen
	}
}

// RedisLimiter implements Limiter backed by Redis for distributed deployments.
type RedisLimiter struct {
	client    redis.UniversalClient
	limit     int64
	window    time.Duration
	prefix    string
	algorithm Algorithm
	failOpen  bool

	fixedWindowScript   *redis.Script
	slidingWindowScript *redis.Script
}

// Lua script for atomic Fixed Window (INCRBY + EXPIRE).
const fixedWindowLua = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])

local current = redis.call('INCRBY', key, cost)
if current == cost then
    redis.call('EXPIRE', key, window)
end

local ttl = redis.call('TTL', key)
if ttl < 0 then
    redis.call('EXPIRE', key, window)
    ttl = window
end

if current <= limit then
    return {1, limit - current, ttl}
else
    return {0, 0, ttl}
end
`

// Lua script for rolling Sliding Window via ZSET.
const slidingWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local randSeed = ARGV[5]

local clearBefore = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', clearBefore)

local currentCount = redis.call('ZCARD', key)
if currentCount + cost <= limit then
    for i = 1, cost do
        local member = string.format("%d-%s-%d", now, randSeed, i)
        redis.call('ZADD', key, now, member)
    end
    redis.call('EXPIRE', key, math.ceil(window / 1000) + 1)
    return {1, limit - (currentCount + cost), math.ceil(window / 1000)}
else
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local resetMs = window
    if #oldest > 0 then
        local oldestScore = tonumber(oldest[2])
        resetMs = math.max(0, (oldestScore + window) - now)
    end
    return {0, 0, math.ceil(resetMs / 1000)}
end
`

// NewRedisLimiter initializes a distributed Redis rate limiter.
func NewRedisLimiter(client redis.UniversalClient, limit int64, window time.Duration, opts ...RedisLimiterOption) *RedisLimiter {
	if limit <= 0 {
		limit = defaultRedisLimit
	}
	if window <= 0 {
		window = defaultRedisWindow
	}

	r := &RedisLimiter{
		client:              client,
		limit:               limit,
		window:              window,
		prefix:              defaultRedisPrefix,
		algorithm:           AlgorithmFixedWindow,
		failOpen:            defaultFailOpenState,
		fixedWindowScript:   redis.NewScript(fixedWindowLua),
		slidingWindowScript: redis.NewScript(slidingWindowLua),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Allow evaluates if a single request passes the distributed rate limit.
func (r *RedisLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return r.AllowN(ctx, key, 1)
}

// AllowN evaluates if n requests pass the distributed rate limit.
func (r *RedisLimiter) AllowN(ctx context.Context, key string, n int64) (bool, error) {
	res, err := r.allowInternal(ctx, key, n)
	if err != nil {
		if r.failOpen {
			return true, nil
		}
		return false, err
	}
	return res.Allowed, nil
}

// AllowDetailed returns complete rate limit telemetry headers.
func (r *RedisLimiter) AllowDetailed(ctx context.Context, key string) (Result, error) {
	res, err := r.allowInternal(ctx, key, 1)
	if err != nil {
		if r.failOpen {
			return Result{Allowed: true, Remaining: 1, Reset: r.window, Limit: r.limit}, nil
		}
		return Result{}, err
	}
	return res, nil
}

func (r *RedisLimiter) allowInternal(ctx context.Context, key string, n int64) (Result, error) {
	if r.client == nil {
		return Result{}, errors.New("redis client is nil")
	}

	fullKey := fmt.Sprintf("%s%s", r.prefix, key)

	if r.algorithm == AlgorithmSlidingWindow {
		nowMs := time.Now().UnixMilli()
		windowMs := r.window.Milliseconds()
		randSeed := fmt.Sprintf("%d", nowMs%1000000)

		res, err := r.slidingWindowScript.Run(ctx, r.client, []string{fullKey}, nowMs, windowMs, r.limit, n, randSeed).Result()
		if err != nil {
			return Result{}, fmt.Errorf("redis sliding window script failed: %w", err)
		}
		return parseScriptResult(res, r.limit)
	}

	// Fixed Window algorithm
	windowSec := int64(math.Ceil(r.window.Seconds()))
	if windowSec < 1 {
		windowSec = 1
	}

	res, err := r.fixedWindowScript.Run(ctx, r.client, []string{fullKey}, r.limit, windowSec, n).Result()
	if err != nil {
		return Result{}, fmt.Errorf("redis fixed window script failed: %w", err)
	}

	return parseScriptResult(res, r.limit)
}

func parseScriptResult(res interface{}, limit int64) (Result, error) {
	slice, ok := res.([]interface{})
	if !ok || len(slice) < 3 {
		return Result{}, fmt.Errorf("unexpected script response type: %T", res)
	}

	allowedInt, ok1 := slice[0].(int64)
	remainingInt, ok2 := slice[1].(int64)
	resetSec, ok3 := slice[2].(int64)

	if !ok1 || !ok2 || !ok3 {
		return Result{}, fmt.Errorf("malformed script result elements: %v", slice)
	}

	return Result{
		Allowed:   allowedInt == 1,
		Remaining: remainingInt,
		Reset:     time.Duration(resetSec) * time.Second,
		Limit:     limit,
	}, nil
}

// Close gracefully closes the Redis client connection.
func (r *RedisLimiter) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}
