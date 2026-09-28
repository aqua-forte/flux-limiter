package limiter

import (
	"context"
	"hash/fnv"
	"sync"
	"time"
)

const (
	defaultNumShards       = 64
	defaultCleanupInterval = 1 * time.Minute
	defaultEntryTTL        = 3 * time.Minute
)

// bucket tracks token status for a specific key.
type bucket struct {
	tokens     float64
	lastRefill time.Time
	lastAccess time.Time
}

// shard holds a partition of rate-limiting buckets with its own RWMutex.
type shard struct {
	mu      sync.RWMutex
	buckets map[string]*bucket
}

// MemoryLimiterOption defines functional configuration options for MemoryLimiter.
type MemoryLimiterOption func(*MemoryLimiter)

// WithCleanupInterval sets how often the background cleaner scans for idle buckets.
func WithCleanupInterval(interval time.Duration) MemoryLimiterOption {
	return func(m *MemoryLimiter) {
		if interval > 0 {
			m.cleanupInterval = interval
		}
	}
}

// WithTTL sets the maximum idle duration before a bucket is pruned.
func WithTTL(ttl time.Duration) MemoryLimiterOption {
	return func(m *MemoryLimiter) {
		if ttl > 0 {
			m.ttl = ttl
		}
	}
}

// WithClock allows mocking the time provider for testing.
func WithClock(clock func() time.Time) MemoryLimiterOption {
	return func(m *MemoryLimiter) {
		if clock != nil {
			m.nowFunc = clock
		}
	}
}

// MemoryLimiter implements Limiter using an in-memory, sharded Token Bucket algorithm.
type MemoryLimiter struct {
	rate            float64       // Tokens generated per second
	burst           int64         // Maximum bucket capacity
	cleanupInterval time.Duration // Interval between garbage collections
	ttl             time.Duration // Time before an inactive bucket is discarded
	nowFunc         func() time.Time

	shards    []*shard
	numShards uint64

	stopChan chan struct{}
	wg       sync.WaitGroup
	closeOnce sync.Once
	isClosed bool
	closedMu sync.RWMutex
}

// NewMemoryLimiter creates an initialized, thread-safe in-memory Token Bucket limiter.
func NewMemoryLimiter(rate float64, burst int64, opts ...MemoryLimiterOption) *MemoryLimiter {
	if rate <= 0 {
		rate = 1.0
	}
	if burst <= 0 {
		burst = 1
	}

	m := &MemoryLimiter{
		rate:            rate,
		burst:           burst,
		cleanupInterval: defaultCleanupInterval,
		ttl:             defaultEntryTTL,
		nowFunc:         time.Now,
		numShards:       defaultNumShards,
		shards:          make([]*shard, defaultNumShards),
		stopChan:        make(chan struct{}),
	}

	for i := uint64(0); i < m.numShards; i++ {
		m.shards[i] = &shard{
			buckets: make(map[string]*bucket),
		}
	}

	for _, opt := range opts {
		opt(m)
	}

	m.wg.Add(1)
	go m.cleanupLoop()

	return m
}

// getShard returns the shard responsible for the given key based on FNV-1a hash.
func (m *MemoryLimiter) getShard(key string) *shard {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	idx := h.Sum64() % m.numShards
	return m.shards[idx]
}

// Allow checks if 1 token is available for the given key.
func (m *MemoryLimiter) Allow(ctx context.Context, key string) (bool, error) {
	return m.AllowN(ctx, key, 1)
}

// AllowN checks if n tokens are available for the given key.
func (m *MemoryLimiter) AllowN(ctx context.Context, key string, n int64) (bool, error) {
	res, err := m.allowInternal(ctx, key, n)
	if err != nil {
		return false, err
	}
	return res.Allowed, nil
}

// AllowDetailed evaluates rate limits and returns complete telemetry metadata.
func (m *MemoryLimiter) AllowDetailed(ctx context.Context, key string) (Result, error) {
	return m.allowInternal(ctx, key, 1)
}

func (m *MemoryLimiter) allowInternal(ctx context.Context, key string, n int64) (Result, error) {
	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	default:
	}

	m.closedMu.RLock()
	if m.isClosed {
		m.closedMu.RUnlock()
		return Result{}, ErrLimiterClosed
	}
	m.closedMu.RUnlock()

	now := m.nowFunc()
	s := m.getShard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	b, exists := s.buckets[key]
	if !exists {
		b = &bucket{
			tokens:     float64(m.burst),
			lastRefill: now,
			lastAccess: now,
		}
		s.buckets[key] = b
	} else {
		// Refill tokens based on elapsed time
		elapsed := now.Sub(b.lastRefill).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * m.rate
			if b.tokens > float64(m.burst) {
				b.tokens = float64(m.burst)
			}
			b.lastRefill = now
		}
		b.lastAccess = now
	}

	needed := float64(n)
	var allowed bool
	var remaining int64
	var resetDuration time.Duration

	if b.tokens >= needed {
		b.tokens -= needed
		allowed = true
		remaining = int64(b.tokens)
		resetDuration = 0
	} else {
		allowed = false
		remaining = 0
		deficit := needed - b.tokens
		resetDuration = time.Duration((deficit / m.rate) * float64(time.Second))
	}

	return Result{
		Allowed:   allowed,
		Remaining: remaining,
		Reset:     resetDuration,
		Limit:     m.burst,
	}, nil
}

// cleanupLoop executes background eviction of inactive buckets.
func (m *MemoryLimiter) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case now := <-ticker.C:
			m.pruneExpired(now)
		}
	}
}

// pruneExpired removes buckets that have not been accessed within the TTL.
func (m *MemoryLimiter) pruneExpired(now time.Time) {
	for i := uint64(0); i < m.numShards; i++ {
		s := m.shards[i]
		s.mu.Lock()
		for k, b := range s.buckets {
			if now.Sub(b.lastAccess) > m.ttl {
				delete(s.buckets, k)
			}
		}
		s.mu.Unlock()
	}
}

// KeyCount returns total active tracked keys across all shards (useful for metrics & testing).
func (m *MemoryLimiter) KeyCount() int {
	total := 0
	for i := uint64(0); i < m.numShards; i++ {
		s := m.shards[i]
		s.mu.RLock()
		total += len(s.buckets)
		s.mu.RUnlock()
	}
	return total
}

// Close gracefully terminates the background cleanup goroutine.
func (m *MemoryLimiter) Close() error {
	m.closeOnce.Do(func() {
		m.closedMu.Lock()
		m.isClosed = true
		m.closedMu.Unlock()

		close(m.stopChan)
		m.wg.Wait()
	})
	return nil
}
