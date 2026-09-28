package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config encapsulates runtime settings for flux-limiter proxy.
type Config struct {
	Port            string
	TargetURL       *url.URL
	LimiterType     string // "memory" or "redis"
	RateLimit       int64  // Tokens per second or requests per window
	BurstLimit      int64  // Max burst capacity for token bucket
	WindowDuration  time.Duration
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	RedisAlgorithm  string // "fixed_window" or "sliding_window"
	CleanupInterval time.Duration
	EntryTTL        time.Duration
	KeyExtractor    string // "default" or "ip"
}

// LoadFromEnv parses configuration from environment variables with sensible defaults.
func LoadFromEnv() (*Config, error) {
	port := getEnv("PORT", ":8080")
	if port != "" && port[0] != ':' {
		port = ":" + port
	}

	rawTarget := getEnv("TARGET_URL", "http://localhost:8081")
	targetURL, err := url.Parse(rawTarget)
	if err != nil || targetURL.Scheme == "" || targetURL.Host == "" {
		return nil, fmt.Errorf("invalid TARGET_URL '%s': %w", rawTarget, err)
	}

	rateLimit := getEnvInt64("RATE_LIMIT", 100)
	burstLimit := getEnvInt64("BURST_LIMIT", rateLimit)
	windowDuration := getEnvDuration("WINDOW_DURATION", 1*time.Second)

	return &Config{
		Port:            port,
		TargetURL:       targetURL,
		LimiterType:     getEnv("LIMITER_TYPE", "memory"),
		RateLimit:       rateLimit,
		BurstLimit:      burstLimit,
		WindowDuration:  windowDuration,
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   os.Getenv("REDIS_PASSWORD"),
		RedisDB:         getEnvInt("REDIS_DB", 0),
		RedisAlgorithm:  getEnv("REDIS_ALGORITHM", "fixed_window"),
		CleanupInterval: getEnvDuration("CLEANUP_INTERVAL", 1*time.Minute),
		EntryTTL:        getEnvDuration("ENTRY_TTL", 3*time.Minute),
		KeyExtractor:    getEnv("KEY_EXTRACTOR", "default"),
	}, nil
}

func getEnv(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if val, ok := os.LookupEnv(key); ok && val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			return d
		}
	}
	return defaultVal
}
