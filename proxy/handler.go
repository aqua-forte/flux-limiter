package proxy

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/aqua-forte/flux-limiter/limiter"
)

// KeyExtractor extracts a unique rate-limiting key from an incoming HTTP request.
type KeyExtractor func(r *http.Request) string

// DefaultKeyExtractor extracts client identity checking Authorization, X-Forwarded-For, X-Real-IP, and RemoteAddr.
func DefaultKeyExtractor(r *http.Request) string {
	// 1. Authorization header (e.g. API keys or Bearer tokens)
	if auth := r.Header.Get("Authorization"); auth != "" {
		return "auth:" + strings.TrimSpace(auth)
	}

	// 2. X-Forwarded-For (leftmost IP is client IP)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			clientIP := strings.TrimSpace(parts[0])
			if clientIP != "" {
				return "ip:" + clientIP
			}
		}
	}

	// 3. X-Real-IP
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return "ip:" + strings.TrimSpace(realIP)
	}

	// 4. RemoteAddr fallback (strip port if present)
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return "ip:" + host
	}
	return "ip:" + r.RemoteAddr
}

// IPOnlyKeyExtractor extracts purely IP-based keys (ignoring Authorization).
func IPOnlyKeyExtractor(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			clientIP := strings.TrimSpace(parts[0])
			if clientIP != "" {
				return clientIP
			}
		}
	}
	if realIP := r.Header.Get("X-Real-IP"); realIP != "" {
		return strings.TrimSpace(realIP)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return host
	}
	return r.RemoteAddr
}

// HandlerOptions configures the behavior of the rate limiting reverse proxy.
type HandlerOptions struct {
	Limiter      limiter.Limiter
	TargetURL    *url.URL
	KeyExtractor KeyExtractor
}

// Handler wraps an httputil.ReverseProxy with rate-limiting enforcement.
type Handler struct {
	limiter      limiter.Limiter
	proxy        *httputil.ReverseProxy
	keyExtractor KeyExtractor
	targetURL    *url.URL
}

// ErrorResponse defines the structured JSON body returned when rate-limited.
type ErrorResponse struct {
	Error             string `json:"error"`
	Message           string `json:"message"`
	RetryAfterSeconds int    `json:"retry_after_seconds"`
}

// NewHandler constructs a new rate-limiting reverse proxy handler.
func NewHandler(opts HandlerOptions) (*Handler, error) {
	if opts.Limiter == nil {
		return nil, fmt.Errorf("limiter cannot be nil")
	}
	if opts.TargetURL == nil {
		return nil, fmt.Errorf("target URL cannot be nil")
	}
	if opts.KeyExtractor == nil {
		opts.KeyExtractor = DefaultKeyExtractor
	}

	proxy := httputil.NewSingleHostReverseProxy(opts.TargetURL)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "bad_gateway",
			"message": fmt.Sprintf("Upstream gateway error: %v", err),
		})
	}

	return &Handler{
		limiter:      opts.Limiter,
		proxy:        proxy,
		keyExtractor: opts.KeyExtractor,
		targetURL:    opts.TargetURL,
	}, nil
}

// ServeHTTP inspects the request, evaluates rate limits, and proxies or responds 429.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := h.keyExtractor(r)
	ctx := r.Context()

	res, err := h.limiter.AllowDetailed(ctx, key)
	if err != nil {
		// Internal failure in limiter
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":   "rate_limit_error",
			"message": "Failed to verify rate limits",
		})
		return
	}

	retryAfterSec := int(math.Ceil(res.Reset.Seconds()))
	if retryAfterSec < 1 && !res.Allowed {
		retryAfterSec = 1
	}

	// Always emit telemetry headers
	w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%d", res.Limit))

	if !res.Allowed {
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfterSec))
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", retryAfterSec))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)

		_ = json.NewEncoder(w).Encode(ErrorResponse{
			Error:             "too_many_requests",
			Message:           "Rate limit exceeded. Please retry later.",
			RetryAfterSeconds: retryAfterSec,
		})
		return
	}

	// Request is allowed
	w.Header().Set("X-RateLimit-Remaining", fmt.Sprintf("%d", res.Remaining))
	if res.Reset > 0 {
		w.Header().Set("X-RateLimit-Reset", fmt.Sprintf("%d", int(math.Ceil(res.Reset.Seconds()))))
	} else {
		w.Header().Set("X-RateLimit-Reset", "0")
	}

	// Forward transparently to upstream target
	h.proxy.ServeHTTP(w, r)
}
