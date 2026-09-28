package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aqua-forte/flux-limiter/limiter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ProxyAndRateLimiting(t *testing.T) {
	// 1. Mock upstream server
	upstreamCalled := 0
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled++
		w.Header().Set("X-Upstream-Echo", "true")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"message":"upstream response"}`))
	}))
	defer upstreamServer.Close()

	upstreamURL, err := url.Parse(upstreamServer.URL)
	require.NoError(t, err)

	// In-memory limiter: 2 requests allowed burst
	lim := limiter.NewMemoryLimiter(1, 2)
	defer lim.Close()

	handler, err := NewHandler(HandlerOptions{
		Limiter:   lim,
		TargetURL: upstreamURL,
	})
	require.NoError(t, err)

	proxyServer := httptest.NewServer(handler)
	defer proxyServer.Close()

	client := proxyServer.Client()

	// Request 1: Allowed
	req1, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/api/v1/resource", nil)
	req1.Header.Set("X-Forwarded-For", "198.51.100.1")
	resp1, err := client.Do(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()

	assert.Equal(t, http.StatusOK, resp1.StatusCode)
	assert.Equal(t, "true", resp1.Header.Get("X-Upstream-Echo"))
	assert.Equal(t, "2", resp1.Header.Get("X-RateLimit-Limit"))
	assert.Equal(t, "1", resp1.Header.Get("X-RateLimit-Remaining"))

	body1, _ := io.ReadAll(resp1.Body)
	assert.Contains(t, string(body1), "upstream response")

	// Request 2: Allowed (exhausts burst)
	req2, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/api/v1/resource", nil)
	req2.Header.Set("X-Forwarded-For", "198.51.100.1")
	resp2, err := client.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusOK, resp2.StatusCode)
	assert.Equal(t, "0", resp2.Header.Get("X-RateLimit-Remaining"))

	// Request 3: Blocked (HTTP 429)
	req3, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/api/v1/resource", nil)
	req3.Header.Set("X-Forwarded-For", "198.51.100.1")
	resp3, err := client.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()

	assert.Equal(t, http.StatusTooManyRequests, resp3.StatusCode)
	assert.Equal(t, "0", resp3.Header.Get("X-RateLimit-Remaining"))
	assert.NotEmpty(t, resp3.Header.Get("Retry-After"))

	var errResp ErrorResponse
	err = json.NewDecoder(resp3.Body).Decode(&errResp)
	require.NoError(t, err)
	assert.Equal(t, "too_many_requests", errResp.Error)
	assert.Positive(t, errResp.RetryAfterSeconds)

	// Upstream should only have been called twice (blocked request never reached upstream)
	assert.Equal(t, 2, upstreamCalled)

	// Distinct client IP: Should still have capacity
	reqDistinct, _ := http.NewRequest(http.MethodGet, proxyServer.URL+"/api/v1/resource", nil)
	reqDistinct.Header.Set("X-Forwarded-For", "198.51.100.2")
	respDistinct, err := client.Do(reqDistinct)
	require.NoError(t, err)
	defer respDistinct.Body.Close()

	assert.Equal(t, http.StatusOK, respDistinct.StatusCode)
	assert.Equal(t, 3, upstreamCalled)
}

func TestHandler_ClientKeyExtraction(t *testing.T) {
	t.Run("extracts from Authorization", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer my-secret-token")
		req.Header.Set("X-Forwarded-For", "1.2.3.4")

		key := DefaultKeyExtractor(req)
		assert.Equal(t, "auth:Bearer my-secret-token", key)
	})

	t.Run("extracts leftmost IP from X-Forwarded-For", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Forwarded-For", "203.0.113.195, 70.41.3.18, 150.172.238.178")

		key := DefaultKeyExtractor(req)
		assert.Equal(t, "ip:203.0.113.195", key)
	})

	t.Run("extracts from X-Real-IP", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Real-IP", "198.51.100.42")

		key := DefaultKeyExtractor(req)
		assert.Equal(t, "ip:198.51.100.42", key)
	})

	t.Run("fallback to RemoteAddr stripping port", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = "192.168.1.100:54321"

		key := DefaultKeyExtractor(req)
		assert.Equal(t, "ip:192.168.1.100", key)
	})
}

func TestHandler_BadGateway(t *testing.T) {
	deadURL, _ := url.Parse("http://127.0.0.1:59999") // Unreachable
	lim := limiter.NewMemoryLimiter(10, 10)
	defer lim.Close()

	handler, err := NewHandler(HandlerOptions{
		Limiter:   lim,
		TargetURL: deadURL,
	})
	require.NoError(t, err)

	w := httptest.NewRecorder()
	r, _ := http.NewRequest(http.MethodGet, "/test", nil)
	r.RemoteAddr = "127.0.0.1:1234"

	handler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusBadGateway, w.Code)
	assert.Contains(t, w.Body.String(), "bad_gateway")
}
