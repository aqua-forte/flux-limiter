package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aqua-forte/flux-limiter/config"
	"github.com/aqua-forte/flux-limiter/limiter"
	"github.com/aqua-forte/flux-limiter/proxy"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("[FATAL] Configuration load failed: %v", err)
	}

	log.Println("==================================================")
	log.Println("   ⚡ FLUX-LIMITER: High-Performance Proxy       ")
	log.Println("==================================================")
	log.Printf("[INFO] Listening Port:        %s", cfg.Port)
	log.Printf("[INFO] Target Upstream URL:   %s", cfg.TargetURL.String())
	log.Printf("[INFO] Limiter Engine:        %s", cfg.LimiterType)
	log.Printf("[INFO] Rate Limit:            %d req/window", cfg.RateLimit)
	log.Printf("[INFO] Burst Limit:           %d tokens", cfg.BurstLimit)
	log.Printf("[INFO] Window Duration:       %s", cfg.WindowDuration)

	var lim limiter.Limiter

	switch cfg.LimiterType {
	case "redis":
		log.Printf("[INFO] Connecting to Redis at %s...", cfg.RedisAddr)
		rdb := redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       cfg.RedisDB,
		})

		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			pingCancel()
			log.Fatalf("[FATAL] Could not connect to Redis: %v", err)
		}
		pingCancel()
		log.Printf("[INFO] Successfully connected to Redis (Algorithm: %s)", cfg.RedisAlgorithm)

		var algo limiter.Algorithm
		if cfg.RedisAlgorithm == "sliding_window" {
			algo = limiter.AlgorithmSlidingWindow
		} else {
			algo = limiter.AlgorithmFixedWindow
		}

		lim = limiter.NewRedisLimiter(
			rdb,
			cfg.RateLimit,
			cfg.WindowDuration,
			limiter.WithAlgorithm(algo),
		)

	default: // "memory"
		log.Printf("[INFO] Initializing in-memory Token Bucket (Cleanup: %s, TTL: %s)", cfg.CleanupInterval, cfg.EntryTTL)
		lim = limiter.NewMemoryLimiter(
			float64(cfg.RateLimit),
			cfg.BurstLimit,
			limiter.WithCleanupInterval(cfg.CleanupInterval),
			limiter.WithTTL(cfg.EntryTTL),
		)
	}

	keyExtractor := proxy.DefaultKeyExtractor
	if cfg.KeyExtractor == "ip" {
		keyExtractor = proxy.IPOnlyKeyExtractor
	}

	proxyHandler, err := proxy.NewHandler(proxy.HandlerOptions{
		Limiter:      lim,
		TargetURL:    cfg.TargetURL,
		KeyExtractor: keyExtractor,
	})
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize proxy handler: %v", err)
	}

	mux := http.NewServeMux()
	// Built-in health check probe
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, `{"status":"ok","service":"flux-limiter"}`)
	})
	// All other traffic routed through rate-limiting reverse proxy
	mux.Handle("/", proxyHandler)

	server := &http.Server{
		Addr:         cfg.Port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Server runner goroutine
	serverErrChan := make(chan error, 1)
	go func() {
		log.Printf("[INFO] Flux-Limiter proxy listening on %s", cfg.Port)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrChan <- err
		}
	}()

	// Graceful shutdown listener
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serverErrChan:
		log.Fatalf("[FATAL] Server error: %v", err)
	case sig := <-stopChan:
		log.Printf("[INFO] Received signal %v. Initiating graceful shutdown...", sig)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] Server forced to shutdown: %v", err)
	} else {
		log.Println("[INFO] HTTP server stopped cleanly.")
	}

	if err := lim.Close(); err != nil {
		log.Printf("[WARN] Limiter close returned error: %v", err)
	} else {
		log.Println("[INFO] Limiter resources released cleanly.")
	}

	log.Println("[INFO] Flux-Limiter shutdown complete.")
}
