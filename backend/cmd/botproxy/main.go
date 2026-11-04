// Command botproxy is the Bot API proxy mode (Botsight.md, 20): bots without an SDK send
// their Bot API calls through it and get analytics without changing their code.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/botproxy"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/config"
)

func main() {
	if err := run(); err != nil {
		slog.Error("botproxy stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadBotProxy()
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var hooks botproxy.HookStore = botproxy.NewMemoryHooks()
	var rdb *redis.Client
	if cfg.RedisURL != "" {
		opts, err := redis.ParseURL(cfg.RedisURL)
		if err != nil {
			return err
		}
		rdb = redis.NewClient(opts)
		defer rdb.Close()
		hooks = botproxy.RedisHooks{R: rdb}
	} else if cfg.PublicURL != "" {
		logger.Warn("REDIS_URL is not set: webhook addresses live in memory and are lost on restart")
	}
	if cfg.PublicURL == "" {
		logger.Warn("BOTPROXY_PUBLIC_URL is not set: webhooks go around the proxy, only polling bots are seen fully")
	}

	dedup := botproxy.NewDedup()
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := botproxy.NewMetrics(reg, dedup.Bots)

	sink := &botproxy.Sink{Collector: cfg.CollectorURL, Logger: logger, Metrics: metrics, FlushEvery: cfg.FlushInterval}
	sink.Start(cfg.QueueSize)

	proxy, err := botproxy.New(botproxy.Options{
		Telegram: cfg.TelegramURL, PublicURL: cfg.PublicURL, UnsafeWebhooks: cfg.UnsafeWebhooks,
	}, sink, hooks, dedup, metrics, logger)
	if err != nil {
		return err
	}

	var ready atomic.Bool
	ready.Store(true)
	mux := http.NewServeMux()
	mux.Handle("/", proxy.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { status(w, http.StatusOK, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready.Load() {
			status(w, http.StatusServiceUnavailable, "shutting down")
			return
		}
		// the proxy itself needs only Telegram; Redis matters for webhooks only
		if rdb != nil {
			pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := rdb.Ping(pctx).Err(); err != nil {
				status(w, http.StatusServiceUnavailable, "redis is unavailable")
				return
			}
		}
		status(w, http.StatusOK, "ready")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// no ReadTimeout and WriteTimeout: uploads of 50 MB and long polling take long;
		// the transport to Telegram bounds the wait for an answer instead
		IdleTimeout: 120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		logger.Info("botproxy listening", "addr", cfg.Addr, "telegram", cfg.TelegramURL)
		errc <- server.ListenAndServe()
	}()

	go func() { // forget windows of bots that stopped
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				dedup.Sweep()
			}
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Stop without losses: fail readiness, let polling requests finish (they end within
	// the polling timeout), then send the queued events.
	logger.Info("shutting down")
	ready.Store(false)
	time.Sleep(2 * time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Warn("shutdown", "err", err)
	}
	return sink.Close(shutdownCtx)
}

func status(w http.ResponseWriter, code int, s string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": s})
}
