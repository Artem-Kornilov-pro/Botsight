// Command antifraud estimates the share of bot farms per traffic source (Botsight.md, 15).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/antifraud"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/clickhouse"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/config"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

func main() {
	if err := run(); err != nil {
		slog.Error("antifraud stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAntifraud()
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

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	ch, err := clickhouse.Open(ctx, clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr}, Database: cfg.ClickHouseDatabase,
		Username: cfg.ClickHouseUser, Password: cfg.ClickHousePassword,
	})
	if err != nil {
		return err
	}
	defer ch.Close()

	brokers := kafka.Brokers(cfg.KafkaBrokers)
	producer, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, kafka.ProducerOptions()...)...)
	if err != nil {
		return err
	}
	defer producer.Close()
	if cfg.CreateTopics {
		if err := kafka.EnsureTopics(ctx, producer, cfg.KafkaReplication, kafka.Topics()); err != nil {
			return err
		}
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := antifraud.NewMetrics(reg)

	stream, err := antifraud.NewStream(antifraud.StreamConfig{
		Brokers: brokers, Group: cfg.Group, Replay: cfg.Replay,
		FlushInterval: cfg.FlushInterval, MaxUsers: cfg.MaxUsers,
	}, ch, logger, metrics)
	if err != nil {
		return err
	}
	analyzer := antifraud.NewAnalyzer(ch, antifraud.KafkaAlerts{Client: producer}, logger, metrics)
	analyzer.GraphEvery = cfg.GraphEvery
	scheduler := &antifraud.Scheduler{Analyzer: analyzer, Redis: rdb, Every: cfg.RunInterval, Log: logger}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { status(w, http.StatusOK, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := ch.Ping(pctx); err != nil {
			status(w, http.StatusServiceUnavailable, "clickhouse is unavailable")
			return
		}
		if err := stream.Ping(pctx); err != nil {
			status(w, http.StatusServiceUnavailable, "kafka is unavailable")
			return
		}
		status(w, http.StatusOK, "ready")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	httpServer := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
		}
	}()

	logger.Info("antifraud started", "group", cfg.Group, "replay", cfg.Replay.String())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		scheduler.Loop(ctx)
	}()
	runErr := stream.Run(ctx) // replays the window, then follows the stream until ctx ends
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	return runErr
}

func status(w http.ResponseWriter, code int, s string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": s})
}
