// Command chwriter moves events from Kafka to ClickHouse (Botsight.md, 11.2).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/chwriter"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/clickhouse"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/config"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ch-writer stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadCHWriter()
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

	conn, err := clickhouse.Open(ctx, clickhouse.Options{
		Addr: []string{cfg.ClickHouseAddr}, Database: cfg.ClickHouseDatabase,
		Username: cfg.ClickHouseUser, Password: cfg.ClickHousePassword,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	brokers := kafka.Brokers(cfg.KafkaBrokers)
	if cfg.CreateTopics {
		admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
		if err != nil {
			return err
		}
		err = kafka.EnsureTopics(ctx, admin, cfg.KafkaReplication, kafka.Topics())
		admin.Close()
		if err != nil {
			return err
		}
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	writer, err := chwriter.New(chwriter.Config{
		Brokers: brokers, Group: cfg.Group, BatchRows: cfg.BatchRows, FlushInterval: cfg.FlushInterval,
	}, conn, rdb, chwriter.Sinks(), logger, chwriter.NewMetrics(reg))
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { status(w, http.StatusOK, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := conn.Ping(pctx); err != nil {
			status(w, http.StatusServiceUnavailable, "clickhouse is unavailable")
			return
		}
		if err := writer.Ping(pctx); err != nil {
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

	logger.Info("ch-writer started", "group", cfg.Group, "batch_rows", cfg.BatchRows, "flush_interval", cfg.FlushInterval.String())
	runErr := writer.Run(ctx) // returns after flushing buffers and leaving the group
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
