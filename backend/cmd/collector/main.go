// Command collector accepts event batches from SDKs and writes them to Kafka (Botsight.md, 10).
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/config"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ingest"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("collector stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadCollector()
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	producer, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafka.Brokers(cfg.KafkaBrokers)...)}, kafka.ProducerOptions()...)...)
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
	metrics := ingest.NewMetrics(reg)
	st := store.New(pool)
	keys := ingest.NewKeyCache(st, cfg.KeyCacheTTL, 10*time.Second, 100_000, metrics)
	go keys.ListenRevocations(ctx, rdb, logger)

	collector := ingest.New(ingest.Deps{
		Keys:     keys,
		Gate:     ingest.NewGate(rdb, cfg.RatePerSecond, cfg.Burst),
		Producer: producer,
		Bots:     st,
		Logger:   logger,
		Metrics:  metrics,
		Gatherer: reg,
		Ready: func(ctx context.Context) error {
			if err := rdb.Ping(ctx).Err(); err != nil {
				return errors.New("redis is unavailable")
			}
			if err := producer.Ping(ctx); err != nil {
				return errors.New("kafka is unavailable")
			}
			return nil
		},
		Config: ingest.Config{MaxInflight: cfg.MaxInflight},
	})

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           collector.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		logger.Info("collector listening", "addr", cfg.Addr)
		errc <- httpServer.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Botsight.md, 10.6: fail readiness, let the balancer notice, finish in-flight
	// requests (each waits for its Kafka acks), flush the producer, exit.
	logger.Info("shutting down")
	collector.SetShuttingDown()
	time.Sleep(2 * time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	if err := producer.Flush(shutdownCtx); err != nil {
		return err
	}
	collector.Wait()
	return nil
}
