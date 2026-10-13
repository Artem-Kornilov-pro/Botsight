// Command api is the dashboard control plane: accounts, projects, API keys (Botsight.md, 7.2).
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
	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/config"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/mail"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/session"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
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
	if err := pool.Ping(ctx); err != nil {
		return err
	}

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}

	var mailer mail.Mailer = mail.Log{Logger: logger}
	if cfg.SMTPAddr != "" {
		mailer = mail.SMTP{Addr: cfg.SMTPAddr, From: cfg.SMTPFrom, Username: cfg.SMTPUsername, Password: cfg.SMTPPassword}
	}

	srv := httpapi.New(httpapi.Deps{
		Store:    store.New(pool),
		Sessions: session.NewStore(rdb, cfg.SessionTTL),
		Limiter:  ratelimit.NewWindow(rdb),
		Redis:    rdb,
		Mailer:   mailer,
		Logger:   logger,
		Options: httpapi.Options{
			DashboardURL:  cfg.DashboardURL,
			CORSOrigins:   cfg.CORSOrigins,
			CookieSecure:  cfg.CookieSecure,
			SessionTTL:    cfg.SessionTTL,
			TrustProxy:    cfg.TrustProxy,
			TelegramToken: cfg.TelegramToken,
			Limits:        httpapi.DefaultLimits(),
		},
	})
	if cfg.TelegramToken == "" {
		logger.Warn("TELEGRAM_BOT_TOKEN is not set: Telegram sign-in is disabled")
	}

	httpServer := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		logger.Info("api listening", "addr", cfg.Addr)
		errc <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Graceful stop (Botsight.md, 10.6): fail readiness, finish in-flight requests,
	// wait for background emails, then close connections.
	logger.Info("shutting down")
	srv.SetReady(false)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	srv.Wait()
	return nil
}
