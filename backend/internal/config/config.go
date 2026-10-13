// Package config reads service settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// API is the configuration of the api service.
type API struct {
	Addr          string        // API_ADDR, default :8080
	DatabaseURL   string        // DATABASE_URL, required
	RedisURL      string        // REDIS_URL, required
	DashboardURL  string        // DASHBOARD_URL: links in emails and the allowed CORS origin
	CORSOrigins   []string      // CORS_ORIGINS, comma-separated; defaults to DASHBOARD_URL
	CookieSecure  bool          // COOKIE_SECURE, default true; false only for local http
	SessionTTL    time.Duration // SESSION_TTL, default 720h (30 days)
	TrustProxy    bool          // TRUST_PROXY: take the client IP from X-Forwarded-For / X-Real-IP
	TelegramToken string        // TELEGRAM_BOT_TOKEN of the Botsight login bot; empty disables Telegram sign-in
	SMTPAddr      string        // SMTP_ADDR host:port; empty logs emails instead of sending
	SMTPFrom      string        // SMTP_FROM
	SMTPUsername  string        // SMTP_USERNAME
	SMTPPassword  string        // SMTP_PASSWORD
	LogLevel      string        // LOG_LEVEL: debug, info, warn, error
}

func LoadAPI() (API, error) {
	c := API{
		Addr:          env("API_ADDR", ":8080"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		RedisURL:      os.Getenv("REDIS_URL"),
		DashboardURL:  strings.TrimRight(env("DASHBOARD_URL", "http://localhost:5173"), "/"),
		TelegramToken: os.Getenv("TELEGRAM_BOT_TOKEN"),
		SMTPAddr:      os.Getenv("SMTP_ADDR"),
		SMTPFrom:      env("SMTP_FROM", "Botsight <no-reply@botsight.ru>"),
		SMTPUsername:  os.Getenv("SMTP_USERNAME"),
		SMTPPassword:  os.Getenv("SMTP_PASSWORD"),
		LogLevel:      env("LOG_LEVEL", "info"),
	}
	var err error
	if c.CookieSecure, err = envBool("COOKIE_SECURE", true); err != nil {
		return c, err
	}
	if c.TrustProxy, err = envBool("TRUST_PROXY", false); err != nil {
		return c, err
	}
	if c.SessionTTL, err = time.ParseDuration(env("SESSION_TTL", "720h")); err != nil || c.SessionTTL <= 0 {
		return c, fmt.Errorf("config: SESSION_TTL: invalid duration")
	}
	c.CORSOrigins = []string{c.DashboardURL}
	if v := os.Getenv("CORS_ORIGINS"); v != "" {
		c.CORSOrigins = nil
		for _, o := range strings.Split(v, ",") {
			if o = strings.TrimRight(strings.TrimSpace(o), "/"); o != "" {
				c.CORSOrigins = append(c.CORSOrigins, o)
			}
		}
	}
	if c.DatabaseURL == "" {
		return c, fmt.Errorf("config: DATABASE_URL is required")
	}
	if c.RedisURL == "" {
		return c, fmt.Errorf("config: REDIS_URL is required")
	}
	return c, nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def, fmt.Errorf("config: %s: %q is not a boolean", key, v)
	}
	return b, nil
}
