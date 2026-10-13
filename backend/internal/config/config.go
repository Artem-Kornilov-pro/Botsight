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

	// ClickHouse for AI topics and alerts; empty CLICKHOUSE_ADDR turns these endpoints off.
	ClickHouseAddr     string // CLICKHOUSE_ADDR host:9000
	ClickHouseDatabase string // CLICKHOUSE_DATABASE, default botsight
	ClickHouseUser     string // CLICKHOUSE_USER, default default
	ClickHousePassword string // CLICKHOUSE_PASSWORD
	AIURL              string // AI_URL of ai-worker for /ask; empty turns questions off
	AIInternalToken    string // AI_INTERNAL_TOKEN shared with ai-worker
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

		ClickHouseAddr:     os.Getenv("CLICKHOUSE_ADDR"),
		ClickHouseDatabase: env("CLICKHOUSE_DATABASE", "botsight"),
		ClickHouseUser:     env("CLICKHOUSE_USER", "default"),
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
		AIURL:              strings.TrimRight(os.Getenv("AI_URL"), "/"),
		AIInternalToken:    os.Getenv("AI_INTERNAL_TOKEN"),
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

// Collector is the configuration of the collector service (Botsight.md, 10).
type Collector struct {
	Addr             string        // COLLECTOR_ADDR, default :8081
	DatabaseURL      string        // DATABASE_URL: API keys on cache misses
	RedisURL         string        // REDIS_URL: dedup, rate limits, key revocations
	KafkaBrokers     string        // KAFKA_BROKERS, comma-separated
	KafkaReplication int16         // KAFKA_REPLICATION_FACTOR for created topics, default 1 (3 in production)
	CreateTopics     bool          // KAFKA_CREATE_TOPICS: create missing topics on start, default true
	RatePerSecond    float64       // INGEST_RATE: events per second per project, default 2000
	Burst            float64       // INGEST_BURST: bucket size per project, default 20000
	MaxInflight      int           // INGEST_MAX_INFLIGHT: concurrent batches per instance, default 512
	KeyCacheTTL      time.Duration // KEY_CACHE_TTL, default 60s
	LogLevel         string        // LOG_LEVEL
}

func LoadCollector() (Collector, error) {
	c := Collector{
		Addr:         env("COLLECTOR_ADDR", ":8081"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		RedisURL:     os.Getenv("REDIS_URL"),
		KafkaBrokers: os.Getenv("KAFKA_BROKERS"),
		LogLevel:     env("LOG_LEVEL", "info"),
	}
	p := parser{}
	c.KafkaReplication = int16(p.int("KAFKA_REPLICATION_FACTOR", 1))
	c.CreateTopics = p.bool("KAFKA_CREATE_TOPICS", true)
	c.RatePerSecond = p.float("INGEST_RATE", 2000)
	c.Burst = p.float("INGEST_BURST", 20000)
	c.MaxInflight = p.int("INGEST_MAX_INFLIGHT", 512)
	c.KeyCacheTTL = p.duration("KEY_CACHE_TTL", 60*time.Second)
	if p.err != nil {
		return c, p.err
	}
	switch {
	case c.DatabaseURL == "":
		return c, fmt.Errorf("config: DATABASE_URL is required")
	case c.RedisURL == "":
		return c, fmt.Errorf("config: REDIS_URL is required")
	case c.KafkaBrokers == "":
		return c, fmt.Errorf("config: KAFKA_BROKERS is required")
	case c.Burst < 500:
		return c, fmt.Errorf("config: INGEST_BURST must fit a full batch (500 events)")
	case c.RatePerSecond <= 0:
		return c, fmt.Errorf("config: INGEST_RATE must be positive")
	}
	return c, nil
}

// CHWriter is the configuration of ch-writer (Botsight.md, 11.2).
type CHWriter struct {
	Addr               string        // CHWRITER_ADDR: /healthz, /readyz, /metrics; default :8082
	RedisURL           string        // REDIS_URL: chunk plans
	KafkaBrokers       string        // KAFKA_BROKERS
	KafkaReplication   int16         // KAFKA_REPLICATION_FACTOR
	CreateTopics       bool          // KAFKA_CREATE_TOPICS
	Group              string        // CHWRITER_GROUP, default ch-writer
	ClickHouseAddr     string        // CLICKHOUSE_ADDR host:port of the native protocol
	ClickHouseDatabase string        // CLICKHOUSE_DATABASE, default botsight
	ClickHouseUser     string        // CLICKHOUSE_USER, default default
	ClickHousePassword string        // CLICKHOUSE_PASSWORD
	BatchRows          int           // CHWRITER_BATCH_ROWS, default 50000
	FlushInterval      time.Duration // CHWRITER_FLUSH_INTERVAL, default 1s
	LogLevel           string
}

func LoadCHWriter() (CHWriter, error) {
	c := CHWriter{
		Addr:               env("CHWRITER_ADDR", ":8082"),
		RedisURL:           os.Getenv("REDIS_URL"),
		KafkaBrokers:       os.Getenv("KAFKA_BROKERS"),
		Group:              env("CHWRITER_GROUP", "ch-writer"),
		ClickHouseAddr:     os.Getenv("CLICKHOUSE_ADDR"),
		ClickHouseDatabase: env("CLICKHOUSE_DATABASE", "botsight"),
		ClickHouseUser:     env("CLICKHOUSE_USER", "default"),
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
		LogLevel:           env("LOG_LEVEL", "info"),
	}
	p := parser{}
	c.KafkaReplication = int16(p.int("KAFKA_REPLICATION_FACTOR", 1))
	c.CreateTopics = p.bool("KAFKA_CREATE_TOPICS", true)
	c.BatchRows = p.int("CHWRITER_BATCH_ROWS", 50_000)
	c.FlushInterval = p.duration("CHWRITER_FLUSH_INTERVAL", time.Second)
	if p.err != nil {
		return c, p.err
	}
	switch {
	case c.RedisURL == "":
		return c, fmt.Errorf("config: REDIS_URL is required")
	case c.KafkaBrokers == "":
		return c, fmt.Errorf("config: KAFKA_BROKERS is required")
	case c.ClickHouseAddr == "":
		return c, fmt.Errorf("config: CLICKHOUSE_ADDR is required")
	}
	return c, nil
}

// parser reads typed variables and keeps the first error.
type parser struct{ err error }

func (p *parser) fail(key, v string) {
	if p.err == nil {
		p.err = fmt.Errorf("config: %s: invalid value %q", key, v)
	}
}

func (p *parser) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		p.fail(key, v)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail(key, v)
		return def
	}
	return f
}

func (p *parser) bool(key string, def bool) bool {
	b, err := envBool(key, def)
	if err != nil {
		p.fail(key, os.Getenv(key))
	}
	return b
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		p.fail(key, v)
		return def
	}
	return d
}

// Antifraud is the configuration of the antifraud service (Botsight.md, 15).
type Antifraud struct {
	Addr               string        // ANTIFRAUD_ADDR: /healthz, /readyz, /metrics; default :8083
	RedisURL           string        // REDIS_URL: one scoring run per minute across instances
	KafkaBrokers       string        // KAFKA_BROKERS
	KafkaReplication   int16         // KAFKA_REPLICATION_FACTOR
	CreateTopics       bool          // KAFKA_CREATE_TOPICS
	Group              string        // ANTIFRAUD_GROUP, default antifraud
	ClickHouseAddr     string        // CLICKHOUSE_ADDR host:9000
	ClickHouseDatabase string        // CLICKHOUSE_DATABASE, default botsight
	ClickHouseUser     string        // CLICKHOUSE_USER, default default
	ClickHousePassword string        // CLICKHOUSE_PASSWORD
	Replay             time.Duration // ANTIFRAUD_REPLAY: window re-read on partition assignment, default 24h
	FlushInterval      time.Duration // ANTIFRAUD_FLUSH_INTERVAL: user_features writes, default 10s
	MaxUsers           int           // ANTIFRAUD_MAX_USERS per instance, default 5 000 000
	RunInterval        time.Duration // ANTIFRAUD_RUN_INTERVAL: scoring, default 1m
	GraphEvery         int           // ANTIFRAUD_GRAPH_EVERY: rebuild the referral graph on every n-th run, default 5
	LogLevel           string
}

func LoadAntifraud() (Antifraud, error) {
	c := Antifraud{
		Addr:               env("ANTIFRAUD_ADDR", ":8083"),
		RedisURL:           os.Getenv("REDIS_URL"),
		KafkaBrokers:       os.Getenv("KAFKA_BROKERS"),
		Group:              env("ANTIFRAUD_GROUP", "antifraud"),
		ClickHouseAddr:     os.Getenv("CLICKHOUSE_ADDR"),
		ClickHouseDatabase: env("CLICKHOUSE_DATABASE", "botsight"),
		ClickHouseUser:     env("CLICKHOUSE_USER", "default"),
		ClickHousePassword: os.Getenv("CLICKHOUSE_PASSWORD"),
		LogLevel:           env("LOG_LEVEL", "info"),
	}
	p := parser{}
	c.KafkaReplication = int16(p.int("KAFKA_REPLICATION_FACTOR", 1))
	c.CreateTopics = p.bool("KAFKA_CREATE_TOPICS", true)
	c.Replay = p.duration("ANTIFRAUD_REPLAY", 24*time.Hour)
	c.FlushInterval = p.duration("ANTIFRAUD_FLUSH_INTERVAL", 10*time.Second)
	c.MaxUsers = p.int("ANTIFRAUD_MAX_USERS", 5_000_000)
	c.RunInterval = p.duration("ANTIFRAUD_RUN_INTERVAL", time.Minute)
	c.GraphEvery = p.int("ANTIFRAUD_GRAPH_EVERY", 5)
	if p.err != nil {
		return c, p.err
	}
	switch {
	case c.RedisURL == "":
		return c, fmt.Errorf("config: REDIS_URL is required")
	case c.KafkaBrokers == "":
		return c, fmt.Errorf("config: KAFKA_BROKERS is required")
	case c.ClickHouseAddr == "":
		return c, fmt.Errorf("config: CLICKHOUSE_ADDR is required")
	}
	return c, nil
}
