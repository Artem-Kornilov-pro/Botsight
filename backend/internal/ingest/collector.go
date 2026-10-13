// Package ingest is the collector: it accepts SDK batches and writes them to Kafka
// (Botsight.md, 10).
package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// Producer is the part of *kgo.Client the collector uses.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// BotIDSetter records the bot of a project on its first event.
type BotIDSetter interface {
	SetProjectBotID(ctx context.Context, projectID int32, botID int64) error
}

type Config struct {
	MaxInflight int // concurrent batch requests per instance; above it 503 at once
	// ProduceTimeout bounds waiting for Kafka; it must exceed the producer's record
	// delivery timeout so that the outcome of every record is known when it returns.
	ProduceTimeout time.Duration
}

type Collector struct {
	keys     *KeyCache
	gate     *Gate
	producer Producer
	bots     BotIDSetter
	log      *slog.Logger
	metrics  *Metrics
	gatherer prometheus.Gatherer
	ready    func(context.Context) error
	cfg      Config

	inflight chan struct{}
	shutting atomic.Bool
	jobs     sync.WaitGroup
	now      func() time.Time
}

type Deps struct {
	Keys     *KeyCache
	Gate     *Gate
	Producer Producer
	Bots     BotIDSetter
	Logger   *slog.Logger
	Metrics  *Metrics
	Gatherer prometheus.Gatherer
	// Ready checks Kafka and Redis for /readyz.
	Ready  func(context.Context) error
	Config Config
}

func New(d Deps) *Collector {
	if d.Config.MaxInflight <= 0 {
		d.Config.MaxInflight = 512
	}
	if d.Config.ProduceTimeout <= 0 {
		d.Config.ProduceTimeout = 15 * time.Second
	}
	return &Collector{
		keys: d.Keys, gate: d.Gate, producer: d.Producer, bots: d.Bots, log: d.Logger,
		metrics: d.Metrics, gatherer: d.Gatherer, ready: d.Ready, cfg: d.Config,
		inflight: make(chan struct{}, d.Config.MaxInflight),
		now:      time.Now,
	}
}

// SetShuttingDown fails /readyz so the balancer stops sending batches (Botsight.md, 10.6).
func (c *Collector) SetShuttingDown() { c.shutting.Store(true) }

// Wait blocks until background jobs finish.
func (c *Collector) Wait() { c.jobs.Wait() }

func (c *Collector) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/batch", c.handleBatch)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", c.readyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(c.gatherer, promhttp.HandlerOpts{}))
	return mux
}

func (c *Collector) readyz(w http.ResponseWriter, r *http.Request) {
	if c.shutting.Load() {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := c.ready(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type batchResponse struct {
	Accepted  int  `json:"accepted"`
	Duplicate bool `json:"duplicate"`
	Rejected  int  `json:"rejected"`
}

func (c *Collector) handleBatch(w http.ResponseWriter, r *http.Request) {
	started := c.now()
	rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	defer func() {
		c.metrics.Requests.WithLabelValues(strconv.Itoa(rec.status)).Inc()
		c.metrics.Duration.Observe(time.Since(started).Seconds())
	}()
	w = rec

	// Overload protection: answer at once instead of queueing without bound.
	select {
	case c.inflight <- struct{}{}:
		defer func() { <-c.inflight }()
	default:
		c.metrics.Overloaded.Inc()
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "overloaded", "too many requests in flight, retry")
		return
	}

	// 1. API key -> project (cache; PostgreSQL only on a miss).
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "Authorization: Bearer <api key> required")
		return
	}
	project, err := c.keys.Lookup(r.Context(), strings.TrimSpace(key))
	if errors.Is(err, ErrInvalidKey) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or revoked API key")
		return
	}
	if err != nil {
		c.log.Error("key lookup failed", "err", err)
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cannot check the API key now")
		return
	}

	// 2. Body with size limits, then the envelope.
	buf, err := readBody(w, r)
	switch {
	case errors.Is(err, errTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "too_large", err.Error())
		return
	case errors.Is(err, errEncoding):
		writeError(w, http.StatusUnsupportedMediaType, "bad_encoding", err.Error())
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	batch, err := event.ParseBatch(buf.Bytes())
	putBuffer(buf) // ParseBatch copies the events
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	// 3. Dedup and rate limit, one Redis call.
	gate, err := c.gate.Admit(r.Context(), project.ID, batch.BatchID, len(batch.Events))
	if err != nil {
		c.log.Error("redis gate failed", "err", err)
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "temporarily unavailable")
		return
	}
	switch gate.Admission {
	case Duplicate:
		c.metrics.Duplicates.Inc()
		writeJSON(w, http.StatusAccepted, batchResponse{Accepted: gate.Accepted, Duplicate: true})
		return
	case InProgress:
		writeError(w, http.StatusConflict, "in_progress", "this batch is being processed, retry later")
		return
	case Limited:
		c.metrics.RateLimited.Inc()
		w.Header().Set("Retry-After", strconv.Itoa(int(gate.RetryAfter.Seconds())+1))
		writeError(w, http.StatusTooManyRequests, "rate_limited", "project event rate exceeded")
		return
	}

	// 4. Validate, normalize, split texts.
	records, accepted, rejected, botID := c.buildRecords(project, batch)
	c.metrics.Events.WithLabelValues("accepted").Add(float64(accepted))
	c.metrics.Events.WithLabelValues("rejected").Add(float64(rejected))

	// 5. Kafka: answer 202 only after every record is acknowledged. The produce context is
	// detached from the client so a disconnect cannot leave records half-written.
	pctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), c.cfg.ProduceTimeout)
	defer cancel()
	if err := c.producer.ProduceSync(pctx, records...).FirstErr(); err != nil {
		c.metrics.ProduceErrors.Inc()
		c.log.Error("kafka produce failed", "project_id", project.ID, "err", err)
		if rerr := c.gate.Release(context.WithoutCancel(r.Context()), project.ID, batch.BatchID); rerr != nil {
			c.log.Warn("failed to release batch key", "err", rerr)
		}
		w.Header().Set("Retry-After", "2")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "event bus unavailable, retry")
		return
	}
	if err := c.gate.Done(context.WithoutCancel(r.Context()), project.ID, batch.BatchID, accepted); err != nil {
		// The "processing" mark expires in 2 minutes; a retry after that would duplicate.
		c.log.Warn("failed to mark batch done", "err", err)
	}
	if !project.BotIDKnown && botID > 0 {
		c.recordBotID(project, botID)
	}
	writeJSON(w, http.StatusAccepted, batchResponse{Accepted: accepted, Rejected: rejected})
}

// buildRecords turns a batch into Kafka records: events, texts (only if the project
// collects them, Botsight.md, 9.4) and one ingest.batches record with SDK metadata.
func (c *Collector) buildRecords(project Project, batch *event.Batch) (records []*kgo.Record, accepted, rejected int, botID int64) {
	receivedAt := c.now().UTC()
	norm := event.NewNormalizer(uint32(project.ID), batch, receivedAt)
	records = make([]*kgo.Record, 0, len(batch.Events)+1)
	headers := []kgo.RecordHeader{
		{Key: "project_id", Value: []byte(strconv.Itoa(int(project.ID)))},
		{Key: "schema_version", Value: []byte(strconv.Itoa(event.SchemaVersion))},
	}
	for _, raw := range batch.Events {
		rec, text, err := norm.Normalize(raw)
		if err != nil {
			rejected++
			continue
		}
		value, err := json.Marshal(rec)
		if err != nil {
			rejected++
			continue
		}
		key := event.Key(rec.ProjectID, rec.UserID, rec.ChatID)
		records = append(records, &kgo.Record{Topic: kafka.TopicEvents, Key: key, Value: value, Headers: headers})
		if text != nil && project.CaptureText {
			if tv, err := json.Marshal(text); err == nil {
				records = append(records, &kgo.Record{Topic: kafka.TopicTexts, Key: key, Value: tv, Headers: headers})
			}
		}
		if botID == 0 {
			botID = int64(rec.BotID)
		}
		accepted++
	}
	meta, _ := json.Marshal(event.BatchMeta{
		ProjectID: uint32(project.ID), BatchID: strings.ToLower(batch.BatchID), ReceivedAt: receivedAt.UnixMilli(),
		SDKName: limit(batch.SDK.Name, 64), SDKVersion: limit(batch.SDK.Version, 32),
		Framework: limit(batch.SDK.Framework, 64), FrameworkVersion: limit(batch.SDK.FrameworkVersion, 32),
		Events: uint32(accepted), Rejected: uint32(rejected), Dropped: uint32(min(batch.Dropped, 1<<31)),
	})
	records = append(records, &kgo.Record{
		Topic: kafka.TopicBatches, Key: []byte(strconv.Itoa(int(project.ID))), Value: meta, Headers: headers,
	})
	return records, accepted, rejected, botID
}

func (c *Collector) recordBotID(project Project, botID int64) {
	c.keys.MarkBotIDKnown(project.Prefix)
	c.jobs.Add(1)
	go func() {
		defer c.jobs.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.bots.SetProjectBotID(ctx, project.ID, botID); err != nil {
			c.log.Warn("failed to record project bot_id", "project_id", project.ID, "err", err)
		}
	}()
}

func limit(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}
