package botproxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Sink delivers events to the collector in batches, like the Python SDK (Botsight.md, 8.8):
// the bot must never wait for Botsight. Emit only puts an event in a bounded queue; when
// the queue is full the event is dropped and reported in the "dropped" field of the next
// batch of its project. A batch is retried with the same batch_id, so the collector
// accepts it once (Botsight.md, 10.4).
type Sink struct {
	Collector  string // base URL, POST {Collector}/v1/batch
	Client     *http.Client
	Logger     *slog.Logger
	Metrics    *Metrics
	FlushEvery time.Duration // default 1 s
	MaxBatch   int           // default 500, the collector's limit
	Retries    int           // attempts after the first; default 5
	Backoff    time.Duration // first pause between attempts, doubled; default 500 ms
	Parallel   int           // batches in flight; default 8

	queue  chan item
	done   chan struct{}
	mu     sync.RWMutex // Emit holds it for reading, Close for writing: no send on a closed queue
	closed bool
	drops  map[string]int64 // dropped events by key, reported in the key's next batch
	dropMu sync.Mutex
}

type item struct {
	key string
	ev  Event
}

const sdkName, sdkVersion = "botsight-proxy", "0.1.0"

// Start runs the batching loop with room for queue events.
func (s *Sink) Start(queue int) {
	if s.FlushEvery <= 0 {
		s.FlushEvery = time.Second
	}
	if s.MaxBatch <= 0 || s.MaxBatch > 500 {
		s.MaxBatch = 500
	}
	if s.Retries < 0 {
		s.Retries = 0
	} else if s.Retries == 0 {
		s.Retries = 5
	}
	if s.Backoff <= 0 {
		s.Backoff = 500 * time.Millisecond
	}
	if s.Parallel <= 0 {
		s.Parallel = 8
	}
	if s.Client == nil {
		s.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	s.queue = make(chan item, queue)
	s.done = make(chan struct{})
	s.drops = map[string]int64{}
	go s.loop()
}

// Emit never blocks.
func (s *Sink) Emit(key string, ev Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.closed {
		select {
		case s.queue <- item{key, ev}:
			s.Metrics.event("queued")
			return
		default:
		}
	}
	s.Metrics.event("dropped")
	s.dropMu.Lock()
	s.drops[key]++
	s.dropMu.Unlock()
}

func (s *Sink) takeDropped(key string) int64 {
	s.dropMu.Lock()
	defer s.dropMu.Unlock()
	n := s.drops[key]
	delete(s.drops, key)
	return n
}

// Close stops accepting events, sends what is queued and waits up to ctx.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.queue)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Sink) loop() {
	defer close(s.done)
	pending := map[string][]Event{}
	sem := make(chan struct{}, s.Parallel)
	var wg sync.WaitGroup
	send := func(key string, events []Event) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			s.send(key, events)
		}()
	}
	flushAll := func() {
		for key, events := range pending {
			send(key, events)
			delete(pending, key)
		}
	}
	tick := time.NewTicker(s.FlushEvery)
	defer tick.Stop()
	for {
		select {
		case it, ok := <-s.queue:
			if !ok {
				flushAll()
				wg.Wait()
				return
			}
			pending[it.key] = append(pending[it.key], it.ev)
			if len(pending[it.key]) >= s.MaxBatch {
				send(it.key, pending[it.key])
				delete(pending, it.key)
			}
		case <-tick.C:
			flushAll()
		}
	}
}

type batch struct {
	SchemaVersion int     `json:"schema_version"`
	BatchID       string  `json:"batch_id"`
	SentAt        string  `json:"sent_at"`
	SDK           sdkInfo `json:"sdk"`
	Dropped       int64   `json:"dropped"`
	Events        []Event `json:"events"`
}

type sdkInfo struct {
	Name             string `json:"name"`
	Version          string `json:"version"`
	Framework        string `json:"framework"`
	FrameworkVersion string `json:"framework_version"`
}

// send posts one batch with retries on 429, 5xx and network errors. 4xx other than 429
// means the batch or the key is wrong: it is dropped, retrying would not help.
func (s *Sink) send(key string, events []Event) {
	id, _ := uuid.NewV7()
	b := batch{
		SchemaVersion: 1,
		BatchID:       id.String(),
		SDK:           sdkInfo{Name: sdkName, Version: sdkVersion, Framework: "bot-api-proxy", FrameworkVersion: sdkVersion},
		Dropped:       s.takeDropped(key),
		Events:        events,
	}
	pause := s.Backoff
	for attempt := 0; ; attempt++ {
		b.SentAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		status, retryAfter, err := s.post(key, b)
		switch {
		case err == nil && status == http.StatusAccepted:
			s.Metrics.batch("accepted")
			return
		case err == nil && status != http.StatusTooManyRequests && status != http.StatusConflict && status < 500:
			s.Metrics.batch("rejected")
			s.Logger.Warn("collector rejected a batch", "status", status, "key", keyPrefix(key), "events", len(events))
			return
		}
		if attempt >= s.Retries {
			s.Metrics.batch("failed")
			s.Logger.Warn("batch dropped after retries", "status", status, "err", err, "key", keyPrefix(key), "events", len(events))
			return
		}
		wait := pause
		if retryAfter > wait {
			wait = retryAfter
		}
		time.Sleep(wait)
		pause *= 2
	}
}

func (s *Sink) post(key string, b batch) (int, time.Duration, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return 0, 0, err
	}
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(s.Collector, "/")+"/v1/batch", &body)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("User-Agent", sdkName+"/"+sdkVersion)
	resp, err := s.Client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("collector: %w", err)
	}
	defer resp.Body.Close()
	var retry time.Duration
	if v, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && v > 0 {
		retry = min(time.Duration(v)*time.Second, 30*time.Second)
	}
	return resp.StatusCode, retry, nil
}

// keyPrefix is the public part of an API key, safe for logs: bs_live_ab12cd34.
func keyPrefix(key string) string {
	parts := strings.SplitN(key, "_", 4)
	if len(parts) < 3 {
		return "invalid"
	}
	return strings.Join(parts[:3], "_")
}
