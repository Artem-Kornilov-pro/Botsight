package botsight

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// sender delivers events in batches (Botsight.md, 8.8). Enqueue never blocks: a full
// queue drops the event and counts it; the count goes in the "dropped" field of the next
// batch. A batch is retried with the same batch_id, so the collector accepts it once.
type sender struct {
	c     *Client
	queue chan event
	done  chan struct{}
	stop  chan struct{}
	once  sync.Once
	mu    sync.RWMutex // enqueue reads, close writes: no send on a closed queue
	shut  bool

	dropped atomic.Int64 // not reported yet
	lost    atomic.Int64 // in total, for Client.Dropped
}

const (
	maxAttempts = 6
	maxRetry    = 30 * time.Second
)

func newSender(c *Client) *sender {
	s := &sender{c: c, queue: make(chan event, c.cfg.queue), done: make(chan struct{}), stop: make(chan struct{})}
	go s.loop()
	return s
}

func (s *sender) enqueue(ev event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.shut {
		select {
		case s.queue <- ev:
			return
		default:
		}
	}
	s.dropped.Add(1)
	s.lost.Add(1)
}

func (s *sender) close(ctx context.Context) error {
	s.mu.Lock()
	if !s.shut {
		s.shut = true
		close(s.queue)
	}
	s.mu.Unlock()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		s.once.Do(func() { close(s.stop) }) // stop retrying, give up what is left
		return ctx.Err()
	}
}

func (s *sender) loop() {
	defer close(s.done)
	var batch []event
	tick := time.NewTicker(s.c.cfg.flush)
	defer tick.Stop()
	flush := func() {
		if len(batch) > 0 {
			s.deliver(batch)
			batch = nil
		}
	}
	for {
		select {
		case ev, ok := <-s.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= s.c.cfg.batch {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

type wireBatch struct {
	SchemaVersion int            `json:"schema_version"`
	BatchID       string         `json:"batch_id"`
	SentAt        string         `json:"sent_at"`
	SDK           map[string]any `json:"sdk"`
	Dropped       int64          `json:"dropped"`
	Events        []event        `json:"events"`
}

func (s *sender) deliver(events []event) {
	// custom events tracked before the bot was seen get its id now; without it the
	// collector would reject them
	botID := s.c.botID.Load()
	kept := events[:0]
	for _, ev := range events {
		if ev.BotID == 0 {
			ev.BotID = botID
		}
		if ev.BotID > 0 {
			kept = append(kept, ev)
		} else {
			s.lost.Add(1)
		}
	}
	if len(kept) == 0 {
		return
	}
	b := wireBatch{
		SchemaVersion: 1,
		BatchID:       uuid7(time.Now()),
		SDK:           map[string]any{"name": "botsight-go", "version": Version, "framework": s.c.cfg.framework, "framework_version": s.c.cfg.frameworkV},
		Dropped:       s.dropped.Swap(0),
		Events:        kept,
	}
	for attempt := 0; ; attempt++ {
		b.SentAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
		status, retryAfter, err := s.post(b)
		switch {
		case err == nil && status >= 200 && status < 300:
			return
		case err == nil && status == http.StatusUnauthorized:
			s.c.cfg.logger.Error("botsight: invalid or revoked API key (401), delivery stopped until restart")
			s.c.off.Store(true)
			s.lost.Add(int64(len(kept)))
			return
		case err == nil && status != 408 && status != 409 && status != 429 && status < 500:
			s.c.cfg.logger.Warn("botsight: collector rejected a batch", "status", status, "events", len(kept))
			s.lost.Add(int64(len(kept)))
			return
		}
		if attempt+1 >= maxAttempts {
			s.c.cfg.logger.Warn("botsight: batch dropped after retries", "status", status, "err", err, "events", len(kept))
			s.lost.Add(int64(len(kept)))
			s.dropped.Add(int64(len(kept)))
			return
		}
		// full jitter backoff, at least what the collector asked for
		wait := time.Duration(rand.Int64N(int64(min(maxRetry, 500*time.Millisecond<<attempt)) + 1))
		wait = max(wait, retryAfter)
		select {
		case <-time.After(wait):
		case <-s.stop:
			s.lost.Add(int64(len(kept)))
			return
		}
	}
}

func (s *sender) post(b wireBatch) (int, time.Duration, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return 0, 0, err
	}
	var body bytes.Buffer
	zw := gzip.NewWriter(&body)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	req, err := http.NewRequest(http.MethodPost, strings.TrimSuffix(s.c.cfg.endpoint, "/")+"/v1/batch", &body)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+s.c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("User-Agent", "botsight-go/"+Version)
	resp, err := s.c.cfg.delivery.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	var retry time.Duration
	if v, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && v > 0 {
		retry = min(time.Duration(v)*time.Second, maxRetry)
	}
	return resp.StatusCode, retry, nil
}
