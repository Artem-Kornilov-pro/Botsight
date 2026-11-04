package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

// Broadcast endpoints for the SDK (Botsight.md, 22). The SDK already talks to the
// collector with its API key, so it gets its work here: lease recipients, send the
// messages from inside the bot, report the results.

// BroadcastStore is the PostgreSQL queue of recipients.
type BroadcastStore interface {
	LeaseRecipients(ctx context.Context, projectID int32, limit int, lease time.Duration, maxAttempts int) (*store.Lease, bool, error)
	ReportResults(ctx context.Context, projectID, broadcastID int32, results []store.DeliveryResult) (int64, error)
}

type Broadcasts struct {
	Store BroadcastStore
	Redis redis.UniversalClient
	// Lease: how long recipients belong to one SDK instance; it sends at ~25 messages a
	// second, so 100 recipients take 4 s and 2 minutes leave a wide margin.
	Lease       time.Duration
	MaxAttempts int // leases without a report before a recipient is failed
	// Recheck: without the Redis flag, PostgreSQL is still asked this often per project,
	// so a lost flag delays a broadcast and never loses it.
	Recheck time.Duration

	mu      sync.Mutex
	checked map[int32]time.Time
}

const maxLease = 500

func (b *Broadcasts) defaults() {
	if b.Lease <= 0 {
		b.Lease = 2 * time.Minute
	}
	if b.MaxAttempts <= 0 {
		b.MaxAttempts = 3
	}
	if b.Recheck <= 0 {
		b.Recheck = time.Minute
	}
	if b.checked == nil {
		b.checked = map[int32]time.Time{}
	}
}

// idle reports, without PostgreSQL, that the project has nothing to send: the flag is
// absent and the database was asked recently.
func (b *Broadcasts) idle(ctx context.Context, projectID int32) bool {
	n, err := b.Redis.Exists(ctx, store.ActiveBroadcastsKey(projectID)).Result()
	if err != nil || n > 0 {
		return false // Redis down: ask PostgreSQL rather than stall broadcasts
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if time.Since(b.checked[projectID]) < b.Recheck {
		return true
	}
	b.checked[projectID] = time.Now()
	if len(b.checked) > 100_000 { // forget old checks, a recheck is cheap
		b.checked = map[int32]time.Time{projectID: time.Now()}
	}
	return false
}

// authorize checks the API key like a batch request.
func (c *Collector) authorize(w http.ResponseWriter, r *http.Request) (Project, bool) {
	key, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized", "Authorization: Bearer <api key> required")
		return Project{}, false
	}
	project, err := c.keys.Lookup(r.Context(), strings.TrimSpace(key))
	if errors.Is(err, ErrInvalidKey) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "invalid or revoked API key")
		return Project{}, false
	}
	if err != nil {
		c.log.Error("key lookup failed", "err", err)
		w.Header().Set("Retry-After", "5")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cannot check the API key now")
		return Project{}, false
	}
	return project, true
}

type leaseResponse struct {
	BroadcastID  int32     `json:"broadcast_id"`
	Text         string    `json:"text"`
	ParseMode    string    `json:"parse_mode"`
	ChatIDs      []int64   `json:"chat_ids"`
	LeaseSeconds int       `json:"lease_seconds"`
	LeasedUntil  time.Time `json:"leased_until"`
}

// leaseBroadcast: 200 with recipients, or 204 when there is nothing to send.
func (c *Collector) leaseBroadcast(w http.ResponseWriter, r *http.Request) {
	b := c.broadcasts
	project, ok := c.authorize(w, r)
	if !ok {
		return
	}
	var req struct {
		Limit int `json:"limit"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "bad_request", "body is {\"limit\": n}")
		return
	}
	if req.Limit <= 0 || req.Limit > maxLease {
		req.Limit = 100
	}
	if b.idle(r.Context(), project.ID) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	lease, active, err := b.Store.LeaseRecipients(r.Context(), project.ID, req.Limit, b.Lease, b.MaxAttempts)
	if err != nil {
		c.log.Error("lease recipients", "project_id", project.ID, "err", err)
		w.Header().Set("Retry-After", "10")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cannot lease recipients now")
		return
	}
	if !active {
		_ = b.Redis.Del(r.Context(), store.ActiveBroadcastsKey(project.ID)).Err()
	}
	if lease == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, leaseResponse{
		BroadcastID: lease.BroadcastID, Text: lease.Text, ParseMode: lease.ParseMode, ChatIDs: lease.ChatIDs,
		LeaseSeconds: int(b.Lease.Seconds()), LeasedUntil: lease.Until,
	})
}

var reportStatuses = map[string]bool{"sent": true, "blocked": true, "failed": true}

// reportBroadcast records results; repeating a report changes nothing.
func (c *Collector) reportBroadcast(w http.ResponseWriter, r *http.Request) {
	project, ok := c.authorize(w, r)
	if !ok {
		return
	}
	var req struct {
		BroadcastID int32 `json:"broadcast_id"`
		Results     []struct {
			ChatID int64  `json:"chat_id"`
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", "invalid JSON")
		return
	}
	if req.BroadcastID <= 0 || len(req.Results) == 0 || len(req.Results) > maxLease {
		writeError(w, http.StatusBadRequest, "bad_request", "broadcast_id and 1 to 500 results are required")
		return
	}
	results := make([]store.DeliveryResult, 0, len(req.Results))
	for _, res := range req.Results {
		if !reportStatuses[res.Status] {
			writeError(w, http.StatusBadRequest, "bad_request", "status is sent, blocked or failed")
			return
		}
		results = append(results, store.DeliveryResult{ChatID: res.ChatID, Status: res.Status, Error: truncate(res.Error, 64)})
	}
	n, err := c.broadcasts.Store.ReportResults(r.Context(), project.ID, req.BroadcastID, results)
	if err != nil {
		c.log.Error("report results", "project_id", project.ID, "err", err)
		w.Header().Set("Retry-After", "10")
		writeError(w, http.StatusServiceUnavailable, "unavailable", "cannot save results now")
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"updated": n})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
