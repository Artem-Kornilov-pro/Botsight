package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ingest"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func (h *harness) sdkPost(path, key string, body any) (int, map[string]any) {
	h.t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", h.srv.URL+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestBroadcastEndpoints(t *testing.T) {
	requireEnv(t)
	bc := &ingest.Broadcasts{Store: store.New(env.Pool), Redis: env.Redis, Lease: time.Minute, Recheck: time.Hour}
	h := newHarness(t, options{broadcasts: bc})
	ctx := context.Background()

	// nothing to send: the first poll asks PostgreSQL, the next ones only Redis
	if code, _ := h.sdkPost("/v1/broadcasts/lease", h.key, map[string]int{"limit": 10}); code != http.StatusNoContent {
		t.Fatalf("idle: %d", code)
	}
	b, err := h.store.CreateBroadcast(ctx, h.project.ID, "Привет!", "HTML", []byte(`{}`), []int64{11, 12, 13}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := h.sdkPost("/v1/broadcasts/lease", h.key, nil); code != http.StatusNoContent {
		t.Fatalf("without the flag the recheck interval holds: %d", code)
	}
	// the api sets the flag on create
	env.Redis.Set(ctx, store.ActiveBroadcastsKey(h.project.ID), 1, 0)
	code, lease := h.sdkPost("/v1/broadcasts/lease", h.key, map[string]int{"limit": 2})
	if code != http.StatusOK || lease["text"] != "Привет!" || lease["parse_mode"] != "HTML" ||
		len(lease["chat_ids"].([]any)) != 2 || lease["lease_seconds"] != 60.0 || lease["broadcast_id"] != float64(b.ID) {
		t.Fatalf("%d %v", code, lease)
	}

	report := map[string]any{"broadcast_id": b.ID, "results": []map[string]any{
		{"chat_id": 11, "status": "sent"}, {"chat_id": 12, "status": "blocked", "error": "bot_blocked"},
	}}
	if code, out := h.sdkPost("/v1/broadcasts/report", h.key, report); code != 200 || out["updated"] != 2.0 {
		t.Fatalf("report: %d %v", code, out)
	}
	bad := map[string]any{"broadcast_id": b.ID, "results": []map[string]any{{"chat_id": 13, "status": "pending"}}}
	if code, _ := h.sdkPost("/v1/broadcasts/report", h.key, bad); code != http.StatusBadRequest {
		t.Fatalf("pending is not a result: %d", code)
	}
	if code, _ := h.sdkPost("/v1/broadcasts/lease", "bs_live_00000000_wrong", nil); code != http.StatusUnauthorized {
		t.Fatalf("bad key: %d", code)
	}

	code, lease = h.sdkPost("/v1/broadcasts/lease", h.key, nil)
	if code != 200 || len(lease["chat_ids"].([]any)) != 1 {
		t.Fatalf("%d %v", code, lease)
	}
	h.sdkPost("/v1/broadcasts/report", h.key, map[string]any{"broadcast_id": b.ID, "results": []map[string]any{{"chat_id": 13, "status": "sent"}}})

	// all done: the broadcast finishes and the flag goes away
	if code, _ := h.sdkPost("/v1/broadcasts/lease", h.key, nil); code != http.StatusNoContent {
		t.Fatalf("finished: %d", code)
	}
	if n, _ := env.Redis.Exists(ctx, store.ActiveBroadcastsKey(h.project.ID)).Result(); n != 0 {
		t.Fatal("flag left after the last broadcast")
	}
	got, _ := h.store.Broadcast(ctx, h.project.ID, b.ID)
	if got.Status != "done" || got.Stats.Sent != 2 || got.Stats.Blocked != 1 {
		t.Fatalf("%+v", got)
	}
}
