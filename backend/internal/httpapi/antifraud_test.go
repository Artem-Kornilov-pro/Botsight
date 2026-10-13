package httpapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
)

func seedRisk(t *testing.T, pid int64, at time.Time) {
	t.Helper()
	ctx := context.Background()
	src, err := env.ClickHouse.PrepareBatch(ctx,
		`INSERT INTO source_risk (project_id, source, users, suspicious, avg_score, reasons, updated_at)`)
	if err != nil {
		t.Fatal(err)
	}
	_ = src.Append(uint32(pid), "promo_farm", uint32(40), uint32(38), float32(0.66),
		map[string]uint32{"fast_steps": 38, "synced_timing": 37}, at)
	_ = src.Append(uint32(pid), "ch_tech", uint32(200), uint32(1), float32(0.01), map[string]uint32{"fast_steps": 1}, at)
	if err := src.Send(); err != nil {
		t.Fatal(err)
	}
	users, err := env.ClickHouse.PrepareBatch(ctx,
		`INSERT INTO user_risk (project_id, user_id, source, joined, score, reasons, updated_at)`)
	if err != nil {
		t.Fatal(err)
	}
	_ = users.Append(uint32(pid), uint64(9_100_000_000_000_000_001), "promo_farm", at.Add(-time.Hour), float32(0.7),
		[]string{"fast_steps", "synced_timing"}, at)
	_ = users.Append(uint32(pid), uint64(9_100_000_002), "promo_farm", at.Add(-time.Hour), float32(0.5),
		[]string{"fast_steps", "start_spike"}, at)
	_ = users.Append(uint32(pid), uint64(123), "ch_tech", at.Add(-2*time.Hour), float32(0.5),
		[]string{"fast_steps", "fresh_accounts"}, at)
	if err := users.Send(); err != nil {
		t.Fatal(err)
	}
}

func TestAntifraudEndpoints(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	pid := c.createProject("антифрод").num("project", "id")
	p := path("/v1/projects/%d/antifraud", pid)

	empty := c.expect(http.StatusOK, "GET", p, nil)
	if empty.get("updated_at") != nil || len(empty.list("sources")) != 0 {
		t.Fatalf("before any run: %v", empty.body)
	}

	old := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	seedRisk(t, pid, old)
	latest := old.Add(time.Hour)
	seedRisk(t, pid, latest)
	// a user flagged only in the old run must not show up
	if err := env.ClickHouse.Exec(context.Background(),
		`INSERT INTO user_risk (project_id, user_id, source, joined, score, reasons, updated_at)
		 VALUES (?, 777, 'old', now64(3), 0.9, ['fast_steps'], ?)`, uint32(pid), old); err != nil {
		t.Fatal(err)
	}

	r := c.expect(http.StatusOK, "GET", p, nil)
	sources := r.list("sources")
	if len(sources) != 2 || sources[0]["source"] != "promo_farm" || sources[0]["share"] != 0.95 ||
		r.num("users") != 240 || r.num("suspicious") != 39 || r.num("window_hours") != 24 {
		t.Fatalf("summary: %v", r.body)
	}
	if texts, _ := r.get("reason_texts").(map[string]any); texts["synced_timing"] == nil {
		t.Fatalf("reason texts: %v", r.body)
	}

	users := c.expect(http.StatusOK, "GET", p+"/users", nil).list("users")
	if len(users) != 3 || users[0]["user_id"] != "9100000000000000001" || users[0]["score"].(float64) < 0.69 {
		t.Fatalf("users: %v", users)
	}
	farm := c.expect(http.StatusOK, "GET", p+"/users?source=promo_farm&limit=1", nil).list("users")
	if len(farm) != 1 || farm[0]["source"] != "promo_farm" {
		t.Fatalf("filtered: %v", farm)
	}
	if none := c.expect(http.StatusOK, "GET", p+"/users?source=", nil).list("users"); len(none) != 0 {
		t.Fatalf("empty source label: %v", none)
	}
	c.expect(http.StatusBadRequest, "GET", p+"/users?limit=0", nil)

	stranger, _ := h.newUser()
	stranger.expect(http.StatusNotFound, "GET", p, nil)
}

func TestAntifraudWithoutClickHouse(t *testing.T) {
	h := newHarnessDeps(t, func(d *httpapi.Deps) { d.ClickHouse = nil })
	c, _ := h.newUser()
	p := path("/v1/projects/%d/antifraud", c.createProject("x").num("project", "id"))
	c.expect(http.StatusServiceUnavailable, "GET", p, nil)
	c.expect(http.StatusServiceUnavailable, "GET", p+"/users", nil)
}
