package httpapi_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
)

type chEvent struct {
	at                       time.Time
	typ, name                string
	user                     uint64
	chat                     int64
	command, param           string
	handler, from, to, error string
	code                     uint16
}

func seedEvents(t *testing.T, pid int64, events []chEvent) {
	t.Helper()
	b, err := env.ClickHouse.PrepareBatch(context.Background(), `INSERT INTO events (project_id, bot_id, event_id, ts,
		received_at, type, name, user_id, chat_id, chat_type, command, start_param, handler, state_from, state_to,
		handled, ok, duration_ms, error_code, error)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		id, _ := uuid.NewV7()
		ok := uint8(1)
		if e.error != "" {
			ok = 0
		}
		if err := b.Append(uint32(pid), uint64(7012345678), id, e.at, e.at, e.typ, e.name, e.user, e.chat, "private",
			e.command, e.param, e.handler, e.from, e.to, uint8(1), ok, uint32(15), e.code, e.error); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
}

func TestAnalyticsEndpoints(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	pid := c.createProject("аналитика").num("project", "id")
	p := path("/v1/projects/%d", pid)

	day := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -10)
	date := func(days int) string { return day.AddDate(0, 0, days).Format(time.DateOnly) }
	hour := func(n int) time.Time { return day.Add(time.Duration(n) * time.Hour) }
	start := func(at time.Time, user uint64, label string) chEvent {
		return chEvent{at: at, typ: "update", name: "message", user: user, chat: int64(user), command: "start", param: label}
	}
	step := func(at time.Time, user uint64, handler, from, to string) chEvent {
		return chEvent{at: at, typ: "step", name: "message", user: user, chat: int64(user), handler: handler, from: from, to: to}
	}
	crash := step(hour(25), 203, "bot:y", "", "")
	crash.error = "ValueError"
	seedEvents(t, pid, []chEvent{
		start(hour(1), 201, "vk"), step(hour(1).Add(time.Minute), 201, "bot:a", "", "Form:a"),
		{at: hour(2), typ: "custom", name: "purchase", user: 201},
		start(hour(3), 202, "vk"), step(hour(3).Add(time.Minute), 202, "bot:a", "", "Form:a"),
		step(hour(3).Add(2*time.Minute), 202, "bot:help", "Form:a", "Form:a"),
		step(hour(3).Add(3*time.Minute), 202, "bot:x", "Form:a", "Form:a"),
		start(hour(24), 203, ""), crash,
		{at: hour(26), typ: "outgoing", name: "sendMessage", chat: 203, code: 403, error: "bot_blocked"},
	})
	period := "?from=" + date(0) + "&to=" + date(6)

	// overview, and the second identical request comes from the cache
	o := c.expect(http.StatusOK, "GET", p+"/overview"+period, nil)
	if o.num("users") != 3 || o.num("new_users") != 3 || o.num("blocked") != 1 || o.num("handler_errors") != 1 ||
		len(o.body["days"].([]any)) != 7 || o.header.Get("X-Cache") != "miss" {
		t.Fatalf("overview: %v", o.body)
	}
	if again := c.expect(http.StatusOK, "GET", p+"/overview"+period, nil); again.header.Get("X-Cache") != "hit" {
		t.Fatal("repeated overview is not cached")
	}
	if def := c.expect(http.StatusOK, "GET", p+"/overview", nil); len(def.body["days"].([]any)) != 30 || def.num("users") != 3 {
		t.Fatalf("default period: %v", def.body["range"])
	}

	// funnel with the source breakdown, any role may compute it
	funnel := map[string]any{"definition": map[string]any{
		"steps":     []map[string]string{{"kind": "command", "value": "/start"}, {"kind": "state", "value": "Form:a"}, {"kind": "custom", "value": "purchase"}},
		"breakdown": "source",
	}}
	f := c.expect(http.StatusOK, "POST", p+"/funnels/query"+period, funnel)
	steps := f.list("steps")
	if len(steps) != 3 || steps[0]["users"] != 3.0 || steps[1]["users"] != 2.0 || steps[2]["users"] != 1.0 || steps[2]["from_prev"] != 0.5 {
		t.Fatalf("funnel: %v", f.body)
	}
	if seg := f.list("breakdown"); len(seg) != 2 || seg[0]["source"] != "vk" {
		t.Fatalf("breakdown: %v", seg)
	}
	c.expect(http.StatusBadRequest, "POST", p+"/funnels/query", map[string]any{"definition": map[string]any{
		"steps": []map[string]string{{"kind": "command", "value": "start"}}}})

	// sources with the goal and the cost of a user: 1000 rub over 10 days, 7 of them in the period
	c.expect(http.StatusCreated, "POST", p+"/ad-spend", map[string]any{"source": "vk", "from": date(-3), "to": date(6), "amount_rub": 1000})
	c.expect(http.StatusCreated, "POST", p+"/ad-spend", map[string]any{"source": "tg_promo", "from": date(0), "to": date(0), "amount_rub": 50})
	s := c.expect(http.StatusOK, "GET", p+"/sources"+period+"&goal=custom:purchase", nil)
	src := s.list("sources")
	if len(src) != 3 || src[0]["source"] != "vk" || src[0]["users"] != 2.0 || src[0]["converted"] != 1.0 ||
		src[0]["spend_rub"] != 700.0 || src[0]["cost_per_user_rub"] != 350.0 || src[0]["cost_per_conversion_rub"] != 700.0 {
		t.Fatalf("sources: %v", src)
	}
	if src[1]["source"] != "" || src[1]["blocked"] != 1.0 || src[1]["spend_rub"] != nil {
		t.Fatalf("no label: %v", src[1])
	}
	if src[2]["source"] != "tg_promo" || src[2]["users"] != 0.0 || src[2]["spend_rub"] != 50.0 {
		t.Fatalf("spend without users: %v", src[2])
	}
	c.expect(http.StatusBadRequest, "GET", p+"/sources?goal=purchase", nil)

	// the graph skips neutral handlers in retries
	c.expect(http.StatusOK, "PATCH", p, map[string]any{"settings": map[string]any{"neutral_handlers": []string{"bot:help"}}})
	g := c.expect(http.StatusOK, "GET", p+"/fsm-graph"+period, nil)
	if retries := g.list("retries"); len(retries) != 1 || retries[0]["retries"] != 1.0 || len(g.list("edges")) != 1 {
		t.Fatalf("graph: %v", g.body)
	}

	e := c.expect(http.StatusOK, "GET", p+"/errors"+period, nil)
	if len(e.list("handlers")) != 1 || len(e.list("api")) != 1 || e.list("api")[0]["error"] != "bot_blocked" {
		t.Fatalf("errors: %v", e.body)
	}

	ret := c.expect(http.StatusOK, "GET", p+"/retention?weeks=4&tz=Europe/Moscow", nil)
	if ret.num("weeks") != 4 || len(ret.list("cohorts")) == 0 {
		t.Fatalf("retention: %v", ret.body)
	}
	c.expect(http.StatusBadRequest, "GET", p+"/retention?weeks=27", nil)

	// the timeline of 202 in two pages
	tl := c.expect(http.StatusOK, "GET", p+"/users/202/timeline?limit=3", nil)
	if len(tl.list("events")) != 3 || tl.str("next_cursor") == "" || tl.list("events")[0]["handler"] != "bot:x" {
		t.Fatalf("timeline: %v", tl.body)
	}
	rest := c.expect(http.StatusOK, "GET", p+"/users/202/timeline?limit=3&cursor="+url.QueryEscape(tl.str("next_cursor")), nil)
	if len(rest.list("events")) != 1 || rest.str("next_cursor") != "" || rest.list("events")[0]["start_param"] != "vk" {
		t.Fatalf("second page: %v", rest.body)
	}
	c.expect(http.StatusBadRequest, "GET", p+"/users/202/timeline?cursor=nope", nil)
	c.expect(http.StatusNotFound, "GET", p+"/users/abc/timeline", nil)

	for _, bad := range []string{"?tz=Mars/Base", "?tz=../../etc", "?from=2026-13-01", "?from=" + date(6) + "&to=" + date(0), "?from=2020-01-01"} {
		if r := c.expect(http.StatusBadRequest, "GET", p+"/overview"+bad, nil); r.str("error", "code") != "validation_failed" {
			t.Fatalf("%s: %v", bad, r.body)
		}
	}

	stranger, _ := h.newUser()
	stranger.expect(http.StatusNotFound, "GET", p+"/overview", nil)
	stranger.expect(http.StatusNotFound, "POST", p+"/funnels/query", funnel)
}

func TestAnalyticsWithoutClickHouse(t *testing.T) {
	h := newHarnessDeps(t, func(d *httpapi.Deps) { d.ClickHouse = nil })
	c, _ := h.newUser()
	p := path("/v1/projects/%d", c.createProject("x").num("project", "id"))
	for _, route := range []string{"/overview", "/fsm-graph", "/sources", "/retention", "/errors", "/users/1/timeline"} {
		if r := c.expect(http.StatusServiceUnavailable, "GET", p+route, nil); r.str("error", "code") != "analytics_unavailable" {
			t.Fatalf("%s: %v", route, r.body)
		}
	}
}
