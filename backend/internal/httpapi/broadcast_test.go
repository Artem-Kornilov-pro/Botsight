package httpapi_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func TestBroadcasts(t *testing.T) {
	h := newHarness(t)
	owner, _ := h.newUser()
	pid := owner.createProject("рассылки").num("project", "id")
	p := path("/v1/projects/%d", pid)

	now := time.Now().UTC()
	msg := func(at time.Duration, user uint64, command, label string) chEvent {
		return chEvent{at: now.Add(-at), typ: "update", name: "message", user: user, chat: int64(user), command: command, param: label}
	}
	seedEvents(t, pid, []chEvent{
		msg(48*time.Hour, 301, "start", "vk"), msg(47*time.Hour, 302, "start", "vk"), msg(time.Hour, 303, "start", "tg"),
		{at: now.Add(-46 * time.Hour), typ: "custom", name: "purchase", user: 301},
		{at: now.Add(-time.Hour), typ: "outgoing", name: "sendMessage", chat: 302, code: 403, error: "bot_blocked"},
	})

	// 302 blocked the bot; buyers are left out
	segment := map[string]any{"active_days": 7, "not_reached": map[string]string{"kind": "custom", "value": "purchase"}}
	if r := owner.expect(http.StatusOK, "POST", p+"/broadcasts/preview", map[string]any{"segment": segment}); r.num("recipients") != 1 {
		t.Fatalf("preview: %v", r.body)
	}
	all := owner.expect(http.StatusOK, "POST", p+"/broadcasts/preview", map[string]any{"segment": map[string]any{}})
	if all.num("recipients") != 2 || all.num("segment", "active_days") != 30 {
		t.Fatalf("default segment: %v", all.body)
	}

	created := owner.expect(http.StatusCreated, "POST", p+"/broadcasts", map[string]any{
		"text": "<b>Скидка</b> только сегодня", "parse_mode": "HTML", "segment": segment})
	if created.num("total") != 1 || created.str("status") != "sending" || created.num("stats", "pending") != 1 {
		t.Fatalf("create: %v", created.body)
	}
	if n, _ := env.Redis.Exists(context.Background(), store.ActiveBroadcastsKey(int32(pid))).Result(); n != 1 {
		t.Fatal("the collector's flag is not set")
	}
	id := created.num("id")

	list := owner.expect(http.StatusOK, "GET", p+"/broadcasts", nil).list("broadcasts")
	if len(list) != 1 || list[0]["text"] != "<b>Скидка</b> только сегодня" {
		t.Fatalf("list: %v", list)
	}
	if got := owner.expect(http.StatusOK, "GET", path("%s/broadcasts/%d", p, id), nil); got.get("segment", "not_reached", "value") != "purchase" {
		t.Fatalf("get: %v", got.body)
	}

	for _, bad := range []map[string]any{
		{"text": "", "segment": map[string]any{}},
		{"text": "x", "parse_mode": "Markdown", "segment": map[string]any{}},
		{"text": "x", "segment": map[string]any{"active_days": 1000}},
		{"text": "x", "segment": map[string]any{"source": "bad label!"}},
		{"text": "x", "segment": map[string]any{"reached": map[string]string{"kind": "event", "value": "x"}}},
	} {
		owner.expect(http.StatusBadRequest, "POST", p+"/broadcasts", bad)
	}
	nobody := map[string]any{"text": "x", "segment": map[string]any{"source": "nobody_here"}}
	if r := owner.expect(http.StatusUnprocessableEntity, "POST", p+"/broadcasts", nobody); r.str("error", "code") != "empty_segment" {
		t.Fatal(r.body)
	}

	// a viewer sees broadcasts but cannot send or cancel
	viewer, viewerEmail := h.newUser()
	owner.expect(http.StatusCreated, "POST", p+"/members", map[string]string{"email": viewerEmail, "role": "viewer"})
	viewer.expect(http.StatusOK, "GET", p+"/broadcasts", nil)
	viewer.expect(http.StatusOK, "POST", p+"/broadcasts/preview", map[string]any{"segment": segment})
	viewer.expect(http.StatusForbidden, "POST", p+"/broadcasts", map[string]any{"text": "x", "segment": segment})
	viewer.expect(http.StatusForbidden, "POST", path("%s/broadcasts/%d/cancel", p, id), nil)

	if r := owner.expect(http.StatusOK, "POST", path("%s/broadcasts/%d/cancel", p, id), nil); r.str("status") != "canceled" {
		t.Fatal(r.body)
	}
	if r := owner.expect(http.StatusConflict, "POST", path("%s/broadcasts/%d/cancel", p, id), nil); r.str("error", "code") != "not_sending" {
		t.Fatal(r.body)
	}
	owner.expect(http.StatusNotFound, "GET", path("%s/broadcasts/%d", p, id+100000), nil)
	stranger, _ := h.newUser()
	stranger.expect(http.StatusNotFound, "GET", p+"/broadcasts", nil)
}
