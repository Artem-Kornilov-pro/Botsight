package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
)

const (
	eventA = "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d01"
	eventB = "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d02"
	eventC = "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d03"
)

func seedTopic(t *testing.T, projectID int64, topicID uint32, name string, examples []string, messages uint32) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	batch, err := env.ClickHouse.PrepareBatch(ctx, `INSERT INTO topics (project_id, topic_id, name, description,
		keywords, size, messages_7d, messages_prev_7d, share_7d, share_prev_7d, growth, example_event_ids,
		centroid, active, named_by, updated_at)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(uint32(projectID), topicID, name, "Пользователи пишут об этом.",
		[]string{"код", "смс"}, uint32(10), messages, uint32(2), 0.6, 0.1, 6.0, examples,
		[]float32{0.1, 0.2}, uint8(1), "keywords", now); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func seedText(t *testing.T, projectID int64, eventID, text string) {
	t.Helper()
	if err := env.ClickHouse.Exec(context.Background(),
		`INSERT INTO messages_text (project_id, bot_id, event_id, ts, user_id, state, handled, text)
		 VALUES (?, 77, ?, now64(3), 1, '', 1, ?)`, uint32(projectID), eventID, text); err != nil {
		t.Fatal(err)
	}
}

func TestTopicsAndExamples(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	pid := c.createProject("темы").num("project", "id")
	other := c.createProject("другой").num("project", "id")
	p := path("/v1/projects/%d", pid)

	seedTopic(t, pid, 1, "Не приходит код", []string{eventA, eventB}, 60)
	seedTopic(t, pid, 2, "Оплата", []string{eventC}, 90)
	seedTopic(t, other, 3, "Чужая тема", []string{eventC}, 10)
	seedText(t, pid, eventA, "не приходит код")
	seedText(t, pid, eventB, "смс не пришла")
	seedText(t, pid, eventC, "не проходит оплата")

	topics := c.expect(http.StatusOK, "GET", p+"/topics", nil).list("topics")
	if len(topics) != 2 || topics[0]["name"] != "Оплата" || topics[1]["messages_7d"] != 60.0 {
		t.Fatalf("topics: %v", topics)
	}
	if kw, _ := topics[0]["keywords"].([]any); len(kw) != 2 || topics[0]["growth"] != 6.0 {
		t.Fatalf("topic fields: %v", topics[0])
	}

	msgs := c.expect(http.StatusOK, "GET", p+"/topics/1/examples", nil).list("messages")
	texts := map[string]bool{}
	for _, m := range msgs {
		texts[m["text"].(string)] = true
	}
	if len(msgs) != 2 || !texts["не приходит код"] || !texts["смс не пришла"] {
		t.Fatalf("examples: %v", msgs)
	}
	// a topic of another project is not reachable through this one
	c.expect(http.StatusNotFound, "GET", p+"/topics/3/examples", nil)
	c.expect(http.StatusNotFound, "GET", p+"/topics/999/examples", nil)

	stranger, _ := h.newUser()
	stranger.expect(http.StatusNotFound, "GET", p+"/topics", nil)
}

func TestAlerts(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	pid := c.createProject("алерты").num("project", "id")
	for i, a := range []struct{ kind, details string }{
		{"topic_trend", `{"topic_id": 1, "growth": 4.2}`},
		{"anomaly", `not json`},
	} {
		if err := env.ClickHouse.Exec(context.Background(),
			`INSERT INTO alerts (project_id, alert_id, kind, created_at, title, text, details)
			 VALUES (?, generateUUIDv4(), ?, now64(3) + ?, ?, 'текст', ?)`,
			uint32(pid), a.kind, i, "заголовок "+strconv.Itoa(i), a.details); err != nil {
			t.Fatal(err)
		}
	}
	alerts := c.expect(http.StatusOK, "GET", path("/v1/projects/%d/alerts", pid), nil).list("alerts")
	if len(alerts) != 2 || alerts[0]["kind"] != "anomaly" || alerts[1]["kind"] != "topic_trend" {
		t.Fatalf("alerts: %v", alerts)
	}
	if d, _ := alerts[1]["details"].(map[string]any); d["growth"] != 4.2 {
		t.Fatalf("details: %v", alerts[1])
	}
	if d, _ := alerts[0]["details"].(map[string]any); d == nil || len(d) != 0 {
		t.Fatalf("invalid details must become {}: %v", alerts[0])
	}
}

func TestAskProxy(t *testing.T) {
	var got struct {
		token string
		body  map[string]any
	}
	status, reply := http.StatusOK, `{"sql": "SELECT 1", "rows": [[1]], "columns": ["x"], "chart": "number"}`
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.token = r.Header.Get("X-Internal-Token")
		_ = json.NewDecoder(r.Body).Decode(&got.body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	defer ai.Close()

	rule := ratelimit.Rule{Name: "t:ask:" + strconv.FormatInt(time.Now().UnixNano(), 36), Limit: 3, Period: time.Minute}
	h := newHarness(t, func(o *httpapi.Options) {
		o.AIURL, o.AIToken, o.Limits.AskAccount = ai.URL, "internal-secret", rule
	})
	c, _ := h.newUser()
	pid := c.createProject("вопросы").num("project", "id")
	p := path("/v1/projects/%d/ask", pid)

	r := c.expect(http.StatusOK, "POST", p, map[string]string{"question": "сколько пользователей?"})
	if r.str("sql") != "SELECT 1" || got.token != "internal-secret" ||
		got.body["project_id"] != float64(pid) || got.body["question"] != "сколько пользователей?" {
		t.Fatalf("proxy: %v %+v", r.body, got)
	}

	status, reply = http.StatusUnprocessableEntity, `{"error": {"code": "invalid_sql", "message": "only events"}}`
	if r := c.expect(http.StatusUnprocessableEntity, "POST", p, map[string]string{"question": "удали всё"}); r.str("error", "code") != "invalid_sql" {
		t.Fatalf("%v", r.body)
	}
	status, reply = http.StatusUnauthorized, `{"error": {"code": "unauthorized", "message": "token"}}`
	if r := c.expect(http.StatusBadGateway, "POST", p, map[string]string{"question": "?"}); r.str("error", "code") != "ai_unavailable" {
		t.Fatalf("token mismatch must not leak as 401: %v", r.body)
	}
	if r := c.expect(http.StatusTooManyRequests, "POST", p, map[string]string{"question": "?"}); r.str("error", "code") != "rate_limited" {
		t.Fatalf("%v", r.body)
	}
	c.expect(http.StatusBadRequest, "POST", path("/v1/projects/%d/ask", pid), map[string]string{"question": ""})
}

func TestAIEndpointsOff(t *testing.T) {
	h := newHarnessDeps(t, func(d *httpapi.Deps) { d.ClickHouse = nil })
	c, _ := h.newUser()
	p := path("/v1/projects/%d", c.createProject("без аналитики").num("project", "id"))
	for _, route := range []string{"/topics", "/alerts", "/topics/1/examples"} {
		if r := c.expect(http.StatusServiceUnavailable, "GET", p+route, nil); r.str("error", "code") != "analytics_unavailable" {
			t.Fatalf("%s: %v", route, r.body)
		}
	}
	if r := c.expect(http.StatusServiceUnavailable, "POST", p+"/ask", map[string]string{"question": "?"}); r.str("error", "code") != "ai_unavailable" {
		t.Fatalf("%v", r.body)
	}
}
