package ingest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
)

// sdkBatchBody is a gzipped batch of n events in the mix of a real bot: updates with a
// user, FSM steps, Bot API calls and a manual event with props, like the Python SDK sends.
func sdkBatchBody(tb testing.TB, n int) []byte {
	tb.Helper()
	now := time.Now().UTC()
	events := make([]map[string]any, 0, n)
	for i := 0; len(events) < n; i++ {
		ts := now.Add(-time.Duration(n-i) * time.Second).Format("2006-01-02T15:04:05.000Z")
		user := int64(100_000_000 + i%300*7919)
		base := func(typ, name string) map[string]any {
			return map[string]any{
				"event_id": fmt.Sprintf("0192f6c8-4d1e-7a3b-9c55-%012x", i*4+len(events)%4),
				"type":     typ, "name": name, "ts": ts, "bot_id": 7012345678, "user_id": user, "chat_id": user,
			}
		}
		u := base("update", "message")
		u["chat_type"], u["content_type"], u["text_len"], u["state"] = "private", "text", 17, "Form:lab"
		u["handled"], u["duration_ms"] = true, 41
		u["user"] = map[string]any{"lang": "ru", "is_premium": false, "has_username": true}
		if i%10 == 0 {
			u["command"], u["start_param"] = "start", "vk_ads_oct"
		}
		s := base("step", "message")
		s["handler"], s["state_from"], s["state_to"], s["duration_ms"], s["error"] = "bot.handlers:cb_lab", "Form:lab", "Form:name", 12, nil
		o := base("outgoing", "sendMessage")
		o["user_id"], o["ok"], o["error_code"], o["error"], o["duration_ms"] = 0, true, 0, nil, 87
		events = append(events, u, s, o)
		if i%5 == 0 {
			c := base("custom", "lab_generated")
			c["props"] = map[string]any{"lab": "lab2", "queue_ms": 340, "gen_ms": 2150, "paid": true}
			events = append(events, c)
		}
	}
	raw, err := json.Marshal(map[string]any{
		"schema_version": 1, "batch_id": "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d80", "sent_at": now.Format(time.RFC3339Nano),
		"sdk":     map[string]any{"name": "botsight-python", "version": "0.1.0", "framework": "aiogram", "framework_version": "3.31.0"},
		"dropped": 0, "events": events[:n],
	})
	if err != nil {
		tb.Fatal(err)
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(raw)
	_ = zw.Close()
	return buf.Bytes()
}

// BenchmarkHotPath is the CPU work of one /v1/batch request without its network calls:
// gunzip with limits, the envelope, then every event validated, normalized and encoded
// as a Kafka record (Botsight.md, 10.5). One op is one batch of 500 events.
func BenchmarkHotPath(b *testing.B) {
	body := sdkBatchBody(b, 500)
	c := &Collector{now: time.Now}
	project := Project{ID: 42, CaptureText: false}
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := httptest.NewRequest("POST", "/v1/batch", bytes.NewReader(body))
		req.Header.Set("Content-Encoding", "gzip")
		buf, err := readBody(httptest.NewRecorder(), req)
		if err != nil {
			b.Fatal(err)
		}
		batch, err := event.ParseBatch(buf.Bytes())
		putBuffer(buf)
		if err != nil {
			b.Fatal(err)
		}
		records, accepted, _, _ := c.buildRecords(project, batch)
		if accepted != 500 || len(records) != 501 {
			b.Fatalf("accepted %d, records %d", accepted, len(records))
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/500, "ns/event")
}
