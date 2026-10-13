package event

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// The shared contract fixture (Botsight.md, 9.5) must parse without rejected events.
func TestContractFixture(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	body, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "..", "docs", "contract", "batch_v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseBatch(body)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture is dated 2026-10-13; normalize as if it arrived shortly after.
	n := NewNormalizer(42, b, time.Date(2026, 10, 13, 0, 20, 0, 0, time.UTC))
	types := map[string]int{}
	for _, raw := range b.Events {
		rec, text, err := n.Normalize(raw)
		if err != nil {
			t.Fatalf("fixture event rejected: %v\n%s", err, raw)
		}
		if rec.ProjectID != 42 || rec.BotID != 7012345678 || text != nil {
			t.Fatalf("unexpected record %+v", rec)
		}
		types[rec.Type]++
		switch rec.Name {
		case "message":
			if rec.Type == "update" && (rec.Command != "start" || rec.StartParam != "vk_ads_oct" || rec.UserLang != "ru" || !rec.HasUsername) {
				t.Errorf("update fields lost: %+v", rec)
			}
		case "my_chat_member":
			if rec.MemberStatus != "kicked" {
				t.Errorf("member_status lost: %+v", rec)
			}
		case "sendDocument":
			if rec.OK || rec.ErrorCode != 403 || rec.Error != "bot_blocked" {
				t.Errorf("outgoing fields lost: %+v", rec)
			}
		case "purchase":
			if rec.PropsNum["amount"] != 990 || rec.PropsStr["plan"] != "pro" {
				t.Errorf("props lost: %+v", rec)
			}
		case "bot_started":
			if rec.PropsStr["sdk_version"] != "0.1.0" {
				t.Errorf("lifecycle props lost: %+v", rec)
			}
		}
	}
	for _, typ := range []string{"update", "step", "outgoing", "custom", "lifecycle"} {
		if types[typ] == 0 {
			t.Errorf("fixture has no %s events", typ)
		}
	}
}

func TestParseBatchErrors(t *testing.T) {
	ev := `{"event_id":"0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81"}`
	cases := map[string]string{
		"json":    `{`,
		"version": `{"schema_version":2,"batch_id":"0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d80","events":[` + ev + `]}`,
		"id":      `{"schema_version":1,"batch_id":"nope","events":[` + ev + `]}`,
		"empty":   `{"schema_version":1,"batch_id":"0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d80","events":[]}`,
		"big":     `{"schema_version":1,"batch_id":"0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d80","events":[` + strings.Repeat(ev+",", 500) + ev + `]}`,
	}
	for name, body := range cases {
		if _, err := ParseBatch([]byte(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func event(fields map[string]any) json.RawMessage {
	base := map[string]any{
		"event_id": "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81",
		"type":     "update",
		"name":     "message",
		"ts":       "2026-10-13T00:12:45.123Z",
		"bot_id":   7012345678,
		"user_id":  123,
		"chat_id":  123,
	}
	for k, v := range fields {
		if v == nil {
			delete(base, k)
		} else {
			base[k] = v
		}
	}
	b, _ := json.Marshal(base)
	return b
}

var received = time.Date(2026, 10, 13, 0, 13, 0, 0, time.UTC)

func normalizer(sentAt string) *Normalizer {
	return NewNormalizer(1, &Batch{SentAt: sentAt}, received)
}

func TestNormalizeRejects(t *testing.T) {
	n := normalizer("2026-10-13T00:12:59.000Z")
	cases := map[string]map[string]any{
		"event_id": {"event_id": "x"},
		"type":     {"type": "pageview"},
		"name":     {"name": ""},
		"bot_id":   {"bot_id": 0},
		"user_id":  {"user_id": -1},
		"ts":       {"ts": "yesterday"},
		"old":      {"ts": "2026-10-01T00:00:00Z"},
		"size":     {"text": strings.Repeat("x", MaxEventBytes)},
	}
	for name, f := range cases {
		if _, _, err := n.Normalize(event(f)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, _, err := n.Normalize(json.RawMessage(`[1,2]`)); err == nil {
		t.Error("non-object accepted")
	}
}

func TestNormalizeTimeAndSkew(t *testing.T) {
	rec, _, err := normalizer("2026-10-13T00:12:59.000Z").Normalize(event(nil))
	if err != nil || rec.TS != time.Date(2026, 10, 13, 0, 12, 45, 123e6, time.UTC).UnixMilli() || rec.ClockSkewed {
		t.Fatalf("%+v %v", rec, err)
	}
	if rec.ReceivedAt != received.UnixMilli() {
		t.Fatal("received_at not set")
	}

	// the bot clock is two days behind: every event gets received_at
	rec, _, err = normalizer("2026-10-11T00:12:59Z").Normalize(event(map[string]any{"ts": "2026-10-11T00:12:45Z"}))
	if err != nil || !rec.ClockSkewed || rec.TS != received.UnixMilli() {
		t.Fatalf("skew not handled: %+v %v", rec, err)
	}

	// one event from the future with a sane batch clock
	rec, _, err = normalizer("2026-10-13T00:12:59Z").Normalize(event(map[string]any{"ts": "2026-10-14T00:00:00Z"}))
	if err != nil || !rec.ClockSkewed || rec.TS != received.UnixMilli() {
		t.Fatalf("future event: %+v %v", rec, err)
	}
}

func TestNormalizeTextAndLimits(t *testing.T) {
	n := normalizer("2026-10-13T00:12:59Z")
	rec, text, err := n.Normalize(event(map[string]any{
		"text":        "привет",
		"state":       "Form:name",
		"handled":     true,
		"start_param": strings.Repeat("я", 100),
		"duration_ms": -5,
		"error_code":  1e9,
		"props":       map[string]any{"ok": true, "n": 1.5, "s": "x", "obj": map[string]int{"a": 1}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if text == nil || text.Text != "привет" || text.State != "Form:name" || !text.Handled || text.EventID != rec.EventID {
		t.Fatalf("text: %+v", text)
	}
	if len([]rune(rec.StartParam)) != 64 || rec.DurationMS != 0 || rec.ErrorCode != 65535 {
		t.Fatalf("limits: %+v", rec)
	}
	if rec.PropsStr["ok"] != "true" || rec.PropsNum["n"] != 1.5 || rec.PropsStr["s"] != "x" || len(rec.PropsStr)+len(rec.PropsNum) != 3 {
		t.Fatalf("props: %+v %+v", rec.PropsStr, rec.PropsNum)
	}

	// texts are taken only from update events
	_, text, _ = n.Normalize(event(map[string]any{"type": "custom", "name": "x", "text": "secret"}))
	if text != nil {
		t.Fatal("text taken from a custom event")
	}
}

func TestKey(t *testing.T) {
	if string(Key(42, 7, 7)) != "42:7" || string(Key(42, 0, -100500)) != "42:-100500" || string(Key(42, 0, 0)) != "42:0" {
		t.Fatal("unexpected keys")
	}
}

func TestTruncate(t *testing.T) {
	if truncate("abcdef", 3) != "abc" || truncate("яяяя", 2) != "яя" || truncate("ab", 5) != "ab" {
		t.Fatal("truncate")
	}
}
