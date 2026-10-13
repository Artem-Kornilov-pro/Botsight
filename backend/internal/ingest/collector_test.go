package ingest_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ingest"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

var (
	env      *testenv.Env
	envErr   error
	producer *kgo.Client
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	env, envErr = testenv.StartWith(ctx, testenv.Options{Postgres: true, Redis: true, Kafka: true})
	if envErr == nil {
		producer, envErr = kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(env.KafkaBrokers...)}, kafka.ProducerOptions()...)...)
	}
	if envErr == nil {
		envErr = kafka.EnsureTopics(ctx, producer, 1, kafka.Topics())
	}
	cancel()
	code := m.Run()
	if producer != nil {
		producer.Close()
	}
	if env != nil {
		env.Close()
	}
	os.Exit(code)
}

func requireEnv(t *testing.T) {
	t.Helper()
	if envErr == nil {
		return
	}
	if os.Getenv("CI") != "" {
		t.Fatalf("integration environment: %v", envErr)
	}
	t.Skipf("integration environment unavailable (Docker?): %v", envErr)
}

type harness struct {
	t        *testing.T
	srv      *httptest.Server
	store    *store.Store
	keys     *ingest.KeyCache
	project  store.Project
	key      string
	producer ingest.Producer
}

type options struct {
	rate, burst float64
	maxInflight int
	producer    ingest.Producer
	captureText bool
}

var accountSeq atomic.Int64

func newHarness(t *testing.T, o options) *harness {
	t.Helper()
	requireEnv(t)
	ctx := context.Background()
	st := store.New(env.Pool)
	account, err := st.CreateAccount(ctx, fmt.Sprintf("ingest-%d-%d@example.com", time.Now().UnixNano(), accountSeq.Add(1)), "x", "")
	if err != nil {
		t.Fatal(err)
	}
	key, err := auth.NewAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	project, _, err := st.CreateProject(ctx, "ingest", account.ID, store.NewKey{Prefix: key.Prefix, Hash: key.Hash})
	if err != nil {
		t.Fatal(err)
	}
	if o.captureText {
		if _, err := st.UpdateProject(ctx, project.ID, account.ID, nil, &store.ProjectSettings{CaptureText: true}); err != nil {
			t.Fatal(err)
		}
	}
	if o.rate == 0 {
		o.rate, o.burst = 1e6, 1e6
	}
	if o.producer == nil {
		o.producer = producer
	}
	reg := prometheus.NewRegistry()
	m := ingest.NewMetrics(reg)
	keys := ingest.NewKeyCache(st, time.Minute, time.Second, 1000, m)
	listenCtx, stopListen := context.WithCancel(ctx)
	go keys.ListenRevocations(listenCtx, env.Redis, slog.New(slog.NewTextHandler(io.Discard, nil)))
	c := ingest.New(ingest.Deps{
		Keys: keys, Gate: ingest.NewGate(env.Redis, o.rate, o.burst), Producer: o.producer, Bots: st,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Metrics: m, Gatherer: reg,
		Ready:  func(ctx context.Context) error { return env.Redis.Ping(ctx).Err() },
		Config: ingest.Config{MaxInflight: o.maxInflight},
	})
	srv := httptest.NewServer(c.Handler())
	t.Cleanup(func() {
		srv.Close()
		stopListen()
		c.Wait()
	})
	return &harness{t: t, srv: srv, store: st, keys: keys, project: project, key: key.Plain, producer: o.producer}
}

var eventSeq atomic.Int64

func uuid7() string {
	n := eventSeq.Add(1)
	return fmt.Sprintf("%08x-%04x-7%03x-8%03x-%012x", time.Now().Unix(), n>>16&0xffff, n&0xfff, n>>28&0xfff, time.Now().UnixNano()&0xffffffffffff)
}

func sdkEvent(fields map[string]any) map[string]any {
	ev := map[string]any{
		"event_id": uuid7(), "type": "update", "name": "message",
		"ts":     time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		"bot_id": 7012345678, "user_id": 42, "chat_id": 42,
		"chat_type": "private", "state": "", "text_len": 6, "content_type": "text",
		"user":    map[string]any{"lang": "ru", "is_premium": false, "has_username": true},
		"handled": true, "duration_ms": 3,
	}
	for k, v := range fields {
		ev[k] = v
	}
	return ev
}

func sdkBatch(events ...map[string]any) map[string]any {
	return map[string]any{
		"schema_version": 1,
		"batch_id":       uuid7(),
		"sent_at":        time.Now().UTC().Format(time.RFC3339Nano),
		"sdk":            map[string]any{"name": "botsight-python", "version": "0.1.0", "framework": "aiogram", "framework_version": "3.31.0"},
		"dropped":        3,
		"events":         events,
	}
}

func gz(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(b)
	zw.Close()
	return buf.Bytes()
}

type resp struct {
	status int
	header http.Header
	body   map[string]any
}

func (h *harness) post(body []byte, headers map[string]string) resp {
	h.t.Helper()
	req, _ := http.NewRequest("POST", h.srv.URL+"/v1/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Authorization", "Bearer "+h.key)
	for k, v := range headers {
		if v == "" {
			req.Header.Del(k)
		} else {
			req.Header.Set(k, v)
		}
	}
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer r.Body.Close()
	out := resp{status: r.StatusCode, header: r.Header}
	_ = json.NewDecoder(r.Body).Decode(&out.body)
	return out
}

func (h *harness) send(batch map[string]any) resp {
	h.t.Helper()
	b, _ := json.Marshal(batch)
	return h.post(gz(b), nil)
}

// consume returns every record of the project in a topic. It first asks Kafka for the
// end offset of each partition and reads up to it, so it sees everything already written
// however slow the broker is, and "nothing written" checks are exact.
func consume(t *testing.T, topic string, projectID int32) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.KafkaBrokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	remaining := map[int32]int64{} // partition -> end offset still to reach
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			t.Fatalf("end offset of %s/%d: %v", topic, o.Partition, o.Err)
		}
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})
	pid := []byte(strconv.Itoa(int(projectID)))
	var out []*kgo.Record
	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("reading %s: %d partitions not read to the end", topic, len(remaining))
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if end, ok := remaining[r.Partition]; ok && r.Offset+1 >= end {
				delete(remaining, r.Partition)
			}
			for _, hd := range r.Headers {
				if hd.Key == "project_id" && bytes.Equal(hd.Value, pid) {
					out = append(out, r)
				}
			}
		})
	}
	return out
}

func TestAcceptBatch(t *testing.T) {
	h := newHarness(t, options{})
	r := h.send(sdkBatch(
		sdkEvent(map[string]any{"command": "start", "start_param": "ch_tech", "text": "/start ch_tech"}),
		sdkEvent(map[string]any{"type": "step", "name": "message", "handler": "bot:start", "state_from": "", "state_to": "Form:name"}),
		sdkEvent(map[string]any{"type": "outgoing", "name": "sendMessage", "user_id": 0, "chat_id": 42, "ok": false, "error_code": 403, "error": "bot_blocked"}),
		sdkEvent(map[string]any{"type": "custom", "name": "purchase", "props": map[string]any{"amount": 990, "plan": "pro"}}),
		sdkEvent(map[string]any{"type": "lifecycle", "name": "bot_started", "user_id": 0, "chat_id": 0}),
	))
	if r.status != http.StatusAccepted || r.body["accepted"] != 5.0 || r.body["duplicate"] != false || r.body["rejected"] != 0.0 {
		t.Fatalf("%d %v", r.status, r.body)
	}

	events := consume(t, kafka.TopicEvents, h.project.ID)
	if len(events) != 5 {
		t.Fatalf("events in Kafka: %d", len(events))
	}
	for _, rec := range events {
		var e event.Record
		if err := json.Unmarshal(rec.Value, &e); err != nil {
			t.Fatal(err)
		}
		if e.ProjectID != uint32(h.project.ID) || e.ReceivedAt == 0 {
			t.Fatalf("bad record %+v", e)
		}
		// outgoing events have no user and are keyed by chat; lifecycle events have neither
		want := fmt.Sprintf("%d:42", h.project.ID)
		if e.Type == "lifecycle" {
			want = fmt.Sprintf("%d:0", h.project.ID)
		}
		if string(rec.Key) != want {
			t.Fatalf("%s key %q, want %q", e.Type, rec.Key, want)
		}
	}
	if texts := consume(t, kafka.TopicTexts, h.project.ID); len(texts) != 0 {
		t.Fatalf("texts written for a project without capture_text: %d", len(texts))
	}
	batches := consume(t, kafka.TopicBatches, h.project.ID)
	var meta event.BatchMeta
	if len(batches) != 1 || json.Unmarshal(batches[0].Value, &meta) != nil || meta.Dropped != 3 || meta.Events != 5 || meta.SDKVersion != "0.1.0" {
		t.Fatalf("batch meta: %+v", meta)
	}

	// the first event records the project's bot
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var botID *int64
		if err := env.Pool.QueryRow(context.Background(), "SELECT bot_id FROM projects WHERE id = $1", h.project.ID).Scan(&botID); err != nil {
			t.Fatal(err)
		}
		if botID != nil && *botID == 7012345678 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("projects.bot_id not recorded")
}

func TestCaptureText(t *testing.T) {
	h := newHarness(t, options{captureText: true})
	if r := h.send(sdkBatch(sdkEvent(map[string]any{"text": "не приходит код"}))); r.status != http.StatusAccepted {
		t.Fatalf("%d %v", r.status, r.body)
	}
	texts := consume(t, kafka.TopicTexts, h.project.ID)
	var txt event.Text
	if len(texts) != 1 || json.Unmarshal(texts[0].Value, &txt) != nil || txt.Text != "не приходит код" {
		t.Fatalf("texts: %v", texts)
	}
	if bytes.Contains(consume(t, kafka.TopicEvents, h.project.ID)[0].Value, []byte("не приходит")) {
		t.Fatal("text leaked into events.raw")
	}
}

func TestDuplicateAndInProgress(t *testing.T) {
	h := newHarness(t, options{})
	batch := sdkBatch(sdkEvent(nil), sdkEvent(nil))
	if r := h.send(batch); r.status != http.StatusAccepted {
		t.Fatalf("%d %v", r.status, r.body)
	}
	r := h.send(batch)
	if r.status != http.StatusAccepted || r.body["duplicate"] != true || r.body["accepted"] != 2.0 {
		t.Fatalf("retry: %d %v", r.status, r.body)
	}
	if n := len(consume(t, kafka.TopicEvents, h.project.ID)); n != 2 {
		t.Fatalf("events after a retry: %d", n)
	}

	other := sdkBatch(sdkEvent(nil))
	key := fmt.Sprintf("batch:{%d}:%s", h.project.ID, other["batch_id"])
	env.Redis.Set(context.Background(), key, "processing", time.Minute)
	if r := h.send(other); r.status != http.StatusConflict {
		t.Fatalf("in-progress batch: %d %v", r.status, r.body)
	}
}

func TestRateLimitPerProject(t *testing.T) {
	h := newHarness(t, options{rate: 10, burst: 600})
	events := make([]map[string]any, 500)
	for i := range events {
		events[i] = sdkEvent(nil)
	}
	if r := h.send(sdkBatch(events...)); r.status != http.StatusAccepted {
		t.Fatalf("%d %v", r.status, r.body)
	}
	r := h.send(sdkBatch(events...))
	if r.status != http.StatusTooManyRequests || r.header.Get("Retry-After") == "" {
		t.Fatalf("not limited: %d %v", r.status, r.body)
	}

	// another project has its own bucket
	other := newHarness(t, options{rate: 10, burst: 600})
	if r := other.send(sdkBatch(events...)); r.status != http.StatusAccepted {
		t.Fatalf("other project limited: %d %v", r.status, r.body)
	}
}

func TestAuthentication(t *testing.T) {
	h := newHarness(t, options{})
	body, _ := json.Marshal(sdkBatch(sdkEvent(nil)))
	for name, auth := range map[string]string{
		"missing":   "",
		"malformed": "Bearer nope",
		"wrong":     "Bearer " + h.key[:len(h.key)-1] + "x",
		"unknown":   "Bearer bs_live_zzzzzzzz_" + h.key[len(h.key)-40:],
	} {
		if r := h.post(gz(body), map[string]string{"Authorization": auth}); r.status != http.StatusUnauthorized {
			t.Errorf("%s key: %d", name, r.status)
		}
	}

	// revocation reaches the cache through Redis Pub/Sub
	if r := h.send(sdkBatch(sdkEvent(nil))); r.status != http.StatusAccepted {
		t.Fatalf("%d", r.status)
	}
	keys, _ := h.store.Keys(context.Background(), h.project.ID)
	prefix, _, err := h.store.RevokeKey(context.Background(), h.project.ID, keys[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	env.Redis.Publish(context.Background(), auth.KeyRevocationChannel, prefix)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if h.send(sdkBatch(sdkEvent(nil))).status == http.StatusUnauthorized {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("revoked key still accepted")
}

func TestBadRequests(t *testing.T) {
	h := newHarness(t, options{})
	valid, _ := json.Marshal(sdkBatch(sdkEvent(nil)))
	v2 := sdkBatch(sdkEvent(nil))
	v2["schema_version"] = 2
	v2body, _ := json.Marshal(v2)
	bomb := gz(append(append([]byte(`{"schema_version":1,"events":[`), bytes.Repeat([]byte(" "), 3<<20)...), ']', '}'))

	cases := []struct {
		name    string
		body    []byte
		headers map[string]string
		status  int
	}{
		{"invalid json", gz([]byte("{")), nil, http.StatusBadRequest},
		{"schema version", gz(v2body), nil, http.StatusBadRequest},
		{"zip bomb", bomb, nil, http.StatusRequestEntityTooLarge},
		{"not gzip", []byte("plain text"), nil, http.StatusBadRequest},
		{"brotli", valid, map[string]string{"Content-Encoding": "br"}, http.StatusUnsupportedMediaType},
		{"identity", valid, map[string]string{"Content-Encoding": ""}, http.StatusAccepted},
	}
	for _, c := range cases {
		if r := h.post(c.body, c.headers); r.status != c.status {
			t.Errorf("%s: %d %v, want %d", c.name, r.status, r.body, c.status)
		}
	}
	if len(bomb) > 64<<10 {
		t.Fatalf("the bomb is not a bomb: %d bytes compressed", len(bomb))
	}
}

func TestRejectedEventsDoNotFailTheBatch(t *testing.T) {
	h := newHarness(t, options{})
	r := h.send(sdkBatch(sdkEvent(nil), sdkEvent(map[string]any{"event_id": "bad"}), sdkEvent(map[string]any{"type": "pageview"}), sdkEvent(nil)))
	if r.status != http.StatusAccepted || r.body["accepted"] != 2.0 || r.body["rejected"] != 2.0 {
		t.Fatalf("%d %v", r.status, r.body)
	}
}

// failingProducer fails the first produce like an unavailable Kafka.
type failingProducer struct {
	failures atomic.Int32
}

func (p *failingProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	if p.failures.Add(-1) >= 0 {
		return kgo.ProduceResults{{Err: errors.New("kafka: not enough in-sync replicas")}}
	}
	return producer.ProduceSync(ctx, rs...)
}

func TestKafkaUnavailableReleasesTheBatch(t *testing.T) {
	p := &failingProducer{}
	p.failures.Store(1)
	h := newHarness(t, options{producer: p})
	batch := sdkBatch(sdkEvent(nil))
	r := h.send(batch)
	if r.status != http.StatusServiceUnavailable || r.header.Get("Retry-After") == "" {
		t.Fatalf("%d %v", r.status, r.body)
	}
	// the SDK retry with the same batch_id is processed, not reported as duplicate
	r = h.send(batch)
	if r.status != http.StatusAccepted || r.body["duplicate"] != false {
		t.Fatalf("retry: %d %v", r.status, r.body)
	}
}

// blockingProducer holds produce calls until released.
type blockingProducer struct {
	entered chan struct{}
	release chan struct{}
}

func (p *blockingProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.entered <- struct{}{}
	<-p.release
	return producer.ProduceSync(ctx, rs...)
}

func TestOverloadAnswersAtOnce(t *testing.T) {
	p := &blockingProducer{entered: make(chan struct{}, 1), release: make(chan struct{})}
	h := newHarness(t, options{producer: p, maxInflight: 1})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.send(sdkBatch(sdkEvent(nil)))
	}()
	<-p.entered
	r := h.send(sdkBatch(sdkEvent(nil)))
	close(p.release)
	wg.Wait()
	if r.status != http.StatusServiceUnavailable || r.body["error"].(map[string]any)["code"] != "overloaded" {
		t.Fatalf("%d %v", r.status, r.body)
	}
}

func TestHealthAndMetrics(t *testing.T) {
	h := newHarness(t, options{})
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		r, err := http.Get(h.srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(r.Body)
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", path, r.StatusCode)
		}
		if path == "/metrics" && !bytes.Contains(b, []byte("botsight_ingest_duration_seconds")) {
			t.Fatal("metrics missing")
		}
	}
}
