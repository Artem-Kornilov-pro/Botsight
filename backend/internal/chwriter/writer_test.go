package chwriter_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/chwriter"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

var (
	env    *testenv.Env
	envErr error
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	env, envErr = testenv.StartWith(ctx, testenv.Options{Redis: true, Kafka: true, ClickHouse: true})
	cancel()
	code := m.Run()
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

var projectSeq atomic.Uint32

// fixture gives a test its own topics, consumer group and project id, so tests
// sharing Kafka and ClickHouse never see each other's data.
type fixture struct {
	t         *testing.T
	name      string
	projectID uint32
	sinks     []chwriter.Sink
	producer  *kgo.Client
}

func newFixture(t *testing.T, partitions int32) *fixture {
	t.Helper()
	requireEnv(t)
	name := fmt.Sprintf("t%d-%s", time.Now().UnixNano()%1e9, strings.ToLower(t.Name()))
	f := &fixture{t: t, name: name, projectID: 1000 + projectSeq.Add(1)}
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(env.KafkaBrokers...)}, kafka.ProducerOptions()...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	f.producer = cl
	var topics []kafka.Topic
	for _, s := range chwriter.Sinks() {
		s.Topic = name + "." + s.Topic
		f.sinks = append(f.sinks, s)
		topics = append(topics, kafka.Topic{Name: s.Topic, Partitions: partitions, Retention: time.Hour})
	}
	topics = append(topics, kafka.Topic{Name: kafka.TopicDLQ, Partitions: 1, Retention: time.Hour})
	if err := kafka.EnsureTopics(context.Background(), cl, 1, topics); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) topic(sink string) string { return f.name + "." + sink }

func (f *fixture) writer(cfg chwriter.Config, conn driver.Conn) (*chwriter.Writer, *chwriter.Metrics) {
	f.t.Helper()
	cfg.Brokers = env.KafkaBrokers
	if cfg.Group == "" {
		cfg.Group = f.name
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 200 * time.Millisecond
	}
	cfg.RetryMax = 200 * time.Millisecond
	if conn == nil {
		conn = env.ClickHouse
	}
	m := chwriter.NewMetrics(prometheus.NewRegistry())
	w, err := chwriter.New(cfg, conn, env.Redis, f.sinks, slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	if err != nil {
		f.t.Fatal(err)
	}
	return w, m
}

// run starts a writer and returns a function that stops it and returns Run's error.
func run(w *chwriter.Writer) (stop func() error) {
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	return func() error {
		cancel()
		return <-errc
	}
}

func uuidFor(project uint32, i int) string {
	return fmt.Sprintf("0192f6c8-%04x-7a3b-9c55-%012x", project%0xffff, i)
}

func (f *fixture) produceEvents(n int) {
	f.t.Helper()
	now := time.Now().UnixMilli()
	recs := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		r := event.Record{
			ProjectID: f.projectID, BotID: 77, EventID: uuidFor(f.projectID, i), TS: now - int64(i), ReceivedAt: now,
			Type: "update", Name: "message", UserID: uint64(i%50 + 1), ChatID: int64(i%50 + 1),
			Command: "start", StartParam: fmt.Sprintf("src_%d", i%3), Handled: true, UserLang: "ru",
			PropsNum: map[string]float64{"n": float64(i)},
		}
		recs = append(recs, f.record(kafka.TopicEvents, r.UserID, r))
	}
	if err := f.producer.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) record(sink string, user uint64, v any) *kgo.Record {
	f.t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		f.t.Fatal(err)
	}
	return &kgo.Record{Topic: f.topic(sink), Key: event.Key(f.projectID, user, 0), Value: b}
}

func (f *fixture) count(query string) uint64 {
	f.t.Helper()
	var n uint64
	if err := env.ClickHouse.QueryRow(context.Background(), query, f.projectID).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// waitCount waits until the query (with project_id as $1) returns want.
func (f *fixture) waitCount(query string, want uint64) {
	f.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var got uint64
	for time.Now().Before(deadline) {
		if got = f.count(query); got == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.t.Fatalf("%s: got %d, want %d", query, got, want)
}

const (
	countEvents  = "SELECT count() FROM events WHERE project_id = ?"
	uniqueEvents = "SELECT uniqExact(event_id) FROM events WHERE project_id = ?"
	dailyEvents  = "SELECT sum(events) FROM events_daily WHERE project_id = ?"
)

func TestWritesAllTables(t *testing.T) {
	f := newFixture(t, 3)
	w, m := f.writer(chwriter.Config{BatchRows: 100}, nil)
	stop := run(w)
	defer stop()

	f.produceEvents(250)
	text := event.Text{ProjectID: f.projectID, BotID: 77, EventID: uuidFor(f.projectID, 1), TS: time.Now().UnixMilli(), UserID: 5, State: "Form:name", Text: "привет"}
	meta := event.BatchMeta{ProjectID: f.projectID, BatchID: uuidFor(f.projectID, 999), ReceivedAt: time.Now().UnixMilli(),
		SDKName: "botsight-python", SDKVersion: "0.1.0", Framework: "aiogram", FrameworkVersion: "3.31.0", Events: 250, Dropped: 7}
	if err := f.producer.ProduceSync(context.Background(),
		f.record(kafka.TopicTexts, 5, text), f.record(kafka.TopicBatches, 0, meta)).FirstErr(); err != nil {
		t.Fatal(err)
	}

	f.waitCount(countEvents, 250)
	f.waitCount("SELECT count() FROM messages_text WHERE project_id = ?", 1)
	f.waitCount("SELECT sum(dropped) FROM ingest_batches WHERE project_id = ?", 7)
	f.waitCount(dailyEvents, 250)
	f.waitCount("SELECT uniqCombinedMerge(users) FROM events_daily WHERE project_id = ?", 50)
	// first touch attribution via the user_first_start view (Botsight.md, 12.3)
	f.waitCount("SELECT count() FROM (SELECT user_id, argMinMerge(source) AS s FROM user_first_start WHERE project_id = ? GROUP BY user_id) WHERE s != ''", 50)

	var lang, startParam string
	var n float64
	if err := env.ClickHouse.QueryRow(context.Background(),
		"SELECT user_lang, start_param, props_num['n'] FROM events WHERE project_id = ? AND event_id = ?",
		f.projectID, uuidFor(f.projectID, 4)).Scan(&lang, &startParam, &n); err != nil {
		t.Fatal(err)
	}
	if lang != "ru" || startParam != "src_1" || n != 4 {
		t.Fatalf("row mismatch: %q %q %v", lang, startParam, n)
	}
	if got := testutil.ToFloat64(m.Rows.WithLabelValues("events")); got != 250 {
		t.Fatalf("rows metric %v", got)
	}
}

// TestCrashBetweenInsertAndCommit is the chaos case of Botsight.md, 18.3: the writer
// dies after ClickHouse accepted a chunk but before the Kafka offset is committed.
// The next writer reads the same records, rebuilds the same chunk from the plan in
// Redis and ClickHouse drops the repeated insert: every event exists exactly once.
func TestCrashBetweenInsertAndCommit(t *testing.T) {
	f := newFixture(t, 3)
	f.produceEvents(3000)

	var crashed atomic.Int32
	first, _ := f.writer(chwriter.Config{
		BatchRows: 400,
		AfterInsert: func(topic string, _ int32, _ string) error {
			if strings.HasSuffix(topic, kafka.TopicEvents) && crashed.Add(1) == 1 {
				return errors.New("kill -9")
			}
			return nil
		},
	}, nil)
	stopFirst := run(first)
	deadline := time.Now().Add(30 * time.Second)
	for crashed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if err := stopFirst(); !errors.Is(err, chwriter.ErrCrashed) {
		t.Fatalf("first writer: %v", err)
	}
	inserted := f.count(countEvents)
	if inserted == 0 {
		t.Fatal("the crashed writer inserted nothing; the test proves nothing")
	}

	second, _ := f.writer(chwriter.Config{BatchRows: 400}, nil)
	stop := run(second)
	defer stop()
	f.waitCount(uniqueEvents, 3000)
	time.Sleep(500 * time.Millisecond) // give a duplicate time to appear
	if rows := f.count(countEvents); rows != 3000 {
		t.Fatalf("duplicates after crash: %d rows for 3000 events (crashed writer had inserted %d)", rows, inserted)
	}
	if daily := f.count(dailyEvents); daily != 3000 {
		t.Fatalf("daily aggregate counted the repeated chunk: %d", daily)
	}
}

func TestDeadLetterQueue(t *testing.T) {
	f := newFixture(t, 1)
	w, m := f.writer(chwriter.Config{BatchRows: 100}, nil)
	stop := run(w)
	defer stop()

	bad := &kgo.Record{Topic: f.topic(kafka.TopicEvents), Key: []byte("x"), Value: []byte(`{"project_id":` + fmt.Sprint(f.projectID) + `,"event_id":"not-a-uuid"}`)}
	if err := f.producer.ProduceSync(context.Background(), bad).FirstErr(); err != nil {
		t.Fatal(err)
	}
	f.produceEvents(10)
	f.waitCount(countEvents, 10)

	deadline := time.Now().Add(10 * time.Second)
	for testutil.ToFloat64(m.DLQ.WithLabelValues(f.topic(kafka.TopicEvents))) < 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := testutil.ToFloat64(m.DLQ.WithLabelValues(f.topic(kafka.TopicEvents))); got != 1 {
		t.Fatalf("DLQ metric %v", got)
	}
	if !dlqHas(t, "original_topic", f.topic(kafka.TopicEvents)) {
		t.Fatal("bad record not found in events.dlq")
	}
}

func dlqHas(t *testing.T, header, value string) bool {
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.KafkaBrokers...), kgo.ConsumeTopics(kafka.TopicDLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for ctx.Err() == nil {
		found := false
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) {
			for _, h := range r.Headers {
				if h.Key == header && string(h.Value) == value {
					found = true
				}
			}
		})
		if found {
			return true
		}
	}
	return false
}

// flakyConn fails the first inserts like an unavailable ClickHouse.
type flakyConn struct {
	driver.Conn
	failures atomic.Int32
}

func (c *flakyConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.failures.Add(-1) >= 0 {
		return nil, errors.New("clickhouse: connection refused")
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

func TestClickHouseUnavailable(t *testing.T) {
	f := newFixture(t, 1)
	conn := &flakyConn{Conn: env.ClickHouse}
	conn.failures.Store(5)
	w, m := f.writer(chwriter.Config{BatchRows: 1000}, conn)
	stop := run(w)
	defer stop()

	f.produceEvents(500)
	f.waitCount(countEvents, 500)
	if errs := testutil.ToFloat64(m.InsertErrors.WithLabelValues("events")); errs < 5 {
		t.Fatalf("insert errors metric %v", errs)
	}
	// the partition resumes after the commit that follows the successful insert
	deadline := time.Now().Add(5 * time.Second)
	for testutil.ToFloat64(m.Paused) != 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if paused := testutil.ToFloat64(m.Paused); paused != 0 {
		t.Fatalf("partition still paused: %v", paused)
	}
}

func TestGracefulStopFlushesBuffer(t *testing.T) {
	f := newFixture(t, 1)
	// a long flush interval: only the shutdown drain can write these rows
	w, _ := f.writer(chwriter.Config{BatchRows: 10_000, FlushInterval: time.Hour}, nil)
	stop := run(w)
	f.produceEvents(100)
	time.Sleep(2 * time.Second) // let the writer fetch them
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	f.waitCount(countEvents, 100)

	// the drain committed: a new writer of the same group has nothing to repeat
	again, _ := f.writer(chwriter.Config{Group: f.name, BatchRows: 10}, nil)
	stopAgain := run(again)
	time.Sleep(2 * time.Second)
	_ = stopAgain()
	if rows := f.count(countEvents); rows != 100 {
		t.Fatalf("rows %d after restart", rows)
	}
}
