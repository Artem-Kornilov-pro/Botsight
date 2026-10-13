package antifraud_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/antifraud"
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
	env, envErr = testenv.StartWith(ctx, testenv.Options{Kafka: true, ClickHouse: true})
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

func newProject() uint32 { return 5000 + projectSeq.Add(1)*10 + uint32(time.Now().UnixNano()%10) }

type fakeAlerts struct {
	mu     sync.Mutex
	alerts []event.Alert
}

func (f *fakeAlerts) Send(_ context.Context, a event.Alert) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alerts = append(f.alerts, a)
	return nil
}

func (f *fakeAlerts) of(pid uint32) []event.Alert {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []event.Alert
	for _, a := range f.alerts {
		if a.ProjectID == pid {
			out = append(out, a)
		}
	}
	return out
}

type seedUser struct {
	id          uint64
	source      string
	joined      time.Time
	fast        bool
	signature   string
	hasUsername bool
}

// seed writes what the pipeline would have produced: the /start event (user_first_start
// view fills the source) and the user's behaviour row in user_features.
func seed(t *testing.T, pid uint32, users []seedUser) {
	t.Helper()
	ctx := context.Background()
	ev, err := env.ClickHouse.PrepareBatch(ctx, `INSERT INTO events (project_id, bot_id, event_id, ts, received_at,
		type, name, user_id, chat_id, command, start_param, has_username)`)
	if err != nil {
		t.Fatal(err)
	}
	feat := make([]antifraud.Features, 0, len(users))
	for i, u := range users {
		if u.source != "-" {
			if err := ev.Append(pid, uint64(77), fmt.Sprintf("0192f6c8-%04x-7a3b-9c55-%012x", pid%0xffff, i),
				u.joined, u.joined, "update", "message", u.id, int64(u.id), "start", u.source, uint8(0)); err != nil {
				t.Fatal(err)
			}
		}
		f := antifraud.Features{
			ProjectID: pid, UserID: u.id, FirstEvent: u.joined, LastSeen: time.Now().UTC().Add(-time.Minute),
			Updates: 7, Intervals: 6, MedianIntervalMS: 4000, Signature: u.signature, HasUsername: u.hasUsername, Lang: "ru",
		}
		if u.fast {
			f.FastIntervals, f.MedianIntervalMS = 6, 150
		}
		feat = append(feat, f)
	}
	if err := ev.Send(); err != nil {
		t.Fatal(err)
	}
	if err := antifraud.WriteFeatures(ctx, env.ClickHouse, feat, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func analyzer(alerts antifraud.AlertSink) *antifraud.Analyzer {
	a := antifraud.NewAnalyzer(env.ClickHouse, alerts, slog.New(slog.NewTextHandler(io.Discard, nil)),
		antifraud.NewMetrics(prometheus.NewRegistry()))
	a.GraphEvery = 1
	return a
}

type risk struct {
	score   float32
	reasons []string
	source  string
}

func risks(t *testing.T, pid uint32, at time.Time) map[uint64]risk {
	t.Helper()
	rows, err := env.ClickHouse.Query(context.Background(),
		`SELECT user_id, score, reasons, source FROM user_risk FINAL WHERE project_id = ? AND updated_at = ?`, pid, at)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[uint64]risk{}
	for rows.Next() {
		var uid uint64
		var r risk
		if err := rows.Scan(&uid, &r.score, &r.reasons, &r.source); err != nil {
			t.Fatal(err)
		}
		out[uid] = r
	}
	return out
}

func TestScoringFindsFarmsAndSparesPeople(t *testing.T) {
	requireEnv(t)
	pid := newProject()
	now := time.Now().UTC()
	var users []seedUser
	// 60 people from an ad channel: usernames mostly, their own rhythm
	for i := range 60 {
		users = append(users, seedUser{id: 100_000_000 + uint64(i)*7919, source: "ch_tech", joined: now.Add(-time.Duration(i) * 10 * time.Minute),
			signature: fmt.Sprintf("%d.%d.%d.%d.%d", 10+i, 30+i%7, 12+i%5, 80+i, 25+i%11), hasUsername: i%5 != 0})
	}
	// 40 scripted accounts: no usernames, 150 ms between messages, the same rhythm
	for i := range 40 {
		users = append(users, seedUser{id: 9_100_000_000 + uint64(i), source: "promo_farm", joined: now.Add(-30 * time.Minute),
			fast: true, signature: "1.1.1.1.1"})
	}
	// a referral star: 35 accounts without usernames invited by one account, half scripted
	const root = 9_200_000_000
	users = append(users, seedUser{id: root, source: "ch_tech", joined: now.Add(-20 * time.Hour), hasUsername: true,
		signature: "40.40.40.40.41"})
	for i := range 35 {
		users = append(users, seedUser{id: root + 1 + uint64(i), source: fmt.Sprintf("ref_%d", root), joined: now.Add(-2 * time.Hour),
			fast: i%2 == 0, signature: fmt.Sprintf("%d.9.9.9.9", 3+i)})
	}
	seed(t, pid, users)

	alerts := &fakeAlerts{}
	res, err := analyzer(alerts).Run(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	got := risks(t, pid, res.At)

	people, bots, ring := 0, 0, 0
	for _, u := range users {
		r, flagged := got[u.id]
		switch {
		case u.source == "promo_farm":
			if !flagged || !contains(r.reasons, "fast_steps") || !contains(r.reasons, "synced_timing") {
				t.Errorf("bot %d not flagged: %+v", u.id, r)
			}
			bots++
		case strings.HasPrefix(u.source, "ref_") || u.id == root:
			// every member of the ring, the inviting account included, scripted or not
			if !flagged || !contains(r.reasons, "referral_ring") {
				t.Errorf("ring member %d: %+v", u.id, r)
			}
			ring++
		default:
			if flagged {
				t.Errorf("person %d flagged: %+v", u.id, r)
			}
			people++
		}
	}
	if bots != 40 || ring != 36 || people != 60 {
		t.Fatalf("seed mismatch %d %d %d", bots, ring, people)
	}

	// every ring member is in referral_flags, the root included
	var flagged uint64
	if err := env.ClickHouse.QueryRow(context.Background(),
		`SELECT count() FROM referral_flags FINAL WHERE project_id = ?`, pid).Scan(&flagged); err != nil {
		t.Fatal(err)
	}
	if flagged != 36 {
		t.Fatalf("referral_flags: %d", flagged)
	}

	// source summary and an alert for the farm only
	var users32, suspicious uint32
	if err := env.ClickHouse.QueryRow(context.Background(),
		`SELECT users, suspicious FROM source_risk FINAL WHERE project_id = ? AND source = 'promo_farm' AND updated_at = ?`,
		pid, res.At).Scan(&users32, &suspicious); err != nil {
		t.Fatal(err)
	}
	if users32 != 40 || suspicious != 40 {
		t.Fatalf("source_risk promo_farm %d/%d", suspicious, users32)
	}
	// alerts for both farms, none for the ad channel
	titles := ""
	for _, a := range alerts.of(pid) {
		if a.Kind != "fraud" {
			t.Fatalf("alert kind %q", a.Kind)
		}
		titles += a.Title + "\n"
	}
	if strings.Count(titles, "\n") != 2 || !strings.Contains(titles, "«promo_farm»") ||
		!strings.Contains(titles, fmt.Sprintf("«ref_%d»", root)) || strings.Contains(titles, "ch_tech") {
		t.Fatalf("alerts:\n%s", titles)
	}
}

func TestSpikeOfStarts(t *testing.T) {
	requireEnv(t)
	pid := newProject()
	now := time.Now().UTC().Truncate(time.Minute)
	var users []seedUser
	for i := range 120 { // one new user every 10 minutes for 20 hours
		users = append(users, seedUser{id: 300_000_000 + uint64(i), source: "ads", joined: now.Add(-20*time.Hour + time.Duration(i)*10*time.Minute),
			hasUsername: true, signature: fmt.Sprintf("%d.5.5.5.5", 20+i)})
	}
	burst := now.Add(-time.Hour)
	for i := range 50 { // then 50 in one minute, answering fast
		users = append(users, seedUser{id: 310_000_000 + uint64(i), source: "ads", joined: burst.Add(time.Duration(i) * time.Second),
			fast: true, hasUsername: true, signature: fmt.Sprintf("%d.1.1.1.1", i)})
	}
	seed(t, pid, users)
	res, err := analyzer(&fakeAlerts{}).Run(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	got := risks(t, pid, res.At)
	spiked := 0
	for id, r := range got {
		if id >= 310_000_000 && contains(r.reasons, "start_spike") {
			spiked++
		}
		if id < 310_000_000 {
			t.Errorf("regular user %d flagged: %+v", id, r)
		}
	}
	if spiked != 50 {
		t.Fatalf("burst users with start_spike: %d", spiked)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// --- stream ---------------------------------------------------------------------------

func produce(t *testing.T, cl *kgo.Client, topic string, pid uint32, user uint64, start int64, gap int64, n int) {
	t.Helper()
	recs := make([]*kgo.Record, 0, n)
	for i := range n {
		v, _ := json.Marshal(event.Record{
			ProjectID: pid, BotID: 77, EventID: "0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81", TS: start + int64(i)*gap,
			Type: "update", Name: "message", UserID: user, ChatID: int64(user), UserLang: "en",
		})
		recs = append(recs, &kgo.Record{Topic: topic, Key: event.Key(pid, user, 0), Value: v})
	}
	if err := cl.ProduceSync(context.Background(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

type featureRow struct {
	updates, intervals, fast uint32
	signature                string
}

func waitFeatures(t *testing.T, pid uint32, user uint64, updates uint32) featureRow {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var f featureRow
	for time.Now().Before(deadline) {
		err := env.ClickHouse.QueryRow(context.Background(), `SELECT updates, intervals, fast_intervals, signature
			FROM user_features FINAL WHERE project_id = ? AND user_id = ?`, pid, user).
			Scan(&f.updates, &f.intervals, &f.fast, &f.signature)
		if err == nil && f.updates == updates {
			return f
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("user_features of %d: %+v, want %d updates", user, f, updates)
	return f
}

func runStream(t *testing.T, topic, group string) (stop func()) {
	t.Helper()
	s, err := antifraud.NewStream(antifraud.StreamConfig{
		Brokers: env.KafkaBrokers, Group: group, Topic: topic, FlushInterval: 200 * time.Millisecond,
	}, env.ClickHouse, slog.New(slog.NewTextHandler(io.Discard, nil)), antifraud.NewMetrics(prometheus.NewRegistry()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func TestStreamFeaturesAndReplayAfterRestart(t *testing.T) {
	requireEnv(t)
	pid := newProject()
	topic := fmt.Sprintf("af-%d-events", time.Now().UnixNano())
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(env.KafkaBrokers...)}, kafka.ProducerOptions()...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if err := kafka.EnsureTopics(context.Background(), cl, 1, []kafka.Topic{{Name: topic, Partitions: 3, Retention: time.Hour}}); err != nil {
		t.Fatal(err)
	}
	start := time.Now().Add(-10 * time.Minute).UnixMilli()
	produce(t, cl, topic, pid, 1, start, 150, 8)   // script
	produce(t, cl, topic, pid, 2, start, 6_000, 8) // person
	produce(t, cl, topic, pid, 3, start, 150, 3)   // too short for a signature

	group := topic + "-group"
	stop := runStream(t, topic, group)
	bot := waitFeatures(t, pid, 1, 8)
	person := waitFeatures(t, pid, 2, 8)
	short := waitFeatures(t, pid, 3, 3)
	stop()
	if bot.fast != 7 || bot.signature != "1.1.1.1.1" {
		t.Fatalf("bot %+v", bot)
	}
	if person.fast != 0 || person.signature != "60.60.60.60.60" {
		t.Fatalf("person %+v", person)
	}
	if short.signature != "" {
		t.Fatalf("short %+v", short)
	}

	// A restarted instance has no state and commits nothing: it replays the window and
	// arrives at the same features instead of losing or doubling them.
	produce(t, cl, topic, pid, 1, start+8*150, 150, 2)
	stop = runStream(t, topic, group)
	defer stop()
	again := waitFeatures(t, pid, 1, 10)
	if again.intervals != 9 || again.fast != 9 {
		t.Fatalf("after restart %+v", again)
	}
}
