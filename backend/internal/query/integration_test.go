package query

import (
	"context"
	"errors"
	"math/rand/v2"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

var (
	env    *testenv.Env
	envErr error
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	env, envErr = testenv.StartWith(ctx, testenv.Options{ClickHouse: true, Redis: true})
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

type ev struct {
	at                 time.Duration // from the base
	typ, name          string
	user               uint64
	chat               int64
	chatType           string
	command, param, cb string
	handler, from, to  string
	member, err        string
	handled, ok        uint8
	dur                uint32
	code               uint16
	props              map[string]string
}

// labbot is the form bot of Botsight.md, 5, over three weeks starting on a Monday:
//
//	101 vk      /start -> Form:lab -> Form:name -> lab_generated, back in week 1
//	102 vk      /start -> Form:lab, asked again (bot:name) and /help (neutral), blocked the bot
//	103 (none)  /start -> handler crashed, then a button nobody handles
//	104 tg_ads  week 1: /start -> Form:lab, back in week 2
func labbot() []ev {
	const m, d = time.Minute, 24 * time.Hour
	start := func(at time.Duration, user uint64, param string) ev {
		return ev{at: at, typ: "update", name: "message", user: user, chat: int64(user), chatType: "private",
			command: "start", param: param, handled: 1, dur: 30}
	}
	step := func(at time.Duration, user uint64, handler, from, to string) ev {
		return ev{at: at, typ: "step", name: "message", user: user, chat: int64(user), handler: handler, from: from, to: to, dur: 10}
	}
	msg := func(at time.Duration, user uint64) ev {
		return ev{at: at, typ: "update", name: "message", user: user, chat: int64(user), chatType: "private", handled: 1}
	}
	crash := step(21*m, 103, "bot:cb_lab", "", "")
	crash.err = "ValueError"
	return []ev{
		{at: -m, typ: "lifecycle", name: "bot_started", props: map[string]string{"sdk_version": "0.1.0", "framework": "aiogram", "framework_version": "3.13"}},
		start(0, 101, "vk"), step(m, 101, "bot:cb_lab", "", "Form:lab"), step(2*m, 101, "bot:name", "Form:lab", "Form:name"),
		{at: 3 * m, typ: "custom", name: "lab_generated", user: 101},
		{at: 4 * m, typ: "outgoing", name: "sendDocument", chat: 101, ok: 1, dur: 80},
		{at: 5 * m, typ: "outgoing", name: "sendMessage", chat: 101, code: 429, err: "rate_limited"},
		msg(7*d, 101),

		start(10*m, 102, "vk"), step(11*m, 102, "bot:cb_lab", "", "Form:lab"),
		step(12*m, 102, "bot:name", "Form:lab", "Form:lab"), step(13*m, 102, "bot:help", "Form:lab", "Form:lab"),
		{at: 14 * m, typ: "outgoing", name: "sendMessage", chat: 102, code: 403, err: "bot_blocked"},

		start(20*m, 103, ""), crash,
		{at: 22 * m, typ: "update", name: "callback_query", user: 103, chat: 103, chatType: "private", cb: "x"},

		start(7*d+time.Hour, 104, "tg_ads"), step(7*d+time.Hour+m, 104, "bot:cb_lab", "", "Form:lab"), msg(14*d, 104),
	}
}

// seed writes the events of a fresh project and returns its id and the base time:
// Monday 10:00 UTC three weeks before the current week.
func seed(t *testing.T, events []ev) (uint32, time.Time) {
	t.Helper()
	pid := rand.Uint32N(1<<31) + 1
	base := monday(time.Now(), time.UTC).AddDate(0, 0, -21).Add(10 * time.Hour)
	ctx := context.Background()
	b, err := env.ClickHouse.PrepareBatch(ctx, `INSERT INTO events (project_id, bot_id, event_id, ts, received_at,
		type, name, user_id, chat_id, chat_type, command, start_param, callback_data, handler, state_from, state_to,
		member_status, handled, ok, duration_ms, error_code, error, props_str)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		at := base.Add(e.at)
		id, _ := uuid.NewV7()
		if e.props == nil {
			e.props = map[string]string{}
		}
		if err := b.Append(pid, uint64(7012345678), id, at, at, e.typ, e.name, e.user, e.chat, e.chatType,
			e.command, e.param, e.cb, e.handler, e.from, e.to, e.member, e.handled, e.ok, e.dur, e.code, e.err, e.props); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Send(); err != nil {
		t.Fatal(err)
	}
	return pid, base
}

func runner() *Runner { return &Runner{Conn: env.ClickHouse} }

func TestOverview(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	// ends on Monday 13:00 in Moscow: 22 calendar days
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "Europe/Moscow"}
	o, _, err := runner().Overview(context.Background(), pid, rg)
	if err != nil {
		t.Fatal(err)
	}
	if o.Users != 4 || o.NewUsers != 4 || o.Blocked != 1 || o.HandlerErrors != 1 || o.APIErrors != 2 ||
		o.Updates != 7 || o.Unhandled != 1 || o.WAU != 1 || o.MAU != 4 {
		t.Fatalf("totals %+v", o)
	}
	if o.HandlerMS.P50 != 10 {
		t.Fatalf("handler time %+v", o.HandlerMS)
	}
	if len(o.Days) != 22 || o.Days[0].DAU != 3 || o.Days[0].NewUsers != 3 || o.Days[0].Blocked != 1 || o.Days[1] != (Day{Day: o.Days[1].Day}) {
		t.Fatalf("days %+v", o.Days[:2])
	}
	if len(o.Deploys) != 1 || o.Deploys[0].SDKVersion != "0.1.0" || o.Deploys[0].FrameworkVersion != "3.13" {
		t.Fatalf("deploys %+v", o.Deploys)
	}
}

func TestFunnel(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "UTC"}
	def := FunnelDefinition{
		Steps:         []Step{{"command", "start"}, {"state", "Form:lab"}, {"state", "Form:name"}, {"custom", "lab_generated"}},
		WindowSeconds: 86400,
		Breakdown:     "source",
	}
	f, _, err := runner().Funnel(context.Background(), pid, rg, def)
	if err != nil {
		t.Fatal(err)
	}
	reached := func(steps []FunnelStep) []uint64 {
		out := []uint64{}
		for _, s := range steps {
			out = append(out, s.Users)
		}
		return out
	}
	// the crashed handler of 103 does not count as entering Form:lab
	if got := reached(f.Steps); !reflect.DeepEqual(got, []uint64{4, 3, 1, 1}) {
		t.Fatalf("steps %v", got)
	}
	got := map[string][]uint64{}
	for _, s := range f.Breakdown {
		got[s.Source] = reached(s.Steps)
	}
	want := map[string][]uint64{"vk": {2, 2, 1, 1}, "tg_ads": {1, 1, 0, 0}, "": {1, 0, 0, 0}}
	if !reflect.DeepEqual(got, want) || f.Breakdown[0].Source != "vk" {
		t.Fatalf("breakdown %v", got)
	}

	// a short window: 101 generated the lab 3 minutes after /start
	def.WindowSeconds, def.Breakdown = 120, ""
	f, _, err = runner().Funnel(context.Background(), pid, rg, def)
	if err != nil {
		t.Fatal(err)
	}
	if got := reached(f.Steps); !reflect.DeepEqual(got, []uint64{4, 3, 1, 0}) || f.Breakdown != nil {
		t.Fatalf("window: %v", got)
	}
}

func TestSources(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "UTC"}
	goal := Step{"custom", "lab_generated"}
	got, _, err := runner().Sources(context.Background(), pid, rg, &goal)
	if err != nil {
		t.Fatal(err)
	}
	want := []Source{
		{Source: "vk", Users: 2, Converted: 1, Conversion: 0.5, Blocked: 1, BlockRate: 0.5},
		{Source: "", Users: 1},
		{Source: "tg_ads", Users: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v", got)
	}
	// the second week only: 104 came then
	week := Range{From: base.AddDate(0, 0, 7), To: base.AddDate(0, 0, 14), TZ: "UTC"}
	got, _, err = runner().Sources(context.Background(), pid, week, nil)
	if err != nil || len(got) != 1 || got[0].Source != "tg_ads" || got[0].Converted != 0 {
		t.Fatalf("week: %+v %v", got, err)
	}
}

func TestRetention(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	now := base.AddDate(0, 0, 21).Add(time.Hour) // Monday of the fourth week
	ret, _, err := runner().Retention(context.Background(), pid, 4, "UTC", now)
	if err != nil {
		t.Fatal(err)
	}
	if len(ret.Cohorts) != 2 {
		t.Fatalf("cohorts %+v", ret.Cohorts)
	}
	w0, w1 := ret.Cohorts[0], ret.Cohorts[1]
	if w0.Week != base.Format(time.DateOnly) || w0.Users != 3 || !reflect.DeepEqual(w0.Active, []uint64{3, 1, 0, 0}) {
		t.Fatalf("first cohort %+v", w0)
	}
	if w1.Users != 1 || !reflect.DeepEqual(w1.Active, []uint64{1, 1, 0}) || w1.Share[1] != 1 {
		t.Fatalf("second cohort %+v", w1)
	}
}

func TestErrors(t *testing.T) {
	requireEnv(t)
	evs := labbot()
	for i := range 6 { // a slow handler needs at least 5 calls
		evs = append(evs, ev{at: time.Duration(30+i) * time.Minute, typ: "step", name: "message", user: 105, handler: "bot:slow", dur: uint32(1000 + i*100)})
	}
	pid, base := seed(t, evs)
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "UTC"}
	e, _, err := runner().Errors(context.Background(), pid, rg)
	if err != nil {
		t.Fatal(err)
	}
	if len(e.Handlers) != 1 || e.Handlers[0].Handler != "bot:cb_lab" || e.Handlers[0].Error != "ValueError" || e.Handlers[0].Users != 1 {
		t.Fatalf("handlers %+v", e.Handlers)
	}
	if len(e.API) != 2 || e.RateLimited != 1 || e.API[0].Method != "sendMessage" {
		t.Fatalf("api %+v", e.API)
	}
	if len(e.Slow) != 1 || e.Slow[0].Handler != "bot:slow" || e.Slow[0].MaxMS != 1500 || e.Slow[0].MS.P50 < 1200 {
		t.Fatalf("slow %+v", e.Slow)
	}
	if len(e.Unhandled) != 1 || e.Unhandled[0].Name != "callback_query" {
		t.Fatalf("unhandled %+v", e.Unhandled)
	}
}

func TestGraph(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "UTC"}
	g, _, err := runner().Graph(context.Background(), pid, rg, []string{"bot:help"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Edge{
		{From: "", To: "Form:lab", Transitions: 3, Users: 3, Share: 1},
		{From: "Form:lab", To: "Form:name", Transitions: 1, Users: 1, Share: 1},
	}
	if !reflect.DeepEqual(g.Edges, want) {
		t.Fatalf("edges %+v", g.Edges)
	}
	if !reflect.DeepEqual(g.Retries, []Retry{{State: "Form:lab", Retries: 1, Users: 1}}) {
		t.Fatalf("retries %+v", g.Retries)
	}
	if len(g.Nodes) != 3 || g.Nodes[0].State != "Form:lab" {
		t.Fatalf("nodes %+v", g.Nodes)
	}
}

func TestTimeline(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	now := base.Add(24 * time.Hour)
	page, err := runner().Timeline(context.Background(), pid, 102, nil, now, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 3 || page.NextCursor == "" || page.Events[0].Type != "outgoing" || page.Events[0].Error != "bot_blocked" {
		t.Fatalf("first page %+v", page)
	}
	cur, err := ParseCursor(page.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	rest, err := runner().Timeline(context.Background(), pid, 102, &cur, now, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest.Events) != 2 || rest.NextCursor != "" || rest.Events[1].Command != "start" || rest.Events[1].StartParam != "vk" {
		t.Fatalf("second page %+v", rest)
	}
}

func TestCache(t *testing.T) {
	requireEnv(t)
	pid, base := seed(t, labbot())
	r := &Runner{Conn: env.ClickHouse, Cache: env.Redis, TTL: time.Minute}
	rg := Range{From: base.Add(-time.Hour), To: base.AddDate(0, 0, 21), TZ: "UTC"}
	first, hit, err := r.Errors(context.Background(), pid, rg)
	if err != nil || hit {
		t.Fatalf("first call: hit=%v err=%v", hit, err)
	}
	second, hit, err := r.Errors(context.Background(), pid, rg)
	if err != nil || !hit || !reflect.DeepEqual(first.API, second.API) {
		t.Fatalf("second call: hit=%v err=%v", hit, err)
	}
	// another range is another key
	if _, hit, _ := r.Errors(context.Background(), pid, Range{From: rg.From, To: rg.To.Add(time.Minute), TZ: "UTC"}); hit {
		t.Fatal("different range served from cache")
	}
}

func TestTooHeavy(t *testing.T) {
	requireEnv(t)
	r := &Runner{Conn: env.ClickHouse, MaxExecutionTime: time.Second}
	rows, err := r.query(context.Background(), Query{Params{}, "SELECT sleepEachRow(0.5) FROM numbers(10) SETTINGS max_block_size = 1"})
	if err == nil {
		_, err = scanAll(rows, func(v *uint8) []any { return []any{v} })
	}
	if !errors.Is(err, ErrTooHeavy) {
		t.Fatalf("want ErrTooHeavy, got %v", err)
	}
}
