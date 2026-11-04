package query

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParseStep(t *testing.T) {
	cases := map[string]Step{
		"custom:purchase":    {"custom", "purchase"},
		"state:Form:confirm": {"state", "Form:confirm"},
		"command:/start":     {"command", "start"},
		"callback_data:a:b":  {"callback_data", "a:b"},
	}
	for in, want := range cases {
		got, err := ParseStep(in)
		if err != nil || got != want {
			t.Errorf("ParseStep(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "purchase", "custom:", "event:x", "custom:  "} {
		if _, err := ParseStep(bad); err == nil {
			t.Errorf("ParseStep(%q) accepted", bad)
		}
	}
}

// Values go only into parameters: a hostile step value never appears in the SQL text.
func TestValuesAreParameters(t *testing.T) {
	evil := "x') OR 1=1 --"
	rg := Range{From: time.Unix(0, 0), To: time.Unix(3600, 0), TZ: "Europe/Moscow"}
	def := FunnelDefinition{Steps: []Step{{"command", evil}, {"custom", evil}}, WindowSeconds: 60, Breakdown: "source"}
	fq, err := funnelQuery(7, rg, def)
	if err != nil {
		t.Fatal(err)
	}
	goal := Step{"state", evil}
	sq, err := sourcesQuery(7, rg, &goal)
	if err != nil {
		t.Fatal(err)
	}
	e, r := graphQueries(7, rg, []string{evil})
	for _, q := range []Query{fq, sq, e, r, timelineQuery(7, 42, nil, time.Now(), 10)} {
		if strings.Contains(q.SQL, evil) || strings.Contains(q.SQL, "Europe/Moscow") {
			t.Fatalf("value in SQL text:\n%s", q.SQL)
		}
		if len(q.Args()) != len(q.Params) {
			t.Fatalf("args %d, params %d", len(q.Args()), len(q.Params))
		}
	}
	if fq.Params["s0"] != evil || fq.Params["window"] != uint64(60) || fq.Params["pid"] != uint32(7) {
		t.Fatalf("funnel params: %v", fq.Params)
	}
	if !strings.Contains(fq.SQL, "user_first_start") {
		t.Fatal("breakdown by source must join the first touch")
	}
}

func TestFunnelQueryRejectsBadDefinitions(t *testing.T) {
	rg := Range{From: time.Unix(0, 0), To: time.Unix(1, 0), TZ: "UTC"}
	if _, err := funnelQuery(1, rg, FunnelDefinition{Steps: []Step{{"command", "start"}}, WindowSeconds: 60}); err == nil {
		t.Fatal("one step accepted")
	}
	if _, err := funnelQuery(1, rg, FunnelDefinition{Steps: []Step{{"command", "a"}, {"bad", "b"}}, WindowSeconds: 60}); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestBuildFunnel(t *testing.T) {
	def := FunnelDefinition{
		Steps:     []Step{{"command", "start"}, {"state", "A"}, {"custom", "done"}},
		Breakdown: "source",
	}
	// last step reached: 3 users stopped at 1, 2 at 2, 1 finished; level 9 is noise
	f := buildFunnel(def, []levelCount{
		{"vk", 1, 2}, {"vk", 2, 2}, {"vk", 3, 1}, {"", 1, 1}, {"", 9, 5}, {"", 0, 7},
	})
	users := func(steps []FunnelStep) []uint64 {
		out := make([]uint64, len(steps))
		for i, s := range steps {
			out[i] = s.Users
		}
		return out
	}
	if got := users(f.Steps); !reflect.DeepEqual(got, []uint64{6, 3, 1}) {
		t.Fatalf("steps %v", got)
	}
	if f.Steps[1].FromPrev != 0.5 || f.Steps[2].FromFirst != 0.1667 || f.Steps[0].FromPrev != 1 {
		t.Fatalf("shares %+v", f.Steps)
	}
	if len(f.Breakdown) != 2 || f.Breakdown[0].Source != "vk" || !reflect.DeepEqual(users(f.Breakdown[0].Steps), []uint64{5, 3, 1}) {
		t.Fatalf("breakdown %+v", f.Breakdown)
	}
	if plain := buildFunnel(FunnelDefinition{Steps: def.Steps}, nil); plain.Breakdown != nil || plain.Steps[1].FromPrev != 0 {
		t.Fatalf("empty funnel %+v", plain)
	}
}

func TestCohortTrimsFutureWeeks(t *testing.T) {
	thisWeek := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	c := cohort("2026-10-26", 4, []uint64{4, 2, 0, 0}, thisWeek)
	if !reflect.DeepEqual(c.Active, []uint64{4, 2}) || !reflect.DeepEqual(c.Share, []float64{1, 0.5}) {
		t.Fatalf("%+v", c)
	}
	if c := cohort("2026-11-02", 1, []uint64{1, 0}, thisWeek); len(c.Active) != 1 {
		t.Fatalf("current week: %+v", c)
	}
}

func TestMonday(t *testing.T) {
	msk, _ := time.LoadLocation("Europe/Moscow")
	// Sunday 22:30 UTC is already Monday in Moscow
	sun := time.Date(2026, 11, 8, 22, 30, 0, 0, time.UTC)
	if got := monday(sun, time.UTC).Format(time.DateOnly); got != "2026-11-02" {
		t.Fatalf("UTC: %s", got)
	}
	if got := monday(sun, msk).Format(time.DateOnly); got != "2026-11-09" {
		t.Fatalf("Moscow: %s", got)
	}
}

func TestFillDays(t *testing.T) {
	rg := Range{
		From: time.Date(2026, 10, 1, 21, 0, 0, 0, time.UTC), // Oct 2 in Moscow
		To:   time.Date(2026, 10, 4, 21, 0, 0, 0, time.UTC), // end of Oct 4 in Moscow
		TZ:   "Europe/Moscow",
	}
	days, total := fillDays(rg, []Day{{Day: "2026-10-03", DAU: 5}}, []Day{{Day: "2026-10-03", NewUsers: 2}})
	if total != 2 || len(days) != 3 || days[0].Day != "2026-10-02" || days[1].DAU != 5 || days[1].NewUsers != 2 || days[2].Day != "2026-10-04" {
		t.Fatalf("%d %+v", total, days)
	}
}

func TestCursor(t *testing.T) {
	c := Cursor{TS: time.UnixMilli(1_730_000_000_123).UTC(), EventID: uuid.MustParse("0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81")}
	got, err := ParseCursor(c.String())
	if err != nil || got != c {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"", "123", "abc_0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81", "-1_0192f6c8-4d1e-7a3b-9c55-3f1a2b6c7d81", "1_nope"} {
		if _, err := ParseCursor(bad); err == nil {
			t.Errorf("ParseCursor(%q) accepted", bad)
		}
	}
}

func TestGraphNodes(t *testing.T) {
	edges := []Edge{{From: "", To: "A", Users: 4}, {From: "A", To: "B", Users: 3}, {From: "A", To: "", Users: 1}}
	ns := nodes(edges)
	if edges[1].Share != 0.75 || edges[2].Share != 0.25 || edges[0].Share != 1 {
		t.Fatalf("shares %+v", edges)
	}
	if ns[0].State != "A" || ns[0].In != 4 || ns[0].Out != 4 {
		t.Fatalf("nodes %+v", ns)
	}
}
