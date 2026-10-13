// Command loadgen sends SDK-format batches to the collector from many projects and
// users and reports throughput and latency (Botsight.md, 18.1, basic version).
//
//	go run ./cmd/loadgen -projects 20 -users 1000 -rate 20000 -duration 30s
//
// Projects and keys are created through the api service; -keys reuses existing ones.
package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/http/cookiejar"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type options struct {
	collector   string
	api         string
	keys        string
	projects    int
	users       int
	rate        int
	duration    time.Duration
	concurrency int
	batch       int
	duplicates  float64
	texts       bool
	fraud       float64
	journal     string
}

func main() {
	var o options
	flag.StringVar(&o.collector, "collector", "http://localhost:8081", "collector base URL")
	flag.StringVar(&o.api, "api", "http://localhost:8080", "api base URL to create projects")
	flag.StringVar(&o.keys, "keys", "", "comma-separated API keys to use instead of creating projects")
	flag.IntVar(&o.projects, "projects", 10, "projects to create")
	flag.IntVar(&o.users, "users", 1000, "bot users per project")
	flag.IntVar(&o.rate, "rate", 10000, "target events per second in total; 0 sends as fast as possible")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "test duration")
	flag.IntVar(&o.concurrency, "concurrency", 32, "parallel senders")
	flag.IntVar(&o.batch, "batch", 500, "events per batch (max 500)")
	flag.Float64Var(&o.duplicates, "duplicates", 0, "fraction of batches sent twice (checks deduplication)")
	flag.BoolVar(&o.texts, "texts", false, "enable capture_text and send message texts on several topics (for the AI module)")
	flag.Float64Var(&o.fraud, "fraud", 0, "share of users that are bot farms: scripted promo_farm accounts and a referral farm (for antifraud)")
	flag.StringVar(&o.journal, "journal", "", "file to write accepted event_ids to, for reconciliation")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

type project struct {
	id  int64
	key string
}

func run(o options) error {
	if o.batch < 1 || o.batch > 500 {
		return errors.New("-batch must be 1..500")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	projects, err := setupProjects(ctx, o)
	if err != nil {
		return err
	}
	fmt.Printf("projects: %d, users per project: %d, target: %d events/s, %d senders, %s\n",
		len(projects), o.users, o.rate, o.concurrency, o.duration)

	var journal *bufio.Writer
	var journalMu sync.Mutex
	if o.journal != "" {
		f, err := os.Create(o.journal)
		if err != nil {
			return err
		}
		defer f.Close()
		journal = bufio.NewWriterSize(f, 1<<20)
		defer journal.Flush()
	}

	st := &stats{statuses: map[int]int64{}}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{
		MaxIdleConns: o.concurrency * 2, MaxIdleConnsPerHost: o.concurrency * 2, IdleConnTimeout: 90 * time.Second,
	}}
	runCtx, cancel := context.WithTimeout(ctx, o.duration)
	defer cancel()

	// Each sender paces itself to its share of the target rate.
	var interval time.Duration
	if o.rate > 0 {
		perSender := float64(o.rate) / float64(o.concurrency) / float64(o.batch)
		interval = time.Duration(float64(time.Second) / perSender)
	}
	started := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < o.concurrency; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			gen := newGenerator(worker, o.concurrency, o.users)
			gen.texts, gen.fraud = o.texts, o.fraud
			next := time.Now()
			for n := worker; runCtx.Err() == nil; n += o.concurrency {
				p := projects[n%len(projects)]
				events, body := gen.batch(n%len(projects), o.batch)
				// In-flight batches finish after the deadline (ctx, not runCtx), so the
				// journal matches what the collector accepted.
				record := func(accepted bool) {
					if !accepted || journal == nil {
						return
					}
					journalMu.Lock()
					for _, id := range events {
						journal.WriteString(id)
						journal.WriteByte('\n')
					}
					journalMu.Unlock()
				}
				record(st.send(ctx, client, o.collector, p, body))
				if o.duplicates > 0 && mrand.Float64() < o.duplicates {
					st.duplicatesSent.Add(1)
					record(st.send(ctx, client, o.collector, p, body))
				}
				if interval > 0 {
					next = next.Add(interval)
					if d := time.Until(next); d > 0 {
						select {
						case <-time.After(d):
						case <-runCtx.Done():
						}
					} else if d < -time.Second {
						next = time.Now() // fell behind: do not burst to catch up
					}
				}
			}
		}(i)
	}
	wg.Wait()
	st.report(time.Since(started), projects)
	return nil
}

// --- projects ---

func setupProjects(ctx context.Context, o options) ([]project, error) {
	if o.keys != "" {
		var out []project
		for _, k := range strings.Split(o.keys, ",") {
			out = append(out, project{key: strings.TrimSpace(k)})
		}
		return out, nil
	}
	jar, _ := cookiejar.New(nil)
	c := &http.Client{Jar: jar, Timeout: 10 * time.Second}
	email := fmt.Sprintf("loadgen-%d@example.com", time.Now().UnixNano())
	var session struct {
		CSRF string `json:"csrf_token"`
	}
	if err := apiCall(ctx, c, "", o.api+"/v1/auth/register",
		map[string]string{"email": email, "password": "loadgen-password"}, &session); err != nil {
		return nil, fmt.Errorf("register via %s: %w", o.api, err)
	}
	fmt.Printf("account %s, password loadgen-password\n", email)
	var out []project
	for i := 0; i < o.projects; i++ {
		var resp struct {
			Project struct{ ID int64 }   `json:"project"`
			Key     struct{ Key string } `json:"api_key"`
		}
		if err := apiCall(ctx, c, session.CSRF, o.api+"/v1/projects",
			map[string]string{"name": fmt.Sprintf("loadgen %d", i+1)}, &resp); err != nil {
			return nil, fmt.Errorf("create project: %w", err)
		}
		out = append(out, project{id: resp.Project.ID, key: resp.Key.Key})
		if o.texts {
			var ignored map[string]any
			settings := map[string]any{"settings": map[string]any{"capture_text": true, "neutral_handlers": []string{}}}
			if err := apiCallMethod(ctx, c, "PATCH", session.CSRF,
				fmt.Sprintf("%s/v1/projects/%d", o.api, resp.Project.ID), settings, &ignored); err != nil {
				return nil, fmt.Errorf("enable capture_text: %w", err)
			}
		}
	}
	return out, nil
}

func apiCall(ctx context.Context, c *http.Client, csrf, url string, body, out any) error {
	return apiCallMethod(ctx, c, "POST", csrf, url, body, out)
}

func apiCallMethod(ctx context.Context, c *http.Client, method, csrf, url string, body, out any) error {
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s: %d %s", url, resp.StatusCode, msg)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// --- events ---

// generator produces batches like the Python SDK: a user walks a /start -> form ->
// purchase scenario; every action yields an update, a step and an outgoing request.
type generator struct {
	rnd    *mrand.Rand
	worker int // users are split between senders, so one user's timeline stays consistent
	stride int
	n      int                 // users of this sender
	users  map[[2]int]*simUser // per project and user: each bot has its own timeline
	buf    bytes.Buffer
	texts  bool    // add message texts for the AI module
	fraud  float64 // share of farm accounts
}

// simUser is one simulated Telegram user with its own clock: event times follow how
// fast a person or a script acts, not how fast loadgen sends.
type simUser struct {
	id       int64
	kind     int // userPerson, userFarm, userRefFarm
	source   string
	lang     string
	username bool
	premium  bool
	scripted bool // acts with fixed short pauses
	clock    time.Time
	step     int
}

const (
	userPerson = iota
	userFarm
	userRefFarm
	refFarmRoot = 9_500_000_000 // the account that "invites" the referral farm
)

// phrases are user messages on a few support topics plus small talk, for -texts.
var phrases = [][]string{
	{"не приходит код", "код подтверждения не пришел", "смс с кодом не приходит", "жду код уже 10 минут", "не получил код на телефон"},
	{"не могу оплатить", "оплата не проходит", "карта отклонена при оплате", "деньги списались а подписки нет", "не проходит платеж"},
	{"добавьте лабу по физике", "нужна лабораторная по химии", "когда будет лаба 5", "сделайте лабораторную по сетям", "нет моей лабы в списке"},
	{"бот не отвечает", "бот завис после нажатия", "кнопка не работает", "бот молчит уже час", "не работает меню бота"},
	{"привет", "спасибо", "ок", "как дела"},
}

var suffixes = []string{"", "!", " пожалуйста", " срочно", "??", " помогите"}

func (g *generator) text() string {
	topic := phrases[g.rnd.IntN(len(phrases))]
	return topic[g.rnd.IntN(len(topic))] + suffixes[g.rnd.IntN(len(suffixes))]
}

var (
	states  = []string{"", "Form:lab", "Form:name", "Form:group", "Form:confirm"}
	sources = []string{"ch_tech", "ch_memes", "vk_ads_oct", ""}
)

func newGenerator(worker, stride, users int) *generator {
	return &generator{
		rnd:    mrand.New(mrand.NewPCG(uint64(worker), uint64(time.Now().UnixNano()))),
		worker: worker, stride: stride, n: max(1, users/stride), users: map[[2]int]*simUser{},
	}
}

func (g *generator) user(project int, now time.Time) *simUser {
	idx := g.rnd.IntN(g.n)*g.stride + g.worker
	if u := g.users[[2]int{project, idx}]; u != nil {
		return u
	}
	u := &simUser{clock: now.Add(-15*time.Minute + time.Duration(g.rnd.IntN(600))*time.Second)}
	// a stable pseudo-random share of users are farm accounts
	if float64(uint64(idx)*2654435761%1000)/1000 < g.fraud {
		u.kind, u.lang, u.scripted = userFarm, "en", true
		u.id, u.source = 9_000_000_000+int64(idx), "promo_farm"
		if idx%2 == 1 { // the referral farm: half scripted, half driven by hand
			u.kind, u.id, u.source = userRefFarm, refFarmRoot+1+int64(idx), fmt.Sprintf("ref_%d", refFarmRoot)
			u.scripted = (idx/2)%2 == 0
		}
	} else {
		u.kind, u.id = userPerson, 100_000_000+int64(idx)*7919 // older accounts: smaller ids
		u.source = sources[g.rnd.IntN(len(sources))]
		u.lang = []string{"ru", "ru", "ru", "ru", "en", "uk"}[g.rnd.IntN(6)]
		u.username, u.premium = g.rnd.IntN(100) < 85, g.rnd.IntN(100) < 5
	}
	g.users[[2]int{project, idx}] = u
	return u
}

// pause between two actions: people read and type, scripts sleep a fixed time.
func (g *generator) pause(u *simUser) time.Duration {
	if u.scripted {
		return time.Duration(120+g.rnd.IntN(60)) * time.Millisecond
	}
	return time.Duration(1200+g.rnd.ExpFloat64()*6000) * time.Millisecond
}

func (g *generator) batch(project, n int) (ids []string, body []byte) {
	now := time.Now().UTC()
	ts := now.Format("2006-01-02T15:04:05.000Z")
	events := make([]map[string]any, 0, n)
	for len(events) < n {
		u := g.user(project, now)
		user := u.id
		step := u.step
		next := step + 1
		if next >= len(states) || g.rnd.IntN(10) == 0 {
			next = 0 // finished or dropped off: start again
		}
		u.step = next
		u.clock = u.clock.Add(g.pause(u))
		if u.clock.After(now.Add(time.Minute)) {
			u.clock = now // never far in the future
		}
		at := u.clock.UTC().Format("2006-01-02T15:04:05.000Z")
		base := func(typ, name string) map[string]any {
			return map[string]any{"event_id": uuid7(u.clock), "type": typ, "name": name, "ts": at,
				"bot_id": 7000000001, "user_id": user, "chat_id": user}
		}
		update := base("update", "message")
		update["chat_type"], update["content_type"], update["text_len"] = "private", "text", 5+g.rnd.IntN(40)
		update["state"], update["handled"], update["duration_ms"] = states[step], true, g.rnd.IntN(50)
		update["user"] = map[string]any{"lang": u.lang, "is_premium": u.premium, "has_username": u.username}
		if g.texts && step > 0 {
			text := g.text()
			update["text"], update["text_len"] = text, len([]rune(text))
		}
		if step == 0 {
			update["command"], update["start_param"] = "start", u.source
		}
		stepEv := base("step", "message")
		stepEv["handler"], stepEv["state_from"], stepEv["state_to"] = "bot.handlers:step", states[step], states[next]
		stepEv["duration_ms"], stepEv["error"] = g.rnd.IntN(40), nil
		out := base("outgoing", "sendMessage")
		out["user_id"], out["ok"], out["error_code"], out["error"], out["duration_ms"] = 0, true, 0, nil, 20+g.rnd.IntN(80)
		events = append(events, update, stepEv, out)
		if step == len(states)-1 {
			purchase := base("custom", "purchase")
			purchase["props"] = map[string]any{"amount": 990, "plan": "pro"}
			events = append(events, purchase)
		}
	}
	events = events[:n]
	ids = make([]string, n)
	for i, e := range events {
		ids[i] = e["event_id"].(string)
	}
	g.buf.Reset()
	zw := gzip.NewWriter(&g.buf)
	_ = json.NewEncoder(zw).Encode(map[string]any{
		"schema_version": 1, "batch_id": uuid7(now), "sent_at": ts, "dropped": 0,
		"sdk":    map[string]any{"name": "botsight-loadgen", "version": "0.1.0", "framework": "loadgen", "framework_version": "1"},
		"events": events,
	})
	_ = zw.Close()
	return ids, slices.Clone(g.buf.Bytes())
}

func uuid7(t time.Time) string {
	var b [16]byte
	_, _ = rand.Read(b[6:])
	ms := uint64(t.UnixMilli())
	for i := 0; i < 6; i++ {
		b[i] = byte(ms >> (40 - 8*i))
	}
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// --- sending and stats ---

type stats struct {
	mu             sync.Mutex
	statuses       map[int]int64
	latencies      []time.Duration
	accepted       atomic.Int64
	duplicates     atomic.Int64
	duplicatesSent atomic.Int64
	errors         atomic.Int64
}

// send posts a batch, retrying 409/429/503 like the SDK. It reports whether the
// batch was accepted for the first time.
func (s *stats) send(ctx context.Context, c *http.Client, collector string, p project, body []byte) bool {
	for attempt := 0; attempt < 5 && ctx.Err() == nil; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", collector+"/v1/batch", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Authorization", "Bearer "+p.key)
		started := time.Now()
		resp, err := c.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				s.errors.Add(1)
			}
			continue
		}
		var out struct {
			Accepted  int64 `json:"accepted"`
			Duplicate bool  `json:"duplicate"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		s.mu.Lock()
		s.statuses[resp.StatusCode]++
		s.latencies = append(s.latencies, time.Since(started))
		s.mu.Unlock()
		switch resp.StatusCode {
		case http.StatusAccepted:
			if out.Duplicate {
				s.duplicates.Add(1)
				return false
			}
			s.accepted.Add(out.Accepted)
			return true
		case http.StatusConflict, http.StatusTooManyRequests, http.StatusServiceUnavailable:
			time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
		default:
			return false
		}
	}
	return false
}

func (s *stats) report(elapsed time.Duration, projects []project) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slices.Sort(s.latencies)
	pct := func(p float64) time.Duration {
		if len(s.latencies) == 0 {
			return 0
		}
		return s.latencies[min(len(s.latencies)-1, int(float64(len(s.latencies))*p))]
	}
	var requests int64
	for _, n := range s.statuses {
		requests += n
	}
	fmt.Printf("\nelapsed %s, requests %d (%.0f/s), statuses %v, network errors %d\n",
		elapsed.Round(time.Millisecond), requests, float64(requests)/elapsed.Seconds(), s.statuses, s.errors.Load())
	fmt.Printf("accepted events %d (%.0f/s); duplicate batches sent %d, answered as duplicates %d\n",
		s.accepted.Load(), float64(s.accepted.Load())/elapsed.Seconds(), s.duplicatesSent.Load(), s.duplicates.Load())
	fmt.Printf("latency p50 %s, p95 %s, p99 %s, max %s\n",
		pct(.50).Round(time.Microsecond*100), pct(.95).Round(time.Microsecond*100),
		pct(.99).Round(time.Microsecond*100), pct(1).Round(time.Microsecond*100))
	if len(projects) > 0 && projects[0].id != 0 {
		ids := make([]string, len(projects))
		for i, p := range projects {
			ids[i] = fmt.Sprint(p.id)
		}
		fmt.Printf("\nreconcile in ClickHouse (expect %d rows, 0 duplicates):\n"+
			"  SELECT count() AS rows, uniqExact(event_id) AS unique_events, rows - unique_events AS duplicates\n"+
			"  FROM events WHERE project_id IN (%s)\n", s.accepted.Load(), strings.Join(ids, ","))
	}
}
