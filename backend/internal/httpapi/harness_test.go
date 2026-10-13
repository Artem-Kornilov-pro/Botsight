package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/mail"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/session"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

var (
	env    *testenv.Env
	envErr error
)

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	env, envErr = testenv.Start(ctx)
	cancel()
	code := m.Run()
	if env != nil {
		env.Close()
	}
	os.Exit(code)
}

// requireEnv skips integration tests without Docker; in CI a missing Docker is a failure.
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

const botToken = "123456:TEST-login-bot"

type fakeMailer struct {
	mu   sync.Mutex
	sent []mail.Message
}

func (f *fakeMailer) Send(_ context.Context, msg mail.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, msg)
	return nil
}

func (f *fakeMailer) to(addr string) []mail.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []mail.Message
	for _, m := range f.sent {
		if m.To == addr {
			out = append(out, m)
		}
	}
	return out
}

var tokenRe = regexp.MustCompile(`\?token=([A-Za-z0-9_-]+)`)

type harness struct {
	t      *testing.T
	api    *httpapi.Server
	srv    *httptest.Server
	mailer *fakeMailer
	store  *store.Store
}

// generous limits by default; tests of limits set their own rules
func noLimits() httpapi.Limits {
	big := func(name string) ratelimit.Rule {
		return ratelimit.Rule{Name: name, Limit: 1 << 30, Period: time.Hour}
	}
	return httpapi.Limits{
		LoginIP: big("t:login:ip"), LoginAccount: big("t:login:account"), RegisterIP: big("t:register"),
		TelegramIP: big("t:telegram"), ResetIP: big("t:reset:ip"), ResetEmail: big("t:reset:email"),
		VerifyAccount: big("t:verify"),
	}
}

func newHarness(t *testing.T, configure ...func(*httpapi.Options)) *harness {
	t.Helper()
	requireEnv(t)
	opts := httpapi.Options{
		DashboardURL:  "https://app.botsight.test",
		CORSOrigins:   []string{"https://app.botsight.test"},
		CookieSecure:  false, // httptest serves plain http
		SessionTTL:    time.Hour,
		TelegramToken: botToken,
		Limits:        noLimits(),
	}
	for _, c := range configure {
		c(&opts)
	}
	h := &harness{t: t, mailer: &fakeMailer{}, store: store.New(env.Pool)}
	h.api = httpapi.New(httpapi.Deps{
		Store:    h.store,
		Sessions: session.NewStore(env.Redis, opts.SessionTTL),
		Limiter:  ratelimit.NewWindow(env.Redis),
		Redis:    env.Redis,
		Mailer:   h.mailer,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Options:  opts,
	})
	h.srv = httptest.NewServer(h.api.Handler())
	t.Cleanup(func() {
		h.srv.Close()
		h.api.Wait()
	})
	return h
}

// lastMailToken waits for background emails and returns the token from the latest one.
func (h *harness) lastMailToken(to string) string {
	h.t.Helper()
	h.api.Wait()
	msgs := h.mailer.to(to)
	if len(msgs) == 0 {
		h.t.Fatalf("no email to %s", to)
	}
	m := tokenRe.FindStringSubmatch(msgs[len(msgs)-1].Body)
	if m == nil {
		h.t.Fatalf("no link in email: %s", msgs[len(msgs)-1].Body)
	}
	return m[1]
}

var emailSeq atomic.Int64

func uniqueEmail(t *testing.T) string {
	name := strings.ToLower(regexp.MustCompile(`[^A-Za-z0-9]+`).ReplaceAllString(t.Name(), "-"))
	return fmt.Sprintf("%s-%d-%d@example.com", strings.Trim(name, "-"), time.Now().UnixNano()%1e6, emailSeq.Add(1))
}

// client is a browser: a cookie jar plus the CSRF token from the last sign-in.
type client struct {
	h    *harness
	http *http.Client
	csrf string
}

func (h *harness) client() *client {
	jar, _ := cookiejar.New(nil)
	return &client{h: h, http: &http.Client{Jar: jar}}
}

type response struct {
	status int
	header http.Header
	body   map[string]any
}

func (r response) str(path ...string) string {
	v := r.get(path...)
	s, _ := v.(string)
	return s
}

func (r response) num(path ...string) int64 {
	v := r.get(path...)
	f, _ := v.(float64)
	return int64(f)
}

func (r response) get(path ...string) any {
	var cur any = r.body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

func (r response) list(key string) []map[string]any {
	raw, _ := r.body[key].([]any)
	out := make([]map[string]any, len(raw))
	for i, v := range raw {
		out[i], _ = v.(map[string]any)
	}
	return out
}

func (c *client) do(method, path string, body any) response {
	c.h.t.Helper()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			c.h.t.Fatal(err)
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.h.srv.URL+path, rd)
	if err != nil {
		c.h.t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.csrf != "" {
		req.Header.Set("X-CSRF-Token", c.csrf)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.h.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := response{status: resp.StatusCode, header: resp.Header}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.body); err != nil {
			c.h.t.Fatalf("%s %s: non-JSON response %d: %s", method, path, resp.StatusCode, raw)
		}
	}
	return out
}

// expect fails the test unless the response has the given status.
func (c *client) expect(status int, method, path string, body any) response {
	c.h.t.Helper()
	r := c.do(method, path, body)
	if r.status != status {
		c.h.t.Fatalf("%s %s: status %d, want %d; body %v", method, path, r.status, status, r.body)
	}
	return r
}

func (c *client) signedIn(r response) response {
	c.csrf = r.str("csrf_token")
	if c.csrf == "" {
		c.h.t.Fatalf("no csrf_token in %v", r.body)
	}
	return r
}

func (c *client) register(email, password string) response {
	c.h.t.Helper()
	return c.signedIn(c.expect(http.StatusCreated, "POST", "/v1/auth/register",
		map[string]string{"email": email, "password": password, "name": "Тест"}))
}

func (c *client) login(email, password string) response {
	c.h.t.Helper()
	return c.signedIn(c.expect(http.StatusOK, "POST", "/v1/auth/login",
		map[string]string{"email": email, "password": password}))
}

// newUser registers a fresh account and returns its signed-in client and email.
func (h *harness) newUser() (*client, string) {
	c := h.client()
	email := uniqueEmail(h.t)
	c.register(email, "password123")
	return c, email
}

func (c *client) createProject(name string) response {
	c.h.t.Helper()
	return c.expect(http.StatusCreated, "POST", "/v1/projects", map[string]string{"name": name})
}

func path(format string, args ...any) string { return fmt.Sprintf(format, args...) }
