package botsight

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	token  = "7012345678:AAH-secret-token"
	apiKey = "bs_live_ab12cd34_secret"
)

// collector is a fake collector recording accepted batches; status can make it refuse.
type collector struct {
	*httptest.Server
	mu      sync.Mutex
	batches []wireBatch
	raw     []string
	status  atomic.Int32
	posts   atomic.Int32
}

func newCollector(t *testing.T) *collector {
	c := &collector{}
	c.status.Store(http.StatusAccepted)
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+apiKey {
			t.Errorf("authorization %q", r.Header.Get("Authorization"))
		}
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("gzip: %v", err)
			return
		}
		raw, _ := io.ReadAll(zr)
		c.posts.Add(1)
		status := int(c.status.Load())
		if status == http.StatusAccepted {
			var b wireBatch
			_ = json.Unmarshal(raw, &b)
			c.mu.Lock()
			c.batches = append(c.batches, b)
			c.raw = append(c.raw, string(raw))
			c.mu.Unlock()
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *collector) events() []event {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []event
	for _, b := range c.batches {
		out = append(out, b.Events...)
	}
	return out
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 300; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func find(events []event, typ, name string) []event {
	var out []event
	for _, e := range events {
		if e.Type == typ && e.Name == name {
			out = append(out, e)
		}
	}
	return out
}

// telegram is a fake Bot API.
func telegram(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/file/") {
			_, _ = io.WriteString(w, "JPEG")
			return
		}
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		switch method {
		case "getMe":
			_, _ = io.WriteString(w, `{"ok":true,"result":{"id":7012345678,"is_bot":true,"first_name":"Lab"}}`)
		case "getUpdates":
			_, _ = io.WriteString(w, `{"ok":true,"result":[
				{"update_id":10,"message":{"message_id":1,"from":{"id":42,"username":"ann","language_code":"ru"},"chat":{"id":42,"type":"private"},"text":"/start vk"}},
				{"update_id":11,"callback_query":{"id":"q","from":{"id":42},"message":{"chat":{"id":42,"type":"private"}},"data":"lab:2"}}]}`)
		case "sendMessage":
			raw, _ := io.ReadAll(r.Body)
			if bytes.Contains(raw, []byte("555")) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"ok":false,"error_code":403,"description":"Forbidden: bot was blocked by the user"}`)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":5,"chat":{"id":42,"type":"private"}}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"ok":false,"error_code":404,"description":"Not Found"}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newClient(t *testing.T, col *collector, opts ...Option) *Client {
	t.Helper()
	opts = append([]Option{WithEndpoint(col.URL), WithFlushInterval(20 * time.Millisecond), WithLogger(slog.New(slog.DiscardHandler))}, opts...)
	c, err := New(apiKey, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func call(t *testing.T, hc *http.Client, url, contentType string, body io.Reader) int {
	t.Helper()
	resp, err := hc.Post(url, contentType, body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if !json.Valid(raw) && !bytes.Equal(raw, []byte("JPEG")) {
		t.Fatalf("the answer must reach the bot unchanged: %q", raw)
	}
	return resp.StatusCode
}

func TestTransportRecordsBotAPI(t *testing.T) {
	col := newCollector(t)
	tg := telegram(t)
	bs := newClient(t, col)
	hc := bs.HTTPClient()
	base := tg.URL + "/bot" + token + "/"

	call(t, hc, base+"getMe", "application/json", strings.NewReader(`{}`))
	call(t, hc, base+"getUpdates", "application/json", strings.NewReader(`{"timeout":10}`))
	call(t, hc, base+"getUpdates", "application/json", strings.NewReader(`{"timeout":10}`)) // a restart: the same updates
	call(t, hc, base+"sendMessage", "application/json", strings.NewReader(`{"chat_id":42,"text":"hi"}`))

	// the error answer has no Message: chat_id comes from the multipart request
	var mp bytes.Buffer
	mw := multipart.NewWriter(&mp)
	_ = mw.WriteField("chat_id", "555")
	_ = mw.WriteField("text", "hi")
	_ = mw.Close()
	if code := call(t, hc, base+"sendMessage", mw.FormDataContentType(), &mp); code != http.StatusForbidden {
		t.Fatalf("status %d", code)
	}
	call(t, hc, tg.URL+"/file/bot"+token+"/photos/1.jpg", "", nil)

	if err := bs.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	evs := col.events()
	if s := find(evs, "lifecycle", "bot_started"); len(s) != 1 || s[0].BotID != 7012345678 || s[0].Props["sdk_version"] != Version {
		t.Fatalf("bot_started: %+v", s)
	}
	if u := find(evs, "update", "message"); len(u) != 1 || u[0].Command != "start" || u[0].StartParam != "vk" || u[0].UserID != 42 {
		t.Fatalf("updates counted once: %+v", u)
	}
	if u := find(evs, "update", "callback_query"); len(u) != 1 || u[0].CallbackData != "lab:2" {
		t.Fatalf("callback: %+v", u)
	}
	sent := find(evs, "outgoing", "sendMessage")
	if len(sent) != 2 || !sent[0].OK || sent[0].ChatID != 42 {
		t.Fatalf("sent: %+v", sent)
	}
	if sent[1].OK || sent[1].ErrorCode != 403 || *sent[1].Error != "bot_blocked" || sent[1].ChatID != 555 {
		t.Fatalf("blocked: %+v", sent[1])
	}
	if len(find(evs, "outgoing", "getUpdates")) != 0 || len(find(evs, "outgoing", "1.jpg")) != 0 {
		t.Fatal("polling and files are not outgoing events")
	}
	// the token never leaves the process
	for _, raw := range col.raw {
		if strings.Contains(raw, "AAH-secret") {
			t.Fatal("the token is in a batch")
		}
	}
	ids := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	for _, e := range evs {
		if !ids.MatchString(e.EventID) {
			t.Fatalf("not a UUIDv7: %s", e.EventID)
		}
	}
}

func TestTrackAndFrameworkAPI(t *testing.T) {
	col := newCollector(t)
	bs := newClient(t, col)
	// tracked before the bot is known: the bot id is filled when the batch goes
	bs.Track("purchase", 42, map[string]any{"amount": 990, "plan": "pro", "trial": true, "nothing": nil})
	bs.Track("Bad Name", 42, nil)
	bs.UseAdapter()
	bs.Update(7012345678, []byte(`{"update_id":1,"message":{"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"hello"}}`), false, 15*time.Millisecond)
	bs.Step(7012345678, "message", "bot.help", 42, 42, 1200*time.Millisecond, nil)
	bs.Step(7012345678, "message", "bot.pay", 42, 42, time.Millisecond, io.ErrUnexpectedEOF)
	_ = bs.Close(context.Background())

	evs := col.events()
	p := find(evs, "custom", "purchase")
	if len(p) != 1 || p[0].BotID != 7012345678 || p[0].Props["plan"] != "pro" || p[0].Props["trial"] != "true" || p[0].Props["amount"] != float64(990) {
		t.Fatalf("track: %+v", p)
	}
	if _, has := p[0].Props["nothing"]; has {
		t.Fatal("nil props are skipped")
	}
	if len(find(evs, "custom", "Bad Name")) != 0 {
		t.Fatal("invalid name accepted")
	}
	if u := find(evs, "update", "message"); len(u) != 1 || u[0].Handled || u[0].DurationMS != 15 {
		t.Fatalf("update: %+v", u)
	}
	steps := find(evs, "step", "message")
	if len(steps) != 2 || steps[0].Handler != "bot.help" || steps[0].DurationMS != 1200 || steps[0].Error != nil {
		t.Fatalf("steps: %+v", steps)
	}
	if steps[1].Error == nil || *steps[1].Error != "*errors.errorString" {
		t.Fatalf("an error is named by its type, not its message: %+v", steps[1])
	}
}

func TestQueueOverflowIsReported(t *testing.T) {
	col := newCollector(t)
	col.status.Store(http.StatusServiceUnavailable) // Botsight is down
	bs := newClient(t, col, WithQueueSize(5))
	bs.setBotID(7012345678)
	// delivery is stuck retrying bot_started: from now on the queue fills
	waitFor(t, func() bool { return col.posts.Load() > 0 })
	for range 100 {
		bs.Track("tick", 1, nil) // never blocks the bot
	}
	if bs.Dropped() < 90 {
		t.Fatalf("dropped %d", bs.Dropped())
	}
	col.status.Store(http.StatusAccepted)
	_ = bs.Close(context.Background())
	col.mu.Lock()
	defer col.mu.Unlock()
	var reported int64
	for _, b := range col.batches {
		reported += b.Dropped
	}
	if reported < 90 {
		t.Fatalf("the collector learns about the loss: %d", reported)
	}
}

func TestInvalidKeyStopsDelivery(t *testing.T) {
	col := newCollector(t)
	col.status.Store(http.StatusUnauthorized)
	bs := newClient(t, col)
	bs.setBotID(1)
	waitFor(t, func() bool { return !bs.active() })
	bs.Track("x", 1, nil) // a no-op now
	if _, err := New("not-a-key"); err == nil {
		t.Fatal("a malformed key is refused at start")
	}
	off, err := New("", WithDisabled())
	if err != nil {
		t.Fatal(err)
	}
	off.Track("x", 1, nil)
	_ = off.Close(context.Background())
}

func TestDedupWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	d := newDedup()
	d.now = func() time.Time { return now }
	for id := int64(100); id < 110; id++ {
		if !d.fresh(1, id) {
			t.Fatal(id)
		}
	}
	if d.fresh(1, 105) || !d.fresh(1, 115) || !d.fresh(1, 112) || d.fresh(1, 112) {
		t.Fatal("window")
	}
	if !d.fresh(1, 115+70) || d.fresh(1, 115) || !d.fresh(1, 113) {
		t.Fatal("bits lost on a shift")
	}
	now = now.Add(25 * time.Hour)
	if !d.fresh(1, 5) {
		t.Fatal("not forgotten after a day")
	}
}

func TestBotPath(t *testing.T) {
	for path, want := range map[string]bool{
		"/bot123:abc/getMe":            true,
		"/proxy/k/key/bot123:abc/send": true,
		"/file/bot123:abc/photos/1":    false,
		"/bot123:abc/":                 false,
		"/v1/batch":                    false,
	} {
		if _, _, ok := botPath(path); ok != want {
			t.Errorf("%s: %v", path, ok)
		}
	}
}
