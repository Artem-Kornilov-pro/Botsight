package botproxy

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	token  = "7012345678:AAH-secret"
	apiKey = "bs_live_ab12cd34_secret"
)

// collector is a fake collector: it keeps the events of accepted batches and can
// answer 503 to the first attempts, like an overloaded instance.
type collector struct {
	mu       sync.Mutex
	events   []Event
	batches  map[string]int // attempts by batch_id
	failures atomic.Int32
	keys     []string
	*httptest.Server
}

func newCollector(t *testing.T) *collector {
	c := &collector{batches: map[string]int{}}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Errorf("collector: %v", err)
			return
		}
		var b batch
		if err := json.NewDecoder(zr).Decode(&b); err != nil {
			t.Errorf("collector: %v", err)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.batches[b.BatchID]++
		c.keys = append(c.keys, r.Header.Get("Authorization"))
		if c.failures.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		c.events = append(c.events, b.Events...)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(c.Close)
	return c
}

// wait returns the events once n of them arrived.
func (c *collector) wait(t *testing.T, n int) []Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		got := append([]Event(nil), c.events...)
		c.mu.Unlock()
		if len(got) >= n {
			time.Sleep(50 * time.Millisecond) // nothing more should come
			c.mu.Lock()
			defer c.mu.Unlock()
			return append([]Event(nil), c.events...)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("collector got %d events, want %d", len(c.events), n)
	return nil
}

// telegram is a fake Bot API.
type telegram struct {
	*httptest.Server
	mu       sync.Mutex
	webhook  string
	received chan int64 // bytes of sendDocument read so far
}

func newTelegram(t *testing.T) *telegram {
	tg := &telegram{received: make(chan int64, 1024)}
	updates := []string{
		`{"update_id":500,"message":{"from":{"id":42,"language_code":"ru"},"chat":{"id":42,"type":"private"},"text":"/start vk"}}`,
		`{"update_id":501,"callback_query":{"id":"q","from":{"id":42},"message":{"chat":{"id":42,"type":"private"}},"data":"lab:lab2"}}`,
	}
	polls := 0
	answer := func(w http.ResponseWriter, v string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, v)
	}
	tg.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/file/bot"+token+"/") {
			_, _ = io.WriteString(w, "JPEG")
			return
		}
		method, ok := strings.CutPrefix(r.URL.Path, "/bot"+token+"/")
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			answer(w, `{"ok":false,"error_code":401,"description":"Unauthorized"}`)
			return
		}
		tg.mu.Lock()
		defer tg.mu.Unlock()
		switch strings.ToLower(method) {
		case "getupdates":
			polls++
			list := updates
			if polls == 2 { // the bot restarted: the same updates again and a new one
				list = append(list, `{"update_id":502,"message":{"from":{"id":43},"chat":{"id":43,"type":"private"},"text":"hi"}}`)
			}
			answer(w, `{"ok":true,"result":[`+strings.Join(list, ",")+`]}`)
		case "sendmessage":
			_ = r.ParseMultipartForm(1 << 20)
			if r.FormValue("chat_id") == "555" {
				w.WriteHeader(http.StatusForbidden)
				answer(w, `{"ok":false,"error_code":403,"description":"Forbidden: bot was blocked by the user"}`)
				return
			}
			answer(w, `{"ok":true,"result":{"message_id":7,"chat":{"id":`+r.FormValue("chat_id")+`,"type":"private"}}}`)
		case "senddocument":
			var n int64
			buf := make([]byte, 32<<10)
			for {
				k, err := r.Body.Read(buf)
				n += int64(k)
				tg.received <- n
				if err != nil {
					break
				}
			}
			answer(w, `{"ok":true,"result":{"message_id":8,"chat":{"id":42,"type":"private"}}}`)
		case "setwebhook":
			_ = r.ParseMultipartForm(1 << 20)
			tg.webhook = r.FormValue("url")
			answer(w, `{"ok":true,"result":true}`)
		case "getwebhookinfo":
			answer(w, `{"ok":true,"result":{"url":"`+tg.webhook+`","pending_update_count":0}}`)
		case "deletewebhook":
			tg.webhook = ""
			answer(w, `{"ok":true,"result":true}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			answer(w, `{"ok":false,"error_code":404,"description":"Not Found: method not found"}`)
		}
	}))
	t.Cleanup(tg.Close)
	return tg
}

type setup struct {
	tg    *telegram
	col   *collector
	proxy *httptest.Server
	hooks *MemoryHooks
	base  string // what the bot puts into TelegramAPIServer.from_base
}

func newSetup(t *testing.T) *setup {
	s := &setup{tg: newTelegram(t), col: newCollector(t), hooks: NewMemoryHooks()}
	sink := &Sink{Collector: s.col.URL, FlushEvery: 20 * time.Millisecond, Backoff: 10 * time.Millisecond,
		Logger: slog.New(slog.DiscardHandler)}
	sink.Start(1000)
	t.Cleanup(func() { _ = sink.Close(t.Context()) })
	var public string
	s.proxy = httptest.NewUnstartedServer(nil)
	public = "http://" + s.proxy.Listener.Addr().String()
	p, err := New(Options{Telegram: s.tg.URL, PublicURL: public, UnsafeWebhooks: true},
		sink, s.hooks, NewDedup(), nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	s.proxy.Config.Handler = p.Handler()
	s.proxy.Start()
	t.Cleanup(s.proxy.Close)
	s.base = s.proxy.URL + "/k/" + apiKey
	return s
}

// call is a Bot API call the way aiogram makes it: multipart/form-data.
func (s *setup) call(t *testing.T, method string, fields map[string]string) map[string]any {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	for k, v := range fields {
		_ = mw.WriteField(k, v)
	}
	_ = mw.Close()
	resp, err := http.Post(s.base+"/bot"+token+"/"+method, mw.FormDataContentType(), &body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func byName(events []Event) map[string][]Event {
	out := map[string][]Event{}
	for _, e := range events {
		out[e.Type+":"+e.Name] = append(out[e.Type+":"+e.Name], e)
	}
	return out
}

func TestPollingAndOutgoing(t *testing.T) {
	s := newSetup(t)
	s.col.failures.Store(2) // the first two attempts get 503: retried with the same batch_id

	first := s.call(t, "getUpdates", map[string]string{"timeout": "10"})
	if first["ok"] != true || len(first["result"].([]any)) != 2 {
		t.Fatalf("getUpdates passes unchanged: %v", first)
	}
	again := s.call(t, "getUpdates", nil) // after a restart: 500 and 501 again
	if len(again["result"].([]any)) != 3 {
		t.Fatalf("the bot gets every update: %v", again)
	}
	if r := s.call(t, "sendMessage", map[string]string{"chat_id": "42", "text": "hi"}); r["ok"] != true {
		t.Fatal(r)
	}
	if r := s.call(t, "sendMessage", map[string]string{"chat_id": "555", "text": "hi"}); r["error_code"] != 403.0 {
		t.Fatal(r)
	}

	got := byName(s.col.wait(t, 5))
	if len(got["update:message"]) != 2 || len(got["update:callback_query"]) != 1 {
		t.Fatalf("updates counted once: %v", got)
	}
	start := got["update:message"][0]
	if start.Command != "start" || start.StartParam != "vk" || start.UserID != 42 || start.BotID != 7012345678 {
		t.Fatalf("start: %+v", start)
	}
	sent := got["outgoing:sendMessage"]
	if len(sent) != 2 || !sent[0].OK || sent[0].ChatID != 42 {
		t.Fatalf("sent: %+v", sent)
	}
	// the error answer has no Message: chat_id comes from the multipart request
	if sent[1].OK || sent[1].ErrorCode != 403 || *sent[1].Error != "bot_blocked" || sent[1].ChatID != 555 {
		t.Fatalf("blocked: %+v", sent[1])
	}
	if len(got["outgoing:getUpdates"]) != 0 {
		t.Fatal("polling is not an outgoing event")
	}

	s.col.mu.Lock()
	defer s.col.mu.Unlock()
	retried := false
	for _, n := range s.col.batches {
		retried = retried || n > 1
	}
	if !retried || s.col.keys[0] != "Bearer "+apiKey {
		t.Fatalf("batches %v, keys %v", s.col.batches, s.col.keys)
	}
}

// A document is streamed: Telegram reads the beginning of the upload while the bot is
// still sending the rest. A proxy that buffered the body would hang here.
func TestUploadIsStreamed(t *testing.T) {
	s := newSetup(t)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	resp := make(chan *http.Response, 1)
	go func() {
		r, err := http.Post(s.base+"/bot"+token+"/sendDocument", mw.FormDataContentType(), pr)
		if err != nil {
			t.Error(err)
			close(resp)
			return
		}
		resp <- r
	}()
	_ = mw.WriteField("chat_id", "42")
	fw, _ := mw.CreateFormFile("document", "lab.docx")
	chunk := bytes.Repeat([]byte("x"), 1<<20)
	_, _ = fw.Write(chunk)
	deadline := time.After(5 * time.Second)
	for seen := int64(0); seen < 512<<10; {
		select {
		case seen = <-s.tg.received:
		case <-deadline:
			t.Fatal("Telegram got nothing while the upload is in progress: the body is buffered")
		}
	}
	for range 20 { // 20 MiB more
		_, _ = fw.Write(chunk)
	}
	_ = mw.Close()
	_ = pw.Close()
	r := <-resp
	if r == nil || r.StatusCode != 200 {
		t.Fatalf("upload: %v", r)
	}
	r.Body.Close()
	if ev := byName(s.col.wait(t, 1))["outgoing:sendDocument"]; len(ev) != 1 || ev[0].ChatID != 42 {
		t.Fatalf("%+v", ev)
	}
}

func TestFilesAndErrors(t *testing.T) {
	s := newSetup(t)
	resp, err := http.Get(s.base + "/file/bot" + token + "/photos/file_1.jpg")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != "JPEG" {
		t.Fatalf("file: %q", b)
	}
	if r := s.call(t, "noSuchMethod", nil); r["error_code"] != 404.0 {
		t.Fatal(r)
	}
	resp, _ = http.Get(s.proxy.URL + "/k/" + apiKey + "/notabot/getMe")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bad bot segment: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Telegram is down: the bot gets 502 in the Bot API format, the call is a network error
	s.tg.Close()
	if r := s.call(t, "sendMessage", map[string]string{"chat_id": "42", "text": "x"}); r["error_code"] != 502.0 || r["ok"] != false {
		t.Fatal(r)
	}
	got := byName(s.col.wait(t, 2))
	if len(got["outgoing:noSuchMethod"]) != 1 || *got["outgoing:noSuchMethod"][0].Error != "not_found" {
		t.Fatalf("%+v", got)
	}
	if net := got["outgoing:sendMessage"]; len(net) != 1 || *net[0].Error != "network" || net[0].ChatID != 42 {
		t.Fatalf("network: %+v", net)
	}
}

func TestWebhookRoundTrip(t *testing.T) {
	s := newSetup(t)
	var botGot atomic.Int32
	bot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Telegram-Bot-Api-Secret-Token") != "s3" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		botGot.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// the answer to a webhook may itself be a Bot API call
		_, _ = io.WriteString(w, `{"method":"sendMessage","chat_id":42,"text":"hello"}`)
	}))
	defer bot.Close()

	if r := s.call(t, "setWebhook", map[string]string{"url": bot.URL + "/tg", "secret_token": "s3"}); r["ok"] != true {
		t.Fatal(r)
	}
	s.tg.mu.Lock()
	registered := s.tg.webhook
	s.tg.mu.Unlock()
	if !strings.HasPrefix(registered, s.proxy.URL+"/w/") {
		t.Fatalf("Telegram got %q, not the proxy", registered)
	}
	info := s.call(t, "getWebhookInfo", nil)
	if url := info["result"].(map[string]any)["url"]; url != bot.URL+"/tg" {
		t.Fatalf("the bot must see its own address: %v", url)
	}

	update := `{"update_id":900,"message":{"from":{"id":42},"chat":{"id":42,"type":"private"},"text":"/help"}}`
	deliver := func() (int, string) {
		req, _ := http.NewRequest("POST", registered, strings.NewReader(update))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "s3")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	code, body := deliver()
	if code != 200 || !strings.Contains(body, `"method":"sendMessage"`) {
		t.Fatalf("relayed answer: %d %s", code, body)
	}
	deliver() // Telegram retried: the bot gets it, analytics counts it once
	if botGot.Load() != 2 {
		t.Fatalf("bot got %d", botGot.Load())
	}
	got := byName(s.col.wait(t, 3)) // setWebhook, getWebhookInfo, the update
	if u := got["update:message"]; len(u) != 1 || u[0].Command != "help" {
		t.Fatalf("webhook updates: %+v", got)
	}

	// a forged update without the secret: the bot refuses and nothing is counted
	req, _ := http.NewRequest("POST", registered, strings.NewReader(`{"update_id":901,"message":{"from":{"id":1},"chat":{"id":1}}}`))
	resp, _ := http.DefaultClient.Do(req)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged: %d", resp.StatusCode)
	}

	if r := s.call(t, "deleteWebhook", nil); r["ok"] != true {
		t.Fatal(r)
	}
	if code, _ := deliver(); code != http.StatusNotFound {
		t.Fatalf("after deleteWebhook: %d", code)
	}
	if r := s.call(t, "setWebhook", map[string]string{"url": "not a url"}); r["error_code"] != 400.0 {
		t.Fatalf("bad url accepted: %v", r)
	}
}
