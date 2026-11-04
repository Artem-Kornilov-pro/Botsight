package gotelegrambot_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"

	botsight "github.com/Artem-Kornilov-pro/Botsight/sdk/go"
	"github.com/Artem-Kornilov-pro/Botsight/sdk/go/gotelegrambot"
)

const token = "7012345678:AAH-secret-token"

type ev struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	Handler    string `json:"handler"`
	Command    string `json:"command"`
	Error      string `json:"-"`
	Handled    bool   `json:"handled"`
	OK         bool   `json:"ok"`
	UserID     int64  `json:"user_id"`
	ChatID     int64  `json:"chat_id"`
	DurationMS int64  `json:"duration_ms"`
}

// collector keeps the events of every batch.
type collector struct {
	*httptest.Server
	mu        sync.Mutex
	events    []ev
	framework string
}

func newCollector(t *testing.T) *collector {
	c := &collector{}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		zr, _ := gzip.NewReader(r.Body)
		var b struct {
			SDK    map[string]string `json:"sdk"`
			Events []struct {
				ev
				Error *string `json:"error"`
			} `json:"events"`
		}
		_ = json.NewDecoder(zr).Decode(&b)
		c.mu.Lock()
		c.framework = b.SDK["framework"]
		for _, e := range b.Events {
			if e.Error != nil {
				e.ev.Error = *e.Error
			}
			c.events = append(c.events, e.ev)
		}
		c.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(c.Close)
	return c
}

func (c *collector) find(typ, name string) []ev {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []ev
	for _, e := range c.events {
		if e.Type == typ && e.Name == name {
			out = append(out, e)
		}
	}
	return out
}

// telegram serves two updates once, then empty long polls.
func telegram(t *testing.T) *httptest.Server {
	var polled atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		method := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		switch method {
		case "getMe":
			_, _ = io.WriteString(w, `{"ok":true,"result":{"id":7012345678,"is_bot":true,"first_name":"Lab","username":"lab_bot"}}`)
		case "getUpdates":
			if polled.Swap(true) {
				time.Sleep(50 * time.Millisecond)
				_, _ = io.WriteString(w, `{"ok":true,"result":[]}`)
				return
			}
			_, _ = io.WriteString(w, `{"ok":true,"result":[
				{"update_id":1,"message":{"message_id":1,"date":1,"from":{"id":42,"is_bot":false,"first_name":"A"},"chat":{"id":42,"type":"private"},"text":"/start vk"}},
				{"update_id":2,"message":{"message_id":2,"date":1,"from":{"id":42,"is_bot":false,"first_name":"A"},"chat":{"id":42,"type":"private"},"text":"hello"}}]}`)
		case "sendMessage":
			_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":3,"date":1,"chat":{"id":42,"type":"private"},"text":"hi"}}`)
		default:
			_, _ = io.WriteString(w, `{"ok":true,"result":true}`)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
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

func setup(t *testing.T) (*botsight.Client, *bot.Bot, *collector) {
	col := newCollector(t)
	tg := telegram(t)
	bs, err := botsight.New("bs_live_ab12cd34_secret", botsight.WithEndpoint(col.URL),
		botsight.WithFlushInterval(20*time.Millisecond), botsight.WithLogger(slog.New(slog.DiscardHandler)),
		gotelegrambot.Framework())
	if err != nil {
		t.Fatal(err)
	}
	b, err := bot.New(token,
		bot.WithServerURL(tg.URL),
		bot.WithHTTPClient(time.Second, bs.HTTPClient()),
		bot.WithMiddlewares(gotelegrambot.Middleware(bs)),
		bot.WithDefaultHandler(gotelegrambot.Fallback(bs, nil)),
		bot.WithNotAsyncHandlers(),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.RegisterHandler(bot.HandlerTypeMessageText, "/start", bot.MatchTypePrefix,
		gotelegrambot.Named("start", func(ctx context.Context, b *bot.Bot, u *models.Update) {
			time.Sleep(15 * time.Millisecond)
			_, _ = b.SendMessage(ctx, &bot.SendMessageParams{ChatID: u.Message.Chat.ID, Text: "hi"})
		}))
	return bs, b, col
}

func TestPollingBot(t *testing.T) {
	bs, b, col := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	go b.Start(ctx)
	waitFor(t, func() bool {
		return len(col.find("outgoing", "sendMessage")) == 1 && len(col.find("update", "message")) == 2
	})
	cancel()
	_ = bs.Close(context.Background())

	updates := col.find("update", "message")
	if len(updates) != 2 {
		t.Fatalf("each update once, from the middleware: %+v", updates)
	}
	byCmd := map[string]ev{}
	for _, u := range updates {
		byCmd[u.Command] = u
	}
	if s := byCmd["start"]; !s.Handled || s.DurationMS < 15 || s.UserID != 42 {
		t.Fatalf("/start: %+v", s)
	}
	if h := byCmd[""]; h.Handled {
		t.Fatalf("hello reached the default handler: unhandled: %+v", h)
	}
	steps := col.find("step", "message")
	if len(steps) != 1 || steps[0].Handler != "start" || steps[0].DurationMS < 15 || steps[0].ChatID != 42 {
		t.Fatalf("named step: %+v", steps)
	}
	if sent := col.find("outgoing", "sendMessage"); !sent[0].OK || sent[0].ChatID != 42 {
		t.Fatalf("outgoing: %+v", sent)
	}
	if len(col.find("lifecycle", "bot_started")) != 1 || col.framework != "go-telegram/bot" {
		t.Fatalf("bot_started, framework %q", col.framework)
	}
}

func TestPanicIsRecordedAndRethrown(t *testing.T) {
	bs, b, col := setup(t)
	b.RegisterHandler(bot.HandlerTypeMessageText, "/boom", bot.MatchTypeExact,
		gotelegrambot.Named("boom", func(context.Context, *bot.Bot, *models.Update) { panic("nil map") }))
	upd := &models.Update{ID: 9, Message: &models.Message{ID: 9, From: &models.User{ID: 7}, Chat: models.Chat{ID: 7, Type: "private"}, Text: "/boom"}}

	func() {
		defer func() {
			if recover() == nil {
				t.Error("the panic must reach the bot as without Botsight")
			}
		}()
		b.ProcessUpdate(context.Background(), upd)
	}()
	_ = bs.Close(context.Background())
	steps := col.find("step", "message")
	if len(steps) != 1 || steps[0].Handler != "boom" || steps[0].Error != "panic" || steps[0].OK {
		t.Fatalf("failed step: %+v", steps)
	}
	if u := col.find("update", "message"); len(u) != 1 || u[0].Command != "boom" {
		t.Fatalf("the update is recorded too: %+v", u)
	}
}
