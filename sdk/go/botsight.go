// Package botsight is the Go SDK of Botsight: analytics for Telegram bots written in Go.
//
// The SDK runs inside the bot and sends only events; the bot token never leaves the
// process. If Botsight is down, the bot works as usual: events wait in a bounded queue
// and are dropped, never the bot's requests.
//
//	bs, err := botsight.New(os.Getenv("BOTSIGHT_KEY"))
//	defer bs.Close(context.Background())
//	client := bs.HTTPClient() // give it to the bot library
//
// The HTTP client records every Bot API call and the updates of getUpdates. Framework
// adapters (see the gotelegrambot package) add handler times and unhandled updates, and
// work for webhooks too.
package botsight

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Version of the SDK, sent with every batch.
const Version = "0.1.0"

// DefaultEndpoint is the collector of the hosted Botsight.
const DefaultEndpoint = "https://ingest.botsight.ru"

type config struct {
	endpoint   string
	flush      time.Duration
	queue      int
	batch      int
	logger     *slog.Logger
	delivery   *http.Client
	framework  string
	frameworkV string
	disabled   bool
}

// Option configures a Client.
type Option func(*config)

// WithEndpoint sets the collector URL, e.g. http://localhost:8081 for a local stack.
func WithEndpoint(url string) Option { return func(c *config) { c.endpoint = url } }

// WithFlushInterval: how long an event may wait for its batch; default 2 s.
func WithFlushInterval(d time.Duration) Option { return func(c *config) { c.flush = d } }

// WithQueueSize: events waiting for delivery; beyond it they are dropped and counted.
func WithQueueSize(n int) Option { return func(c *config) { c.queue = n } }

// WithLogger receives the SDK's own warnings; default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithDeliveryClient is the HTTP client that talks to the collector.
func WithDeliveryClient(h *http.Client) Option { return func(c *config) { c.delivery = h } }

// WithFramework names the bot library in batches and the bot_started event; adapters set it.
func WithFramework(name, version string) Option {
	return func(c *config) { c.framework, c.frameworkV = name, version }
}

// WithDisabled turns the SDK into a no-op, e.g. in tests of the bot.
func WithDisabled() Option { return func(c *config) { c.disabled = true } }

// Client collects events. Its methods are safe for concurrent use and never block the bot.
type Client struct {
	apiKey string
	cfg    config
	send   *sender
	dedup  *dedup

	botID   atomic.Int64
	started sync.Once
	adapter atomic.Bool // updates come from a framework adapter, not from getUpdates
	off     atomic.Bool
}

var keyRe = regexp.MustCompile(`^bs_[a-z]+_[A-Za-z0-9]{8}_[A-Za-z0-9_-]+$`)

// New starts a client with the project's API key from the dashboard.
func New(apiKey string, opts ...Option) (*Client, error) {
	cfg := config{endpoint: DefaultEndpoint, flush: 2 * time.Second, queue: 10_000, batch: 500, framework: "go", frameworkV: runtime.Version()}
	for _, o := range opts {
		o(&cfg)
	}
	c := &Client{apiKey: apiKey, cfg: cfg, dedup: newDedup()}
	if cfg.disabled {
		c.off.Store(true)
		return c, nil
	}
	if !keyRe.MatchString(apiKey) {
		return nil, fmt.Errorf("botsight: api key must look like bs_live_xxxxxxxx_..., take it in the dashboard")
	}
	if cfg.flush <= 0 || cfg.queue <= 0 {
		return nil, fmt.Errorf("botsight: flush interval and queue size must be positive")
	}
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.delivery == nil {
		cfg.delivery = &http.Client{Timeout: 10 * time.Second}
	}
	c.cfg = cfg
	c.send = newSender(c)
	return c, nil
}

func (c *Client) active() bool { return !c.off.Load() }

// Transport wraps the bot's HTTP transport (nil means http.DefaultTransport) so that
// Bot API calls become events.
func (c *Client) Transport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &transport{c: c, base: base}
}

// HTTPClient is an HTTP client for the bot library. It has no overall timeout: long
// polling holds a request open, and libraries bound it by the request context.
func (c *Client) HTTPClient() *http.Client {
	return &http.Client{Transport: c.Transport(nil)}
}

// UseAdapter tells the client that a framework adapter reports incoming updates, so the
// updates of getUpdates are not counted twice. Adapters call it.
func (c *Client) UseAdapter() { c.adapter.Store(true) }

// Update records an incoming update as raw Update JSON: handled is false when no handler
// of the bot took it, d is the time of handling. For frameworks without an adapter.
func (c *Client) Update(botID int64, raw []byte, handled bool, d time.Duration) {
	if !c.active() {
		return
	}
	c.setBotID(botID)
	ev, id, ok := updateEvent(raw, botID, time.Now().Add(-d))
	if !ok || !c.dedup.fresh(botID, id) {
		return
	}
	ev.Handled, ev.DurationMS = handled, d.Milliseconds()
	c.emit(ev)
}

// Step records one handler run: its name, the kind of update ("message",
// "callback_query", ...), the user and chat, the time and the error (nil, an error or a
// recovered panic value). Steps give the slow handlers and the handler errors.
func (c *Client) Step(botID int64, kind, handler string, userID, chatID int64, d time.Duration, failure any) {
	if !c.active() {
		return
	}
	ev := newEvent("step", kind, botID, time.Now().Add(-d))
	ev.Handler, ev.UserID, ev.ChatID, ev.DurationMS, ev.OK = truncate(handler, 256), userID, chatID, d.Milliseconds(), failure == nil
	if failure != nil {
		ev.Error = strPtr(errorName(failure))
	}
	c.emit(ev)
}

// errorName is a type, not a message: messages carry user data and explode cardinality.
func errorName(v any) string {
	if err, ok := v.(error); ok {
		return truncate(fmt.Sprintf("%T", err), 128)
	}
	return "panic"
}

var eventNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

const maxProps = 32

// Track records a business event, e.g. Track("purchase", userID, map[string]any{"amount": 990}).
// The name is snake_case up to 64 characters, up to 32 properties of strings, numbers or
// booleans. Invalid input is logged and dropped; Track never panics or blocks.
func (c *Client) Track(name string, userID int64, props map[string]any) {
	if !c.active() {
		return
	}
	if !eventNameRe.MatchString(name) {
		c.cfg.logger.Warn("botsight: event name must be snake_case up to 64 characters", "name", name)
		return
	}
	ev := newEvent("custom", name, c.botID.Load(), time.Now())
	ev.UserID, ev.OK, ev.Handled = userID, true, true
	if len(props) > 0 {
		ev.Props = map[string]any{}
		for k, v := range props {
			if len(ev.Props) == maxProps {
				c.cfg.logger.Warn("botsight: more than 32 props, the rest dropped", "name", name)
				break
			}
			switch x := v.(type) {
			case string:
				ev.Props[k] = truncate(x, 1024)
			case bool:
				ev.Props[k] = fmt.Sprint(x) // like the Python SDK: "true"/"false"
			case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
				ev.Props[k] = x
			case nil:
			default:
				ev.Props[k] = truncate(fmt.Sprint(x), 1024)
			}
		}
	}
	c.emit(ev)
}

// Dropped is the number of events lost so far: the queue was full or delivery failed.
func (c *Client) Dropped() int64 {
	if c.send == nil {
		return 0
	}
	return c.send.lost.Load()
}

// Close sends what is queued, waiting up to ctx, and stops the client.
func (c *Client) Close(ctx context.Context) error {
	if c.send == nil || c.off.Swap(true) {
		return nil
	}
	return c.send.close(ctx)
}

// setBotID remembers the bot; the first time it is seen, the start of the bot is an event:
// a deploy mark on the dashboard's charts.
func (c *Client) setBotID(id int64) {
	if id <= 0 {
		return
	}
	c.botID.CompareAndSwap(0, id)
	c.started.Do(func() {
		ev := newEvent("lifecycle", "bot_started", id, time.Now())
		ev.OK, ev.Handled = true, true
		ev.Props = map[string]any{"sdk_version": Version, "framework": c.cfg.framework, "framework_version": c.cfg.frameworkV, "go_version": runtime.Version()}
		c.emit(ev)
	})
}

func (c *Client) emit(ev event) {
	if c.send != nil && c.active() {
		c.send.enqueue(ev)
	}
}
