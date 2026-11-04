// Package botproxy is the Bot API proxy mode (Botsight.md, 20): a gateway between a bot
// and api.telegram.org for bots without an SDK. The owner changes only the Bot API
// address, e.g. in aiogram
//
//	TelegramAPIServer.from_base("https://proxy.botsight.ru/k/<api key>")
//
// and the proxy turns the traffic into Botsight events: incoming updates from getUpdates
// and webhooks, outgoing calls with their errors. The events go to the collector in
// batches, as from an SDK; Botsight being down never breaks the bot.
//
// The bot token passes through the proxy but is never logged or stored: the bot is
// identified by the part of the token before the colon.
package botproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Emitter takes events of a project; *Sink in production.
type Emitter interface {
	Emit(key string, ev Event)
}

type Options struct {
	Telegram string // Bot API base URL, https://api.telegram.org or a local Bot API server
	// PublicURL is the proxy's address as Telegram sees it, for webhooks. Empty: setWebhook
	// passes unchanged and the updates of such bots go around the proxy.
	PublicURL string
	// UnsafeWebhooks allows http webhooks on private addresses: local development only.
	UnsafeWebhooks bool
	// MaxParseBytes: larger Bot API answers are passed through without events.
	MaxParseBytes int64
}

type Proxy struct {
	opts     Options
	upstream *url.URL
	events   Emitter
	hooks    HookStore
	dedup    *Dedup
	metrics  *Metrics
	logger   *slog.Logger
	rp       *httputil.ReverseProxy
	bots     *http.Client // webhooks of bots
}

func New(opts Options, events Emitter, hooks HookStore, dedup *Dedup, metrics *Metrics, logger *slog.Logger) (*Proxy, error) {
	up, err := url.Parse(strings.TrimSuffix(opts.Telegram, "/"))
	if err != nil || up.Host == "" {
		return nil, errors.New("telegram API URL must be absolute")
	}
	if opts.MaxParseBytes <= 0 {
		opts.MaxParseBytes = 16 << 20
	}
	opts.PublicURL = strings.TrimSuffix(opts.PublicURL, "/")
	p := &Proxy{
		opts: opts, upstream: up, events: events, hooks: hooks, dedup: dedup, metrics: metrics, logger: logger,
		bots: webhookClient(opts.UnsafeWebhooks),
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite:        p.rewrite,
		ModifyResponse: p.modifyResponse,
		ErrorHandler:   p.upstreamError,
		Transport:      telegramTransport(),
		ErrorLog:       slog.NewLogLogger(slog.DiscardHandler, slog.LevelError), // its messages hold URLs with tokens
	}
	return p, nil
}

// telegramTransport is tuned for long polling: every bot keeps a getUpdates open to the
// same host. The default MaxIdleConnsPerHost of 2 would reopen a TLS connection for
// nearly every request of thousands of bots.
func telegramTransport() *http.Transport {
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          8192,
		MaxIdleConnsPerHost:   8192,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		// getUpdates holds the answer up to its timeout (aiogram: 10 s, Telegram: up to 50 s)
		ResponseHeaderTimeout: 75 * time.Second,
	}
}

func (p *Proxy) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/k/{key}/{bot}/{method}", p.api)
	mux.HandleFunc("/k/{key}/file/{bot}/{path...}", p.file)
	mux.HandleFunc("POST /w/{id}", p.webhook)
	return mux
}

// call is what the proxy knows about one request of a bot.
type call struct {
	kind     string // polling, api, file
	key      string // empty: no events, the key does not look like one
	botID    int64  // 0: no events, the token does not look like one
	method   string // as sent; the Bot API is case-insensitive about it
	lower    string
	path     string // upstream path
	start    time.Time
	body     *capture
	hookID   string // setWebhook: the hook created for it
	query    url.Values
	ctype    string
	finished bool // an event was recorded in modifyResponse
}

type callKey struct{}

func callFrom(ctx context.Context) *call {
	c, _ := ctx.Value(callKey{}).(*call)
	return c
}

func parseBot(segment string) (token string, botID int64, ok bool) {
	token, ok = strings.CutPrefix(segment, "bot")
	if !ok || token == "" {
		return "", 0, false
	}
	botID, _ = parseToken(token)
	return token, botID, true
}

func eventsKey(key string) string {
	if strings.HasPrefix(key, "bs_") {
		return key
	}
	return ""
}

func (p *Proxy) api(w http.ResponseWriter, r *http.Request) {
	token, bot, ok := parseBot(r.PathValue("bot"))
	method := r.PathValue("method")
	if !ok || method == "" {
		writeTelegramError(w, http.StatusNotFound, "Not Found")
		return
	}
	c := &call{
		kind: "api", key: eventsKey(r.PathValue("key")), botID: bot, method: method,
		lower: strings.ToLower(method), path: "/bot" + token + "/" + method, start: time.Now(),
		query: r.URL.Query(), ctype: r.Header.Get("Content-Type"),
	}
	if c.lower == "getupdates" {
		c.kind = "polling"
	}
	if c.lower == "setwebhook" {
		if !p.prepareWebhook(w, r, c) {
			return
		}
	}
	if r.Body != nil && r.Body != http.NoBody {
		c.body = newCapture(r.Body)
		r.Body = c.body
	}
	p.serve(w, r, c)
}

func (p *Proxy) file(w http.ResponseWriter, r *http.Request) {
	token, _, ok := parseBot(r.PathValue("bot"))
	if !ok {
		writeTelegramError(w, http.StatusNotFound, "Not Found")
		return
	}
	// files are streamed as they are: no events, no parsing
	p.serve(w, r, &call{kind: "file", path: "/file/bot" + token + "/" + r.PathValue("path"), start: time.Now()})
}

func (p *Proxy) serve(w http.ResponseWriter, r *http.Request, c *call) {
	sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
	p.rp.ServeHTTP(sw, r.WithContext(context.WithValue(r.Context(), callKey{}, c)))
	p.metrics.request(c.kind, sw.code, time.Since(c.start).Seconds())
}

func (p *Proxy) rewrite(pr *httputil.ProxyRequest) {
	c := callFrom(pr.In.Context())
	pr.Out.URL.Scheme = p.upstream.Scheme
	pr.Out.URL.Host = p.upstream.Host
	pr.Out.URL.Path = p.upstream.Path + c.path
	pr.Out.URL.RawPath = ""
	pr.Out.Host = p.upstream.Host
	if c.kind != "file" {
		// the answers are parsed: let the transport ask for gzip and unpack it itself
		pr.Out.Header.Del("Accept-Encoding")
	}
}

// modifyResponse reads the Bot API answer, records events and passes it on unchanged
// (except getWebhookInfo, which shows the bot its own address).
func (p *Proxy) modifyResponse(resp *http.Response) error {
	c := callFrom(resp.Request.Context())
	if c == nil || c.kind == "file" {
		return nil
	}
	buf, err := io.ReadAll(io.LimitReader(resp.Body, p.opts.MaxParseBytes+1))
	if err != nil {
		return err
	}
	if int64(len(buf)) > p.opts.MaxParseBytes {
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(buf), resp.Body), resp.Body}
		return nil
	}
	_ = resp.Body.Close()
	duration := time.Since(c.start)

	var ans apiResponse
	parsed := json.Unmarshal(buf, &ans) == nil
	if !parsed { // not a Bot API answer (a proxy error page): an error with the HTTP status
		ans = apiResponse{ErrorCode: resp.StatusCode, Description: http.StatusText(resp.StatusCode)}
	}
	ctx := resp.Request.Context()
	switch c.lower {
	case "getupdates":
		if ans.OK {
			p.updates(c.key, c.botID, ans.Result)
		}
	case "setwebhook":
		p.finishWebhook(ctx, c, ans.OK)
	case "deletewebhook":
		if ans.OK && p.hooks != nil {
			if err := p.hooks.DeleteBot(context.WithoutCancel(ctx), c.botID); err != nil {
				p.logger.Warn("delete webhook", "bot_id", c.botID, "err", err)
			}
		}
	case "getwebhookinfo":
		if ans.OK {
			if b, ok := p.restoreWebhookURL(ctx, buf); ok {
				buf = b
			}
		}
	}
	if c.kind != "polling" {
		p.outgoing(c, ans, duration)
	}
	c.finished = true
	resp.Body = io.NopCloser(bytes.NewReader(buf))
	resp.ContentLength = int64(len(buf))
	resp.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	return nil
}

// updates records the updates of a getUpdates answer. The answer itself goes to the bot
// untouched; duplicates are only not counted twice.
func (p *Proxy) updates(key string, botID int64, result json.RawMessage) {
	if key == "" || botID == 0 {
		return
	}
	var list []json.RawMessage
	if json.Unmarshal(result, &list) != nil {
		return
	}
	now := time.Now()
	for _, raw := range list {
		p.update(key, botID, raw, now)
	}
}

func (p *Proxy) update(key string, botID int64, raw json.RawMessage, at time.Time) {
	ev, id, ok := UpdateEvent(raw, botID, at)
	if !ok {
		return
	}
	if !p.dedup.Fresh(botID, id) {
		p.metrics.duplicate()
		return
	}
	p.events.Emit(key, ev)
}

// outgoing records a Bot API call: its chat comes from the sent Message or, on an error,
// from the beginning of the request.
func (p *Proxy) outgoing(c *call, ans apiResponse, d time.Duration) {
	if c.key == "" || c.botID == 0 {
		return
	}
	ev := newEvent("outgoing", c.method, c.botID, c.start)
	ev.DurationMS = d.Milliseconds()
	if ans.OK {
		ev.OK = true
		ev.ChatID = resultChatID(ans.Result)
	} else {
		code, name := NormalizeError(ans.ErrorCode, ans.Description, ans.Parameters != nil && ans.Parameters.MigrateToChatID != 0)
		ev.ErrorCode, ev.Error = code, strPtr(name)
	}
	if ev.ChatID == 0 {
		var prefix []byte
		if c.body != nil {
			prefix = c.body.prefix
		}
		ev.ChatID = requestChatID(c.query, c.ctype, prefix)
	}
	p.events.Emit(c.key, ev)
}

// upstreamError: Telegram did not answer. The bot gets 502 in the Bot API format, and
// the call is recorded as a network error, like TelegramNetworkError in the SDK.
func (p *Proxy) upstreamError(w http.ResponseWriter, r *http.Request, err error) {
	c := callFrom(r.Context())
	if errors.Is(err, context.Canceled) || r.Context().Err() != nil {
		w.WriteHeader(499) // the bot went away, e.g. stopped polling
		return
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err // url.Error prints the URL, and the URL holds the token
	}
	if c != nil {
		p.logger.Warn("telegram unreachable", "method", c.method, "bot_id", c.botID, "err", err)
		if c.kind == "api" && !c.finished {
			p.outgoingNetwork(c)
		}
		if c.hookID != "" && p.hooks != nil {
			_ = p.hooks.Delete(context.WithoutCancel(r.Context()), c.hookID)
		}
	}
	writeTelegramError(w, http.StatusBadGateway, "Bad Gateway: Telegram is unreachable from the Botsight proxy")
}

func (p *Proxy) outgoingNetwork(c *call) {
	if c.key == "" || c.botID == 0 {
		return
	}
	ev := newEvent("outgoing", c.method, c.botID, c.start)
	ev.DurationMS = time.Since(c.start).Milliseconds()
	ev.Error = strPtr("network")
	var prefix []byte
	if c.body != nil {
		// the connection failed before the body was sent: read its beginning now
		_, _ = io.CopyN(io.Discard, c.body, int64(captureSize-len(c.body.prefix)))
		prefix = c.body.prefix
	}
	ev.ChatID = requestChatID(c.query, c.ctype, prefix)
	p.events.Emit(c.key, ev)
}

func writeTelegramError(w http.ResponseWriter, code int, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "error_code": code, "description": description})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets ReverseProxy flush and hijack through the wrapper.
func (s *statusWriter) Unwrap() http.ResponseWriter { return s.ResponseWriter }
