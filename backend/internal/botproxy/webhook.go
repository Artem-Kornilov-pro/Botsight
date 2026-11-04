package botproxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// Webhooks through the proxy (Botsight.md, 20): setWebhook gets the proxy's own address
// /w/{id} instead of the bot's, the bot's address is kept in the store, and updates are
// forwarded to it with the bot's answer relayed back to Telegram.

// Hook is where updates of a bot go.
type Hook struct {
	Key   string `json:"key"` // project API key for the events
	BotID int64  `json:"bot_id"`
	URL   string `json:"url"` // the bot's own webhook
}

// HookStore keeps hooks by id. A hook is active once Telegram accepted it; activating
// removes the previous hook of the bot.
type HookStore interface {
	Put(ctx context.Context, id string, h Hook) error
	Get(ctx context.Context, id string) (Hook, bool, error)
	Activate(ctx context.Context, botID int64, id string) error
	Delete(ctx context.Context, id string) error
	DeleteBot(ctx context.Context, botID int64) error
}

func newHookID() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// --- Redis ---

// RedisHooks stores proxy:hook:{id} -> Hook and proxy:hook:bot:{bot_id} -> id.
type RedisHooks struct{ R redis.UniversalClient }

func hookKey(id string) string      { return "proxy:hook:" + id }
func botHookKey(botID int64) string { return "proxy:hook:bot:" + strconv.FormatInt(botID, 10) }

func (s RedisHooks) Put(ctx context.Context, id string, h Hook) error {
	b, err := json.Marshal(h)
	if err != nil {
		return err
	}
	// not active yet: if Telegram refuses the address, the key expires by itself
	return s.R.Set(ctx, hookKey(id), b, time.Hour).Err()
}

func (s RedisHooks) Get(ctx context.Context, id string) (Hook, bool, error) {
	b, err := s.R.Get(ctx, hookKey(id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Hook{}, false, nil
	}
	if err != nil {
		return Hook{}, false, err
	}
	var h Hook
	return h, true, json.Unmarshal(b, &h)
}

func (s RedisHooks) Activate(ctx context.Context, botID int64, id string) error {
	old, err := s.R.GetSet(ctx, botHookKey(botID), id).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return err
	}
	pipe := s.R.TxPipeline()
	pipe.Persist(ctx, hookKey(id))
	if old != "" && old != id {
		pipe.Del(ctx, hookKey(old))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s RedisHooks) Delete(ctx context.Context, id string) error {
	return s.R.Del(ctx, hookKey(id)).Err()
}

func (s RedisHooks) DeleteBot(ctx context.Context, botID int64) error {
	id, err := s.R.GetDel(ctx, botHookKey(botID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil
	}
	if err != nil {
		return err
	}
	return s.R.Del(ctx, hookKey(id)).Err()
}

// --- memory, for tests and a single instance ---

type MemoryHooks struct {
	mu    sync.Mutex
	hooks map[string]Hook
	bots  map[int64]string
}

func NewMemoryHooks() *MemoryHooks {
	return &MemoryHooks{hooks: map[string]Hook{}, bots: map[int64]string{}}
}

func (m *MemoryHooks) Put(_ context.Context, id string, h Hook) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hooks[id] = h
	return nil
}

func (m *MemoryHooks) Get(_ context.Context, id string) (Hook, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hooks[id]
	return h, ok, nil
}

func (m *MemoryHooks) Activate(_ context.Context, botID int64, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if old, ok := m.bots[botID]; ok && old != id {
		delete(m.hooks, old)
	}
	m.bots[botID] = id
	return nil
}

func (m *MemoryHooks) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hooks, id)
	return nil
}

func (m *MemoryHooks) DeleteBot(_ context.Context, botID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.hooks, m.bots[botID])
	delete(m.bots, botID)
	return nil
}

// --- setWebhook parameters ---

// maxParamsBody: setWebhook carries at most a public key certificate.
const maxParamsBody = 1 << 20

var errParamsTooLarge = errors.New("setWebhook body is larger than 1 MiB")

// params are the parameters of a Bot API call in any of its encodings: the query, JSON,
// a urlencoded form or multipart/form-data. Get and Set work on string fields; Encode
// writes them back in the same encoding, other fields (a certificate) unchanged.
type params struct {
	query     url.Values
	kind      string // "", "json", "form", "multipart"
	json      map[string]json.RawMessage
	form      url.Values
	parts     []part
	inQuery   bool
	boundary  string
	setFields map[string]string
}

type part struct {
	header textproto.MIMEHeader
	name   string
	body   []byte
}

func readParams(r *http.Request) (*params, error) {
	p := &params{query: r.URL.Query(), setFields: map[string]string{}}
	if r.Body == nil || r.Body == http.NoBody {
		return p, nil
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxParamsBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxParamsBody {
		return nil, errParamsTooLarge
	}
	mt, mp, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	switch {
	case len(body) == 0:
	case mt == "application/json":
		p.kind = "json"
		if err := json.Unmarshal(body, &p.json); err != nil {
			return nil, err
		}
	case mt == "application/x-www-form-urlencoded":
		p.kind = "form"
		if p.form, err = url.ParseQuery(string(body)); err != nil {
			return nil, err
		}
	case mt == "multipart/form-data":
		p.kind, p.boundary = "multipart", mp["boundary"]
		mr := multipart.NewReader(bytes.NewReader(body), p.boundary)
		for {
			pt, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			b, err := io.ReadAll(pt)
			if err != nil {
				return nil, err
			}
			p.parts = append(p.parts, part{header: pt.Header, name: pt.FormName(), body: b})
		}
	default:
		return nil, fmt.Errorf("unsupported content type %q", mt)
	}
	return p, nil
}

func (p *params) Get(name string) string {
	switch p.kind {
	case "json":
		var s string
		if raw, ok := p.json[name]; ok && json.Unmarshal(raw, &s) == nil {
			return s
		}
	case "form":
		if p.form.Has(name) {
			return p.form.Get(name)
		}
	case "multipart":
		for _, pt := range p.parts {
			if pt.name == name {
				return string(pt.body)
			}
		}
	}
	return p.query.Get(name)
}

func (p *params) Set(name, value string) { p.setFields[name] = value }

// Encode returns the new body, its content type and the new query.
func (p *params) Encode() ([]byte, string, string, error) {
	q := url.Values{}
	for k, v := range p.query {
		q[k] = v
	}
	for name, v := range p.setFields {
		if q.Has(name) || p.kind == "" {
			q.Set(name, v)
		}
	}
	switch p.kind {
	case "json":
		for name, v := range p.setFields {
			if _, ok := p.json[name]; ok {
				p.json[name], _ = json.Marshal(v)
			}
		}
		b, err := json.Marshal(p.json)
		return b, "application/json", q.Encode(), err
	case "form":
		for name, v := range p.setFields {
			if p.form.Has(name) {
				p.form.Set(name, v)
			}
		}
		return []byte(p.form.Encode()), "application/x-www-form-urlencoded", q.Encode(), nil
	case "multipart":
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		if err := mw.SetBoundary(p.boundary); err != nil {
			return nil, "", "", err
		}
		for _, pt := range p.parts {
			body := pt.body
			if v, ok := p.setFields[pt.name]; ok {
				body = []byte(v)
			}
			w, err := mw.CreatePart(pt.header)
			if err != nil {
				return nil, "", "", err
			}
			if _, err := w.Write(body); err != nil {
				return nil, "", "", err
			}
		}
		if err := mw.Close(); err != nil {
			return nil, "", "", err
		}
		return buf.Bytes(), mw.FormDataContentType(), q.Encode(), nil
	}
	return nil, "", q.Encode(), nil
}

// --- forwarding updates to the bot ---

// webhookPorts are the ports Telegram itself delivers webhooks to.
var webhookPorts = map[string]bool{"": true, "443": true, "80": true, "88": true, "8443": true}

// checkWebhookURL accepts what Telegram would accept: https on its ports. unsafe allows
// http and any port for local development.
func checkWebhookURL(raw string, unsafe bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("webhook url is not an absolute URL")
	}
	if unsafe {
		return nil
	}
	if u.Scheme != "https" || !webhookPorts[u.Port()] {
		return errors.New("webhook url must be https on port 443, 80, 88 or 8443")
	}
	return nil
}

// errPrivateAddress: the bot's address resolved to the proxy's own network.
var errPrivateAddress = errors.New("webhook address is private")

// webhookClient calls bots' webhooks. The address comes from the bot owner, so it could
// point at the proxy's internal network (SSRF). The dialer checks the resolved IP right
// before connecting, which also defeats DNS rebinding between a check and the request.
func webhookClient(unsafe bool) *http.Client {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	if !unsafe {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip := net.ParseIP(host)
			if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() {
				return errPrivateAddress
			}
			return nil
		}
	}
	return &http.Client{
		Timeout: 60 * time.Second, // Telegram itself gives up on a webhook after about a minute
		Transport: &http.Transport{
			DialContext:         dialer.DialContext,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
