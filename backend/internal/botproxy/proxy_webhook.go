package botproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxUpdateBody = 1 << 20

// prepareWebhook rewrites setWebhook so that Telegram calls the proxy. It returns false
// when it already answered the bot.
func (p *Proxy) prepareWebhook(w http.ResponseWriter, r *http.Request, c *call) bool {
	if p.opts.PublicURL == "" || p.hooks == nil || c.botID == 0 || c.key == "" {
		return true // pass unchanged: incoming updates go around the proxy
	}
	params, err := readParams(r)
	if errors.Is(err, errParamsTooLarge) {
		writeTelegramError(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large")
		return false
	}
	if err != nil {
		writeTelegramError(w, http.StatusBadRequest, "Bad Request: cannot read setWebhook parameters")
		return false
	}
	target := params.Get("url")
	if target != "" {
		if err := checkWebhookURL(target, p.opts.UnsafeWebhooks); err != nil {
			writeTelegramError(w, http.StatusBadRequest, "Bad Request: "+err.Error())
			return false
		}
		c.hookID = newHookID()
		if err := p.hooks.Put(r.Context(), c.hookID, Hook{Key: c.key, BotID: c.botID, URL: target}); err != nil {
			// the bot must keep working: set its own address, without events
			p.logger.Warn("webhook store unavailable, setWebhook passes unchanged", "bot_id", c.botID, "err", err)
			c.hookID = ""
		} else {
			params.Set("url", p.opts.PublicURL+"/w/"+c.hookID)
		}
	}
	// an empty url removes the webhook: finishWebhook forgets the bot's hook
	body, ctype, query, err := params.Encode()
	if err != nil {
		writeTelegramError(w, http.StatusBadRequest, "Bad Request: cannot encode setWebhook parameters")
		return false
	}
	r.URL.RawQuery = query
	c.query = r.URL.Query()
	if body == nil {
		r.Body, r.ContentLength = http.NoBody, 0
		r.Header.Del("Content-Type")
	} else {
		r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		r.Header.Set("Content-Type", ctype)
		c.ctype = ctype
	}
	return true
}

// finishWebhook makes the new hook the bot's one when Telegram accepted it.
func (p *Proxy) finishWebhook(ctx context.Context, c *call, ok bool) {
	if p.hooks == nil || c.botID == 0 || p.opts.PublicURL == "" {
		return
	}
	ctx = context.WithoutCancel(ctx)
	var err error
	switch {
	case c.hookID != "" && ok:
		err = p.hooks.Activate(ctx, c.botID, c.hookID)
	case c.hookID != "":
		err = p.hooks.Delete(ctx, c.hookID) // the old hook keeps working
	case ok && c.key != "":
		err = p.hooks.DeleteBot(ctx, c.botID) // url was empty: the webhook is removed
	}
	if err != nil {
		p.logger.Warn("webhook store", "bot_id", c.botID, "err", err)
	}
}

// restoreWebhookURL shows the bot its own address in getWebhookInfo. Frameworks compare
// it with theirs on start, and would call setWebhook on every start otherwise.
func (p *Proxy) restoreWebhookURL(ctx context.Context, body []byte) ([]byte, bool) {
	if p.opts.PublicURL == "" || p.hooks == nil {
		return nil, false
	}
	var ans map[string]json.RawMessage
	if json.Unmarshal(body, &ans) != nil {
		return nil, false
	}
	var info map[string]json.RawMessage
	if json.Unmarshal(ans["result"], &info) != nil {
		return nil, false
	}
	var current string
	if json.Unmarshal(info["url"], &current) != nil {
		return nil, false
	}
	id, ok := strings.CutPrefix(current, p.opts.PublicURL+"/w/")
	if !ok {
		return nil, false
	}
	h, found, err := p.hooks.Get(ctx, id)
	if err != nil || !found {
		return nil, false
	}
	info["url"], _ = json.Marshal(h.URL)
	ans["result"], _ = json.Marshal(info)
	out, err := json.Marshal(ans)
	return out, err == nil
}

// webhook takes an update from Telegram, records it and forwards it to the bot. The
// bot's answer, which may itself be a Bot API call, goes back to Telegram as it is.
// A duplicate (Telegram retries a webhook that failed) is forwarded but not counted.
func (p *Proxy) webhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	code := http.StatusOK
	defer func() { p.metrics.request("webhook", code, time.Since(start).Seconds()) }()
	if p.hooks == nil {
		code = http.StatusNotFound
		http.NotFound(w, r)
		return
	}

	h, found, err := p.hooks.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		code = http.StatusServiceUnavailable
		http.Error(w, "webhook store unavailable", code) // Telegram retries later
		return
	}
	if !found {
		code = http.StatusNotFound
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxUpdateBody+1))
	if err != nil || len(body) > maxUpdateBody {
		code = http.StatusBadRequest
		http.Error(w, "bad update", code)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		code = http.StatusBadGateway
		http.Error(w, "bad webhook url", code)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if s := r.Header.Get("X-Telegram-Bot-Api-Secret-Token"); s != "" {
		req.Header.Set("X-Telegram-Bot-Api-Secret-Token", s) // the bot checks it itself
	}
	resp, err := p.bots.Do(req)
	if err != nil {
		code = http.StatusBadGateway
		p.logger.Warn("bot webhook unreachable", "bot_id", h.BotID, "err", errors.Unwrap(err))
		http.Error(w, "bot unreachable", code) // Telegram retries; the retry is not counted twice
		return
	}
	defer resp.Body.Close()
	// counted only when the bot took it: a refused update will come again
	if resp.StatusCode < 300 {
		p.update(h.Key, h.BotID, body, start)
	}
	code = resp.StatusCode
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, io.LimitReader(resp.Body, maxUpdateBody))
}
