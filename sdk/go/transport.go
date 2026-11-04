package botsight

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Transport records the bot's Bot API calls: outgoing calls with their errors, and the
// updates of getUpdates when no framework adapter reports them. The request goes to
// Telegram as it is, streamed; the answer is read, parsed and handed back unchanged.
// Files (/file/...) are not touched. Nothing the transport does can fail a request.
type transport struct {
	c    *Client
	base http.RoundTripper
}

// captureSize: chat_id is among the first fields of every method.
const captureSize = 4 << 10

// maxParse: larger answers are passed through without events.
const maxParse = 16 << 20

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, method, ok := botPath(req.URL.Path)
	if !ok || !t.c.active() {
		return t.base.RoundTrip(req)
	}
	botID := parseToken(token)
	t.c.setBotID(botID)
	lower := strings.ToLower(method)

	var body *capture
	if req.Body != nil && req.Body != http.NoBody {
		body = &capture{ReadCloser: req.Body}
		req = req.Clone(req.Context())
		req.Body = body
	}
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	d := time.Since(start)
	if err != nil {
		if lower != "getupdates" && req.Context().Err() == nil {
			t.c.emit(t.network(botID, method, req, body, start, d))
		}
		return resp, err
	}

	buf, rerr := io.ReadAll(io.LimitReader(resp.Body, maxParse+1))
	if rerr != nil || len(buf) > maxParse {
		// unreadable or too big to parse: hand back what was read and the rest
		resp.Body = struct {
			io.Reader
			io.Closer
		}{io.MultiReader(bytes.NewReader(buf), resp.Body), resp.Body}
		return resp, nil
	}
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(buf))

	var ans apiResponse
	if json.Unmarshal(buf, &ans) != nil {
		ans = apiResponse{ErrorCode: resp.StatusCode, Description: http.StatusText(resp.StatusCode)}
	}
	if lower == "getupdates" {
		if ans.OK && !t.c.adapter.Load() {
			t.updates(botID, ans.Result)
		}
		return resp, nil
	}
	ev := newEvent("outgoing", method, botID, start)
	ev.DurationMS = d.Milliseconds()
	if ans.OK {
		ev.OK = true
		ev.ChatID = resultChatID(ans.Result)
	} else {
		code, name := normalizeError(ans.ErrorCode, ans.Description, ans.Parameters != nil && ans.Parameters.MigrateToChatID != 0)
		ev.ErrorCode, ev.Error = code, strPtr(name)
	}
	if ev.ChatID == 0 {
		ev.ChatID = requestChatID(req, body)
	}
	t.c.emit(ev)
	return resp, nil
}

func (t *transport) updates(botID int64, result json.RawMessage) {
	var list []json.RawMessage
	if json.Unmarshal(result, &list) != nil {
		return
	}
	now := time.Now()
	for _, raw := range list {
		if ev, id, ok := updateEvent(raw, botID, now); ok && t.c.dedup.fresh(botID, id) {
			t.c.emit(ev)
		}
	}
}

func (t *transport) network(botID int64, method string, req *http.Request, body *capture, start time.Time, d time.Duration) event {
	ev := newEvent("outgoing", method, botID, start)
	ev.DurationMS = d.Milliseconds()
	ev.Error = strPtr("network")
	// only what was sent before the failure: the base transport closed the body, and
	// reading a closed pipe would block the library's writer
	ev.ChatID = requestChatID(req, body)
	return ev
}

// botPath splits /bot<token>/<method>; servers may sit under a prefix path.
func botPath(p string) (token, method string, ok bool) {
	i := strings.LastIndex(p, "/bot")
	if i < 0 || strings.Contains(p[:i+1], "/file/") {
		return "", "", false
	}
	token, method, ok = strings.Cut(p[i+len("/bot"):], "/")
	return token, method, ok && token != "" && method != "" && !strings.Contains(method, "/")
}

// capture passes the body through and keeps a copy of its beginning. Close is left to
// the HTTP client, which closes the original body itself.
type capture struct {
	io.ReadCloser
	prefix []byte
}

func (c *capture) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if room := captureSize - len(c.prefix); room > 0 && n > 0 {
		c.prefix = append(c.prefix, p[:min(n, room)]...)
	}
	return n, err
}

var (
	jsonChatID      = regexp.MustCompile(`"chat_id"\s*:\s*"?(-?\d+)`)
	multipartChatID = regexp.MustCompile(`name="?chat_id"?\r\n(?:[^\r\n]+\r\n)*\r\n(-?\d+)\r\n`)
)

// requestChatID finds chat_id in the query or the beginning of the body, in any encoding
// the Bot API takes. "@channel" usernames give 0, as in the Python SDK.
func requestChatID(req *http.Request, body *capture) int64 {
	if v := req.URL.Query().Get("chat_id"); v != "" {
		return parseInt(v)
	}
	if body == nil {
		return 0
	}
	mt, _, _ := mime.ParseMediaType(req.Header.Get("Content-Type"))
	switch mt {
	case "application/json":
		if m := jsonChatID.FindSubmatch(body.prefix); m != nil {
			return parseInt(string(m[1]))
		}
	case "application/x-www-form-urlencoded":
		s := string(body.prefix)
		if len(body.prefix) == captureSize {
			if i := strings.LastIndexByte(s, '&'); i >= 0 {
				s = s[:i]
			}
		}
		if q, err := url.ParseQuery(s); err == nil {
			return parseInt(q.Get("chat_id"))
		}
	case "multipart/form-data":
		if m := multipartChatID.FindSubmatch(body.prefix); m != nil {
			return parseInt(string(m[1]))
		}
	}
	return 0
}

func parseInt(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// uuid7 is a UUIDv7 (time-ordered) without a dependency.
func uuid7(at time.Time) string {
	var b [16]byte
	ms := uint64(at.UnixMilli())
	binary.BigEndian.PutUint64(b[:8], ms<<16)
	if _, err := rand.Read(b[6:]); err != nil {
		panic(errors.New("botsight: no randomness"))
	}
	b[6] = b[6]&0x0f | 0x70
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
