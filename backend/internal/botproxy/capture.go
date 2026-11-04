package botproxy

import (
	"io"
	"mime"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// captureSize: chat_id is among the first fields of every Bot API method, so a few
// kilobytes of the body are enough to find it without buffering a 50 MB document.
const captureSize = 4 << 10

// capture passes the request body to Telegram as it is read and keeps a copy of its
// beginning. When Telegram answers with an error, there is no Message in the answer,
// and chat_id is taken from this prefix.
type capture struct {
	io.ReadCloser
	prefix []byte
}

func newCapture(body io.ReadCloser) *capture {
	return &capture{ReadCloser: body, prefix: make([]byte, 0, 512)}
}

// Close does nothing: the transport closes the body when Telegram is unreachable, and
// the proxy still reads its beginning then. http.Server closes the request body itself.
func (c *capture) Close() error { return nil }

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

// requestChatID finds chat_id in the query or in the beginning of the body in any of
// the encodings the Bot API takes. "@channel" usernames give 0, as in the SDK.
func requestChatID(query url.Values, contentType string, prefix []byte) int64 {
	if v := query.Get("chat_id"); v != "" {
		return parseChatID(v)
	}
	mt, _, _ := mime.ParseMediaType(contentType)
	switch mt {
	case "application/json":
		if m := jsonChatID.FindSubmatch(prefix); m != nil {
			return parseChatID(string(m[1]))
		}
	case "application/x-www-form-urlencoded":
		s := string(prefix)
		if len(prefix) == captureSize { // the last pair may be cut
			if i := strings.LastIndexByte(s, '&'); i >= 0 {
				s = s[:i]
			}
		}
		if q, err := url.ParseQuery(s); err == nil {
			return parseChatID(q.Get("chat_id"))
		}
	case "multipart/form-data":
		if m := multipartChatID.FindSubmatch(prefix); m != nil {
			return parseChatID(string(m[1]))
		}
	}
	return 0
}

func parseChatID(s string) int64 {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
