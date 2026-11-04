package botproxy

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestUpdateEvent(t *testing.T) {
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		raw   string
		check func(Event) bool
	}{
		{`{"update_id":10,"message":{"message_id":1,"from":{"id":42,"username":"ann","language_code":"ru","is_premium":true},
		   "chat":{"id":42,"type":"private"},"text":"/start@labbot vk_ads_oct"}}`,
			func(e Event) bool {
				return e.Type == "update" && e.Name == "message" && e.UserID == 42 && e.ChatID == 42 && e.ChatType == "private" &&
					e.ContentType == "text" && e.Command == "start" && e.StartParam == "vk_ads_oct" && e.TextLen == 24 &&
					e.User != nil && e.User.Lang == "ru" && e.User.IsPremium && e.User.HasUsername && e.Handled
			}},
		{`{"update_id":11,"callback_query":{"id":"x","from":{"id":42},"message":{"chat":{"id":42,"type":"private"}},"data":"lab:lab2"}}`,
			func(e Event) bool {
				return e.Name == "callback_query" && e.CallbackData == "lab:lab2" && e.ChatID == 42 && !e.User.HasUsername
			}},
		{`{"update_id":12,"my_chat_member":{"chat":{"id":42,"type":"private"},"from":{"id":42},"new_chat_member":{"status":"kicked"}}}`,
			func(e Event) bool { return e.Name == "my_chat_member" && e.MemberStatus == "kicked" && e.UserID == 42 }},
		{`{"update_id":13,"message":{"from":{"id":7},"chat":{"id":-100,"type":"supergroup"},"photo":[{}],"caption":"привет"}}`,
			func(e Event) bool {
				return e.ContentType == "photo" && e.TextLen == 6 && e.Command == "" && e.ChatID == -100
			}},
		{`{"update_id":14,"poll_answer":{"poll_id":"p","user":{"id":9}}}`,
			func(e Event) bool { return e.Name == "poll_answer" && e.UserID == 9 }},
	}
	for _, c := range cases {
		ev, id, ok := UpdateEvent([]byte(c.raw), 777, at)
		if !ok || id < 10 || ev.BotID != 777 || ev.TS != "2026-11-04T12:00:00.000Z" || len(ev.EventID) != 36 || !c.check(ev) {
			t.Errorf("%s\n-> %+v (ok=%v)", c.raw, ev, ok)
		}
	}
	for _, bad := range []string{`[]`, `{"message":{}}`, `{"update_id":1}`, `nope`} {
		if _, _, ok := UpdateEvent([]byte(bad), 1, at); ok {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestNormalizeError(t *testing.T) {
	cases := []struct {
		code    int
		desc    string
		migrate bool
		want    string
	}{
		{403, "Forbidden: bot was blocked by the user", false, "bot_blocked"},
		{403, "Forbidden: user is deactivated", false, "user_deactivated"},
		{403, "Forbidden: bot can't initiate conversation", false, "forbidden"},
		{429, "Too Many Requests: retry after 5", false, "rate_limited"},
		{400, "Bad Request: group chat was upgraded", true, "migrated"},
		{400, "Bad Request: message is not modified", false, "not_modified"},
		{400, "Bad Request: chat not found", false, "bad_request"},
		{401, "Unauthorized", false, "unauthorized"},
		{409, "Conflict: terminated by other getUpdates request", false, "conflict"},
		{502, "Bad Gateway", false, "server_error"},
		{0, "", false, "api_error"},
	}
	for _, c := range cases {
		if _, got := NormalizeError(c.code, c.desc, c.migrate); got != c.want {
			t.Errorf("%d %q: %s, want %s", c.code, c.desc, got, c.want)
		}
	}
}

func TestResultChatID(t *testing.T) {
	if id := resultChatID([]byte(`{"message_id":1,"chat":{"id":-5}}`)); id != -5 {
		t.Fatal(id)
	}
	if id := resultChatID([]byte(`[{"chat":{"id":8}},{"chat":{"id":8}}]`)); id != 8 {
		t.Fatal(id)
	}
	if id := resultChatID([]byte(`true`)); id != 0 {
		t.Fatal(id)
	}
}

func TestDedupWindow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	d := NewDedup()
	d.now = func() time.Time { return now }
	fresh := func(id int64) bool { return d.Fresh(1, id) }

	for id := int64(100); id < 110; id++ {
		if !fresh(id) {
			t.Fatalf("%d is new", id)
		}
	}
	for id := int64(100); id < 110; id++ { // the bot restarted and polled again
		if fresh(id) {
			t.Fatalf("%d is a repeat", id)
		}
	}
	// out of order, as webhooks with max_connections > 1 deliver
	if !fresh(115) || !fresh(112) || fresh(112) || !fresh(111) {
		t.Fatal("out of order within the window")
	}
	// jumps across the 64-bit words of the bitmap keep the old bits
	if !fresh(115+70) || fresh(115) || !fresh(113) || !fresh(115+200) || fresh(112) || fresh(115+70) {
		t.Fatal("bits lost on a shift")
	}
	if fresh(115 + 200 - windowSize) { // left the window: within 24 hours it can only be a repeat
		t.Fatal("too old accepted")
	}
	if !fresh(10_000) || fresh(10_000) {
		t.Fatal("a jump beyond the window")
	}
	if !d.Fresh(2, 100) { // windows are per bot
		t.Fatal("another bot")
	}

	// after a week of silence Telegram starts from a random, possibly lower, id
	now = now.Add(8 * 24 * time.Hour)
	if !fresh(5) || fresh(5) || !fresh(6) {
		t.Fatal("window not forgotten")
	}
	now = now.Add(25 * time.Hour)
	d.Sweep()
	if d.Bots() != 0 {
		t.Fatalf("%d windows after sweep", d.Bots())
	}
}

func TestRequestChatID(t *testing.T) {
	var mp bytes.Buffer
	mw := multipart.NewWriter(&mp)
	_ = mw.WriteField("chat_id", "-1001234")
	fw, _ := mw.CreateFormFile("document", "lab.docx")
	_, _ = fw.Write(bytes.Repeat([]byte("x"), 10_000))
	_ = mw.Close()

	cases := []struct {
		query, ctype, body string
		want               int64
	}{
		{"", "application/json", `{"chat_id": 42, "text": "hi"}`, 42},
		{"", "application/json; charset=utf-8", `{"text":"x","chat_id":"-77"}`, -77},
		{"", "application/x-www-form-urlencoded", "text=hi&chat_id=55", 55},
		{"", mw.FormDataContentType(), mp.String(), -1001234},
		{"chat_id=9", "", "", 9},
		{"", "application/json", `{"chat_id":"@channel"}`, 0},
		{"", "text/plain", "chat_id=1", 0},
	}
	for _, c := range cases {
		q, _ := url.ParseQuery(c.query)
		body := []byte(c.body)
		if len(body) > captureSize {
			body = body[:captureSize]
		}
		if got := requestChatID(q, c.ctype, body); got != c.want {
			t.Errorf("%s %q: %d, want %d", c.ctype, c.body[:min(len(c.body), 40)], got, c.want)
		}
	}
}

func TestCaptureKeepsOnlyThePrefix(t *testing.T) {
	c := newCapture(io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("a"), 3*captureSize))))
	n, err := io.Copy(io.Discard, c)
	if err != nil || n != 3*captureSize || len(c.prefix) != captureSize {
		t.Fatalf("copied %d, kept %d, %v", n, len(c.prefix), err)
	}
}

func TestParamsRewrite(t *testing.T) {
	newReq := func(ctype, body, query string) *http.Request {
		r := httptest.NewRequest("POST", "/x?"+query, strings.NewReader(body))
		if ctype != "" {
			r.Header.Set("Content-Type", ctype)
		}
		return r
	}
	var mp bytes.Buffer
	mw := multipart.NewWriter(&mp)
	_ = mw.WriteField("url", "https://bot.example/hook")
	fw, _ := mw.CreateFormFile("certificate", "cert.pem")
	_, _ = fw.Write([]byte("-----BEGIN CERTIFICATE-----"))
	_ = mw.WriteField("secret_token", "s3")
	_ = mw.Close()

	for _, r := range []*http.Request{
		newReq("application/json", `{"url":"https://bot.example/hook","secret_token":"s3","max_connections":40}`, ""),
		newReq("application/x-www-form-urlencoded", "url=https%3A%2F%2Fbot.example%2Fhook&secret_token=s3", ""),
		newReq(mw.FormDataContentType(), mp.String(), ""),
		newReq("", "", "url=https%3A%2F%2Fbot.example%2Fhook&secret_token=s3"),
	} {
		p, err := readParams(r)
		if err != nil || p.Get("url") != "https://bot.example/hook" {
			t.Fatalf("%s: %v %q", r.Header.Get("Content-Type"), err, p.Get("url"))
		}
		p.Set("url", "https://proxy.example/w/abc")
		body, ctype, query, err := p.Encode()
		if err != nil {
			t.Fatal(err)
		}
		back := newReq(ctype, string(body), query)
		again, err := readParams(back)
		if err != nil || again.Get("url") != "https://proxy.example/w/abc" || again.Get("secret_token") != "s3" {
			t.Fatalf("%s round trip: %v %q %q", ctype, err, again.Get("url"), again.Get("secret_token"))
		}
		if strings.HasPrefix(ctype, "multipart/") && !bytes.Contains(body, []byte("BEGIN CERTIFICATE")) {
			t.Fatal("certificate lost")
		}
		if strings.HasPrefix(ctype, "application/json") && !bytes.Contains(body, []byte(`"max_connections":40`)) {
			t.Fatalf("other fields lost: %s", body)
		}
	}
	big := newReq("application/json", `{"url":"`+strings.Repeat("a", maxParamsBody)+`"}`, "")
	if _, err := readParams(big); err != errParamsTooLarge {
		t.Fatalf("large body: %v", err)
	}
}

func TestCheckWebhookURL(t *testing.T) {
	for raw, ok := range map[string]bool{
		"https://bot.example/hook":      true,
		"https://bot.example:8443/hook": true,
		"http://bot.example/hook":       false,
		"https://bot.example:6379/":     false,
		"/relative":                     false,
	} {
		if err := checkWebhookURL(raw, false); (err == nil) != ok {
			t.Errorf("%s: %v", raw, err)
		}
	}
	if checkWebhookURL("http://127.0.0.1:9999/hook", true) != nil {
		t.Fatal("unsafe mode")
	}
}

func TestWebhookClientRefusesPrivateAddresses(t *testing.T) {
	bot := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer bot.Close()
	if _, err := webhookClient(false).Post(bot.URL, "application/json", strings.NewReader("{}")); err == nil ||
		!strings.Contains(err.Error(), errPrivateAddress.Error()) {
		t.Fatalf("loopback reached: %v", err)
	}
	resp, err := webhookClient(true).Post(bot.URL, "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestKeyPrefix(t *testing.T) {
	if got := keyPrefix("bs_live_ab12cd34_secretsecret"); got != "bs_live_ab12cd34" {
		t.Fatal(got)
	}
	if got := keyPrefix("garbage"); got != "invalid" {
		t.Fatal(got)
	}
}
