package httpapi_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/httpapi"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
)

func TestRegisterLoginLogout(t *testing.T) {
	h := newHarness(t)
	c := h.client()
	email := uniqueEmail(t)

	r := c.register(strings.ToUpper(email), "password123")
	if r.str("account", "email") != email {
		t.Fatalf("email not normalized: %v", r.body)
	}
	if r.get("account", "email_verified") != false || r.get("account", "has_password") != true {
		t.Fatalf("unexpected account: %v", r.body)
	}
	cookie := r.header.Get("Set-Cookie")
	if !strings.Contains(cookie, "bs_session=") || !strings.Contains(cookie, "HttpOnly") || !strings.Contains(cookie, "SameSite=Lax") {
		t.Fatalf("bad session cookie: %s", cookie)
	}

	me := c.expect(http.StatusOK, "GET", "/v1/account", nil)
	if me.str("account", "email") != email || me.str("csrf_token") != c.csrf {
		t.Fatalf("GET /account: %v", me.body)
	}

	c.expect(http.StatusNoContent, "POST", "/v1/auth/logout", nil)
	c.expect(http.StatusUnauthorized, "GET", "/v1/account", nil)

	c.login(email, "password123")
	c.expect(http.StatusOK, "GET", "/v1/account", nil)
}

func TestRegisterValidation(t *testing.T) {
	h := newHarness(t)
	c := h.client()
	email := uniqueEmail(t)
	cases := []struct {
		body  map[string]string
		field string
	}{
		{map[string]string{"email": "not-an-email", "password": "password123"}, "email"},
		{map[string]string{"email": "a@localhost", "password": "password123"}, "email"},
		{map[string]string{"email": "Name <a@b.ru>", "password": "password123"}, "email"},
		{map[string]string{"email": email, "password": "short"}, "password"},
		{map[string]string{"email": email, "password": strings.Repeat("x", 129)}, "password"},
		{map[string]string{"email": email, "password": "password123", "name": strings.Repeat("я", 101)}, "name"},
	}
	for _, tc := range cases {
		r := c.expect(http.StatusBadRequest, "POST", "/v1/auth/register", tc.body)
		if r.str("error", "code") != "validation_failed" || r.str("error", "field") != tc.field {
			t.Errorf("%v: %v", tc.body, r.body)
		}
	}
	r := c.expect(http.StatusBadRequest, "POST", "/v1/auth/register", map[string]any{"email": email, "password": "password123", "admin": true})
	if r.str("error", "code") != "invalid_json" {
		t.Errorf("unknown field accepted: %v", r.body)
	}

	c.register(email, "password123")
	r = h.client().expect(http.StatusConflict, "POST", "/v1/auth/register", map[string]string{"email": email, "password": "password123"})
	if r.str("error", "code") != "email_taken" {
		t.Errorf("duplicate email: %v", r.body)
	}
}

func TestLoginFailuresLookTheSame(t *testing.T) {
	h := newHarness(t)
	_, email := h.newUser()
	c := h.client()
	wrong := c.expect(http.StatusUnauthorized, "POST", "/v1/auth/login", map[string]string{"email": email, "password": "wrong-password"})
	unknown := c.expect(http.StatusUnauthorized, "POST", "/v1/auth/login", map[string]string{"email": "nobody-" + email, "password": "wrong-password"})
	if wrong.str("error", "code") != "invalid_credentials" || unknown.str("error", "code") != "invalid_credentials" ||
		wrong.str("error", "message") != unknown.str("error", "message") {
		t.Fatalf("responses differ: %v vs %v", wrong.body, unknown.body)
	}
}

func TestLoginRateLimit(t *testing.T) {
	rule := ratelimit.Rule{Name: "t:login:" + strconv.FormatInt(time.Now().UnixNano(), 36), Limit: 3, Period: time.Minute}
	h := newHarness(t, func(o *httpapi.Options) { o.Limits.LoginAccount = rule })
	_, email := h.newUser()
	c := h.client()
	for i := 0; i < 3; i++ {
		c.expect(http.StatusUnauthorized, "POST", "/v1/auth/login", map[string]string{"email": email, "password": "wrong-password"})
	}
	r := c.expect(http.StatusTooManyRequests, "POST", "/v1/auth/login", map[string]string{"email": email, "password": "password123"})
	if r.str("error", "code") != "rate_limited" || r.header.Get("Retry-After") == "" {
		t.Fatalf("no rate limit: %v %v", r.body, r.header)
	}
}

func TestSessionAndCSRFRequired(t *testing.T) {
	h := newHarness(t)
	anon := h.client()
	r := anon.expect(http.StatusUnauthorized, "GET", "/v1/projects", nil)
	if r.str("error", "code") != "unauthorized" {
		t.Fatalf("%v", r.body)
	}

	c, _ := h.newUser()
	token := c.csrf
	c.csrf = ""
	if r := c.expect(http.StatusForbidden, "POST", "/v1/projects", map[string]string{"name": "x"}); r.str("error", "code") != "csrf_failed" {
		t.Fatalf("missing CSRF accepted: %v", r.body)
	}
	c.csrf = token + "x"
	c.expect(http.StatusForbidden, "POST", "/v1/projects", map[string]string{"name": "x"})
	c.csrf = ""
	c.expect(http.StatusOK, "GET", "/v1/projects", nil) // reads need no CSRF token
	c.csrf = token
	c.expect(http.StatusCreated, "POST", "/v1/projects", map[string]string{"name": "x"})
}

func TestRequireJSONContentType(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest("POST", h.srv.URL+"/v1/auth/login", strings.NewReader("email=a@b.ru&password=x"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnsupportedMediaType {
		t.Fatalf("form body accepted: %d", resp.StatusCode)
	}
}

func TestChangePasswordEndsOtherSessions(t *testing.T) {
	h := newHarness(t)
	first, email := h.newUser()
	second := h.client()
	second.login(email, "password123")

	r := first.expect(http.StatusBadRequest, "POST", "/v1/account/password",
		map[string]string{"current_password": "wrong-password", "new_password": "new-password-1"})
	if r.str("error", "code") != "wrong_password" {
		t.Fatalf("%v", r.body)
	}
	first.expect(http.StatusNoContent, "POST", "/v1/account/password",
		map[string]string{"current_password": "password123", "new_password": "new-password-1"})

	first.expect(http.StatusOK, "GET", "/v1/account", nil)
	second.expect(http.StatusUnauthorized, "GET", "/v1/account", nil)
	h.client().expect(http.StatusUnauthorized, "POST", "/v1/auth/login", map[string]string{"email": email, "password": "password123"})
	h.client().login(email, "new-password-1")
}

func TestLogoutOtherSessions(t *testing.T) {
	h := newHarness(t)
	first, email := h.newUser()
	second := h.client()
	second.login(email, "password123")
	first.expect(http.StatusNoContent, "DELETE", "/v1/account/sessions", nil)
	first.expect(http.StatusOK, "GET", "/v1/account", nil)
	second.expect(http.StatusUnauthorized, "GET", "/v1/account", nil)
}

func TestPasswordReset(t *testing.T) {
	h := newHarness(t)
	old, email := h.newUser()

	h.client().expect(http.StatusAccepted, "POST", "/v1/auth/password-reset", map[string]string{"email": "unknown-" + email})
	h.client().expect(http.StatusAccepted, "POST", "/v1/auth/password-reset", map[string]string{"email": email})
	token := h.lastMailToken(email)
	if len(h.mailer.to("unknown-"+email)) != 0 {
		t.Fatal("email sent to an unknown address")
	}

	anon := h.client()
	anon.expect(http.StatusBadRequest, "POST", "/v1/auth/password-reset/confirm", map[string]string{"token": token, "password": "short"})
	anon.expect(http.StatusNoContent, "POST", "/v1/auth/password-reset/confirm", map[string]string{"token": token, "password": "reset-password-1"})
	r := anon.expect(http.StatusBadRequest, "POST", "/v1/auth/password-reset/confirm", map[string]string{"token": token, "password": "reset-password-2"})
	if r.str("error", "code") != "invalid_token" {
		t.Fatalf("token reused: %v", r.body)
	}

	old.expect(http.StatusUnauthorized, "GET", "/v1/account", nil) // every session ends
	me := h.client()
	me.login(email, "reset-password-1")
	if me.expect(http.StatusOK, "GET", "/v1/account", nil).get("account", "email_verified") != true {
		t.Fatal("reset link did not confirm the email")
	}
}

func TestEmailVerification(t *testing.T) {
	h := newHarness(t)
	c, email := h.newUser()
	token := h.lastMailToken(email)

	h.client().expect(http.StatusBadRequest, "POST", "/v1/auth/verify-email", map[string]string{"token": "bogus"})
	h.client().expect(http.StatusNoContent, "POST", "/v1/auth/verify-email", map[string]string{"token": token})
	if c.expect(http.StatusOK, "GET", "/v1/account", nil).get("account", "email_verified") != true {
		t.Fatal("email not verified")
	}
	r := c.expect(http.StatusConflict, "POST", "/v1/account/verify-email", nil)
	if r.str("error", "code") != "already_verified" {
		t.Fatalf("%v", r.body)
	}
}

func TestResendVerification(t *testing.T) {
	h := newHarness(t)
	c, email := h.newUser()
	first := h.lastMailToken(email)
	c.expect(http.StatusAccepted, "POST", "/v1/account/verify-email", nil)
	second := h.lastMailToken(email)
	if first == second {
		t.Fatal("same token resent")
	}
	h.client().expect(http.StatusNoContent, "POST", "/v1/auth/verify-email", map[string]string{"token": second})
}

var telegramSeq = time.Now().UnixNano() % 1_000_000_000

func telegramPayload(id int64, username string, authDate time.Time) map[string]any {
	fields := map[string]string{
		"id":         strconv.FormatInt(id, 10),
		"first_name": "Иван",
		"username":   username,
		"auth_date":  strconv.FormatInt(authDate.Unix(), 10),
	}
	return map[string]any{
		"id":         id, // the widget sends numbers; the signature covers their text form
		"first_name": "Иван",
		"username":   username,
		"auth_date":  authDate.Unix(),
		"hash":       auth.SignTelegramLogin(fields, botToken),
	}
}

func TestTelegramLogin(t *testing.T) {
	h := newHarness(t)
	telegramSeq++
	tgID := 5_000_000_000 + telegramSeq

	c := h.client()
	r := c.signedIn(c.expect(http.StatusOK, "POST", "/v1/auth/telegram", telegramPayload(tgID, "ivan", time.Now())))
	if r.get("account", "email") != nil || r.num("account", "telegram", "id") != tgID ||
		r.str("account", "telegram", "username") != "ivan" || r.str("account", "name") != "Иван" {
		t.Fatalf("unexpected account: %v", r.body)
	}
	accountID := r.num("account", "id")

	again := h.client()
	r = again.signedIn(again.expect(http.StatusOK, "POST", "/v1/auth/telegram", telegramPayload(tgID, "ivan_new", time.Now())))
	if r.num("account", "id") != accountID || r.str("account", "telegram", "username") != "ivan_new" {
		t.Fatalf("second sign-in created another account: %v", r.body)
	}
	again.expect(http.StatusCreated, "POST", "/v1/projects", map[string]string{"name": "via telegram"})

	tampered := telegramPayload(tgID, "ivan", time.Now())
	tampered["id"] = tgID + 1
	if r := h.client().expect(http.StatusUnauthorized, "POST", "/v1/auth/telegram", tampered); r.str("error", "code") != "invalid_telegram_auth" {
		t.Fatalf("%v", r.body)
	}
	h.client().expect(http.StatusUnauthorized, "POST", "/v1/auth/telegram", telegramPayload(tgID, "ivan", time.Now().Add(-48*time.Hour)))

	// Telegram-only accounts have no password to change.
	r = again.expect(http.StatusConflict, "POST", "/v1/account/password", map[string]string{"current_password": "x", "new_password": "new-password-1"})
	if r.str("error", "code") != "no_password" {
		t.Fatalf("%v", r.body)
	}
}

func TestTelegramDisabled(t *testing.T) {
	h := newHarness(t, func(o *httpapi.Options) { o.TelegramToken = "" })
	r := h.client().expect(http.StatusNotFound, "POST", "/v1/auth/telegram", telegramPayload(1, "x", time.Now()))
	if r.str("error", "code") != "telegram_disabled" {
		t.Fatalf("%v", r.body)
	}
}

func TestLinkTelegram(t *testing.T) {
	h := newHarness(t)
	telegramSeq++
	tgID := 5_000_000_000 + telegramSeq

	owner, _ := h.newUser()
	r := owner.expect(http.StatusOK, "POST", "/v1/account/telegram", telegramPayload(tgID, "linked", time.Now()))
	if r.num("telegram", "id") != tgID || r.get("email") == nil {
		t.Fatalf("not linked: %v", r.body)
	}

	// Telegram sign-in now opens the same account.
	tg := h.client()
	if tg.expect(http.StatusOK, "POST", "/v1/auth/telegram", telegramPayload(tgID, "linked", time.Now())).num("account", "id") != r.num("id") {
		t.Fatal("Telegram sign-in opened another account")
	}

	other, _ := h.newUser()
	if r := other.expect(http.StatusConflict, "POST", "/v1/account/telegram", telegramPayload(tgID, "linked", time.Now())); r.str("error", "code") != "telegram_taken" {
		t.Fatalf("%v", r.body)
	}
}

func TestUpdateAccountName(t *testing.T) {
	h := newHarness(t)
	c, _ := h.newUser()
	r := c.expect(http.StatusOK, "PATCH", "/v1/account", map[string]string{"name": "  Анна  "})
	if r.str("name") != "Анна" {
		t.Fatalf("%v", r.body)
	}
	c.expect(http.StatusBadRequest, "PATCH", "/v1/account", map[string]string{"name": "a\x00b"})
}

func TestHealthAndReadiness(t *testing.T) {
	h := newHarness(t)
	c := h.client()
	c.expect(http.StatusOK, "GET", "/healthz", nil)
	c.expect(http.StatusOK, "GET", "/readyz", nil)
	h.api.SetReady(false)
	c.expect(http.StatusServiceUnavailable, "GET", "/readyz", nil)
	c.expect(http.StatusOK, "GET", "/healthz", nil)
}

func TestCORSPreflight(t *testing.T) {
	h := newHarness(t)
	preflight := func(origin string) *http.Response {
		req, _ := http.NewRequest("OPTIONS", h.srv.URL+"/v1/projects", nil)
		req.Header.Set("Origin", origin)
		req.Header.Set("Access-Control-Request-Method", "POST")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp
	}
	ok := preflight("https://app.botsight.test")
	if ok.Header.Get("Access-Control-Allow-Origin") != "https://app.botsight.test" ||
		ok.Header.Get("Access-Control-Allow-Credentials") != "true" ||
		!strings.Contains(ok.Header.Get("Access-Control-Allow-Headers"), "X-CSRF-Token") {
		t.Fatalf("preflight from dashboard: %v", ok.Header)
	}
	if bad := preflight("https://evil.test"); bad.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("foreign origin allowed: %v", bad.Header)
	}
}
