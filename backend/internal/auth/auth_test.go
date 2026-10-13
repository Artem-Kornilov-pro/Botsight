package auth

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestPasswordHashAndVerify(t *testing.T) {
	hash, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(hash, "$argon2id$v=19$m=19456,t=2,p=1$") {
		t.Fatalf("unexpected hash format: %s", hash)
	}
	if ok, err := VerifyPassword("correct horse battery staple", hash); err != nil || !ok {
		t.Fatalf("valid password rejected: ok=%v err=%v", ok, err)
	}
	if ok, _ := VerifyPassword("wrong", hash); ok {
		t.Fatal("wrong password accepted")
	}
	other, _ := HashPassword("correct horse battery staple")
	if other == hash {
		t.Fatal("salt is not random")
	}
}

func TestVerifyPasswordMalformed(t *testing.T) {
	for _, h := range []string{"", "plain", "$argon2i$v=19$m=1,t=1,p=1$AAAA$AAAA", "$argon2id$v=19$m=x$AAAA$AAAA", "$argon2id$v=19$m=1,t=1,p=1$!!$AAAA"} {
		if _, err := VerifyPassword("x", h); err == nil {
			t.Errorf("malformed hash %q accepted", h)
		}
	}
}

func TestAPIKey(t *testing.T) {
	key, err := NewAPIKey()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(key.Plain, key.Prefix+"_") || len(key.Prefix) != len("bs_live_ab12cd34") {
		t.Fatalf("bad key %q / prefix %q", key.Plain, key.Prefix)
	}
	prefix, err := ParseAPIKeyPrefix(key.Plain)
	if err != nil || prefix != key.Prefix {
		t.Fatalf("parse: %q %v", prefix, err)
	}
	if !VerifyAPIKey(key.Plain, key.Hash) {
		t.Fatal("key does not verify against its hash")
	}
	if VerifyAPIKey(key.Plain+"x", key.Hash) {
		t.Fatal("modified key verified")
	}
	other, _ := NewAPIKey()
	if other.Plain == key.Plain || other.Prefix == key.Prefix {
		t.Fatal("keys are not random")
	}
}

func TestParseAPIKeyPrefixRejects(t *testing.T) {
	valid, _ := NewAPIKey()
	for _, k := range []string{
		"",
		"bs_live_ab12cd34",
		"xx_live_ab12cd34_" + strings.Repeat("a", 40),
		"bs_live_AB12CD34_" + strings.Repeat("a", 40),
		"bs_live_ab12cd3_" + strings.Repeat("a", 40),
		"bs_live_ab12cd34_" + strings.Repeat("a", 39),
		"bs_live_ab12cd34_" + strings.Repeat("-", 40),
		valid.Plain + "_extra",
	} {
		if _, err := ParseAPIKeyPrefix(k); err == nil {
			t.Errorf("malformed key %q accepted", k)
		}
	}
}

const botToken = "123456:TEST-token"

func signedTelegram(t *testing.T, authDate time.Time) map[string]string {
	t.Helper()
	fields := map[string]string{
		"id":         "777",
		"first_name": "Иван",
		"last_name":  "Петров",
		"username":   "ivan",
		"photo_url":  "https://t.me/i/userpic/320/ivan.jpg",
		"auth_date":  strconv.FormatInt(authDate.Unix(), 10),
	}
	fields["hash"] = SignTelegramLogin(fields, botToken)
	return fields
}

func TestVerifyTelegramLogin(t *testing.T) {
	now := time.Now()
	user, err := VerifyTelegramLogin(signedTelegram(t, now), botToken, 24*time.Hour, now)
	if err != nil {
		t.Fatal(err)
	}
	if user.ID != 777 || user.Username != "ivan" || user.Name() != "Иван Петров" {
		t.Fatalf("unexpected user %+v", user)
	}
}

func TestVerifyTelegramLoginRejects(t *testing.T) {
	now := time.Now()
	cases := map[string]func(map[string]string) (map[string]string, string, time.Time){
		"tampered field": func(f map[string]string) (map[string]string, string, time.Time) {
			f["id"] = "778"
			return f, botToken, now
		},
		"extra field": func(f map[string]string) (map[string]string, string, time.Time) {
			f["is_admin"] = "true"
			return f, botToken, now
		},
		"other bot": func(f map[string]string) (map[string]string, string, time.Time) {
			return f, "999:OTHER", now
		},
		"bad hash": func(f map[string]string) (map[string]string, string, time.Time) {
			f["hash"] = "zz"
			return f, botToken, now
		},
		"expired": func(f map[string]string) (map[string]string, string, time.Time) {
			return f, botToken, now.Add(25 * time.Hour)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			fields, token, at := mutate(signedTelegram(t, now))
			if _, err := VerifyTelegramLogin(fields, token, 24*time.Hour, at); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestNewToken(t *testing.T) {
	a, hash, err := NewToken()
	if err != nil || len(a) != 43 || string(hash) != string(HashToken(a)) {
		t.Fatalf("token %q err %v", a, err)
	}
	b, _, _ := NewToken()
	if a == b {
		t.Fatal("tokens are not random")
	}
}
