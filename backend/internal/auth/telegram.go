package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TelegramUser is the verified result of a Telegram Login Widget sign-in.
type TelegramUser struct {
	ID        int64
	FirstName string
	LastName  string
	Username  string
	AuthDate  time.Time
}

// Name is the display name built from the Telegram profile.
func (u TelegramUser) Name() string {
	return strings.TrimSpace(u.FirstName + " " + u.LastName)
}

var (
	ErrTelegramSignature = errors.New("auth: invalid Telegram signature")
	ErrTelegramExpired   = errors.New("auth: Telegram auth data is too old")
	ErrTelegramData      = errors.New("auth: malformed Telegram auth data")
)

// VerifyTelegramLogin checks data sent by the Telegram Login Widget
// (https://core.telegram.org/widgets/login#checking-authorization):
// data_check_string is every received field except hash, sorted by key as
// "key=value" lines; the hash is HMAC-SHA256 of it with SHA-256(bot token) as key.
func VerifyTelegramLogin(fields map[string]string, botToken string, maxAge time.Duration, now time.Time) (TelegramUser, error) {
	got, err := hex.DecodeString(fields["hash"])
	if err != nil || len(got) != sha256.Size || !hmac.Equal(telegramHash(fields, botToken), got) {
		return TelegramUser{}, ErrTelegramSignature
	}

	id, err := strconv.ParseInt(fields["id"], 10, 64)
	if err != nil || id <= 0 {
		return TelegramUser{}, ErrTelegramData
	}
	authUnix, err := strconv.ParseInt(fields["auth_date"], 10, 64)
	if err != nil {
		return TelegramUser{}, ErrTelegramData
	}
	authDate := time.Unix(authUnix, 0)
	if now.Sub(authDate) > maxAge || authDate.Sub(now) > time.Minute {
		return TelegramUser{}, ErrTelegramExpired
	}
	return TelegramUser{
		ID:        id,
		FirstName: fields["first_name"],
		LastName:  fields["last_name"],
		Username:  fields["username"],
		AuthDate:  authDate,
	}, nil
}

// SignTelegramLogin produces the hash the widget would send; used in tests and local tools.
func SignTelegramLogin(fields map[string]string, botToken string) string {
	return hex.EncodeToString(telegramHash(fields, botToken))
}

func telegramHash(fields map[string]string, botToken string) []byte {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		if k != "hash" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	lines := make([]string, len(keys))
	for i, k := range keys {
		lines[i] = k + "=" + fields[k]
	}
	secret := sha256.Sum256([]byte(botToken))
	mac := hmac.New(sha256.New, secret[:])
	mac.Write([]byte(strings.Join(lines, "\n")))
	return mac.Sum(nil)
}
