package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/mail"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

const (
	verifyEmailTTL   = 48 * time.Hour
	resetPasswordTTL = time.Hour
	telegramMaxAge   = 24 * time.Hour
)

// startSession signs the account in: new session, cookie, account and CSRF token in the body.
func (s *Server) startSession(w http.ResponseWriter, r *http.Request, status int, a store.Account) {
	token, sess, err := s.Sessions.Create(r.Context(), a.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.setSessionCookie(w, token)
	writeJSON(w, status, sessionView{Account: viewAccount(a), CSRFToken: sess.CSRFToken})
}

type credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
	Name     string `json:"name"`
}

func (s *Server) register(w http.ResponseWriter, r *http.Request) {
	var req credentials
	if !decode(w, r, &req) {
		return
	}
	email, ferr := normalizeEmail(req.Email)
	if ferr == nil {
		ferr = validatePassword("password", req.Password)
	}
	name, nerr := cleanName("name", req.Name, false)
	if ferr == nil {
		ferr = nerr
	}
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	if !s.allow(w, r, s.Options.Limits.RegisterIP, clientIP(r)) {
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	account, err := s.Store.CreateAccount(r.Context(), email, hash, name)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, codeEmailTaken, "an account with this email already exists")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.sendVerification(account.ID, email)
	s.startSession(w, r, http.StatusCreated, account)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req credentials
	if !decode(w, r, &req) {
		return
	}
	if req.Name != "" {
		writeInvalid(w, invalid("name", "unexpected field"))
		return
	}
	email, ferr := normalizeEmail(req.Email)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	if !s.allow(w, r, s.Options.Limits.LoginIP, clientIP(r)) ||
		!s.allow(w, r, s.Options.Limits.LoginAccount, email) {
		return
	}
	account, err := s.Store.AccountByEmail(r.Context(), email)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.internalError(w, r, err)
		return
	}
	if err != nil || account.PasswordHash == nil {
		auth.BurnPasswordCheck(req.Password) // same timing for unknown emails
		writeError(w, http.StatusUnauthorized, codeInvalidCredentials, "wrong email or password")
		return
	}
	ok, err := auth.VerifyPassword(req.Password, *account.PasswordHash)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !ok {
		writeError(w, http.StatusUnauthorized, codeInvalidCredentials, "wrong email or password")
		return
	}
	s.startSession(w, r, http.StatusOK, account)
}

// readTelegramAuth decodes the Telegram Login Widget payload as received:
// every field takes part in the signature, so they are kept as strings.
func readTelegramAuth(w http.ResponseWriter, r *http.Request) (map[string]string, bool) {
	var raw map[string]json.RawMessage
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidJSON, "invalid JSON")
		return nil, false
	}
	fields := make(map[string]string, len(raw))
	for k, v := range raw {
		var str string
		var num json.Number
		switch {
		case json.Unmarshal(v, &str) == nil:
			fields[k] = str
		case json.Unmarshal(v, &num) == nil:
			fields[k] = num.String()
		default:
			writeInvalid(w, invalid(k, "Telegram auth fields are strings or numbers"))
			return nil, false
		}
	}
	return fields, true
}

func (s *Server) verifyTelegram(w http.ResponseWriter, r *http.Request) (auth.TelegramUser, bool) {
	if s.Options.TelegramToken == "" {
		writeError(w, http.StatusNotFound, codeTelegramDisabled, "Telegram sign-in is not configured")
		return auth.TelegramUser{}, false
	}
	fields, ok := readTelegramAuth(w, r)
	if !ok {
		return auth.TelegramUser{}, false
	}
	user, err := auth.VerifyTelegramLogin(fields, s.Options.TelegramToken, telegramMaxAge, time.Now())
	if err != nil {
		writeError(w, http.StatusUnauthorized, codeTelegramInvalid, "Telegram sign-in data is invalid or expired")
		return auth.TelegramUser{}, false
	}
	return user, true
}

func (s *Server) telegramLogin(w http.ResponseWriter, r *http.Request) {
	if !s.allow(w, r, s.Options.Limits.TelegramIP, clientIP(r)) {
		return
	}
	user, ok := s.verifyTelegram(w, r)
	if !ok {
		return
	}
	name, _ := cleanName("name", user.Name(), false)
	account, err := s.Store.UpsertTelegramAccount(r.Context(), user.ID, user.Username, name)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.startSession(w, r, http.StatusOK, account)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if err := s.Sessions.Delete(r.Context(), sessionFrom(r)); err != nil {
		s.internalError(w, r, err)
		return
	}
	s.clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) logoutOtherSessions(w http.ResponseWriter, r *http.Request) {
	sess := sessionFrom(r)
	if err := s.Sessions.DeleteAll(r.Context(), sess.AccountID, sess); err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- password reset and email confirmation ---

func (s *Server) requestPasswordReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
	}
	if !decode(w, r, &req) {
		return
	}
	email, ferr := normalizeEmail(req.Email)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	if !s.allow(w, r, s.Options.Limits.ResetIP, clientIP(r)) ||
		!s.allow(w, r, s.Options.Limits.ResetEmail, email) {
		return
	}
	// The answer and its timing do not reveal whether the email is registered.
	s.background("password-reset-email", func(ctx context.Context) error {
		account, err := s.Store.AccountByEmail(ctx, email)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		link, err := s.issueToken(ctx, account.ID, store.TokenResetPassword, email, resetPasswordTTL, "/reset-password")
		if err != nil {
			return err
		}
		return s.Mailer.Send(ctx, mail.Message{
			To:      email,
			Subject: "Сброс пароля в Botsight",
			Body: "Здравствуйте!\n\nЧтобы задать новый пароль, откройте ссылку:\n" + link +
				"\n\nСсылка действует 1 час. Если вы не запрашивали сброс, просто проигнорируйте письмо: пароль не изменится.\n",
		})
	})
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) confirmPasswordReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token    string `json:"token"`
		Password string `json:"password"`
	}
	if !decode(w, r, &req) {
		return
	}
	if ferr := validatePassword("password", req.Password); ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	accountID, email, err := s.Store.ConsumeToken(r.Context(), auth.HashToken(req.Token), store.TokenResetPassword)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusBadRequest, codeInvalidToken, "the link is invalid, used or expired")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if err := s.Store.SetPassword(r.Context(), accountID, hash); err != nil {
		s.internalError(w, r, err)
		return
	}
	// The link came to this inbox, so the address is confirmed too.
	if err := s.Store.MarkEmailVerified(r.Context(), accountID, email); err != nil && !errors.Is(err, store.ErrNotFound) {
		s.internalError(w, r, err)
		return
	}
	if err := s.Sessions.DeleteAll(r.Context(), accountID, sessionNone); err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) verifyEmail(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if !decode(w, r, &req) {
		return
	}
	accountID, email, err := s.Store.ConsumeToken(r.Context(), auth.HashToken(req.Token), store.TokenVerifyEmail)
	if err == nil {
		err = s.Store.MarkEmailVerified(r.Context(), accountID, email)
	}
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusBadRequest, codeInvalidToken, "the link is invalid, used or expired")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) sendVerification(accountID int64, email string) {
	s.background("verify-email", func(ctx context.Context) error {
		link, err := s.issueToken(ctx, accountID, store.TokenVerifyEmail, email, verifyEmailTTL, "/verify-email")
		if err != nil {
			return err
		}
		return s.Mailer.Send(ctx, mail.Message{
			To:      email,
			Subject: "Подтвердите почту в Botsight",
			Body: "Здравствуйте!\n\nЧтобы подтвердить почту, откройте ссылку:\n" + link +
				"\n\nСсылка действует 48 часов. Если вы не регистрировались в Botsight, просто проигнорируйте письмо.\n",
		})
	})
}

// issueToken stores a one-time token and returns the dashboard link with it.
func (s *Server) issueToken(ctx context.Context, accountID int64, purpose, email string, ttl time.Duration, path string) (string, error) {
	token, hash, err := auth.NewToken()
	if err != nil {
		return "", err
	}
	if err := s.Store.CreateToken(ctx, hash, accountID, purpose, email, ttl); err != nil {
		return "", fmt.Errorf("store token: %w", err)
	}
	return s.Options.DashboardURL + path + "?token=" + url.QueryEscape(token), nil
}
