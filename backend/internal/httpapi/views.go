package httpapi

import (
	"encoding/json"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

type accountView struct {
	ID            int64         `json:"id"`
	Email         *string       `json:"email"`
	EmailVerified bool          `json:"email_verified"`
	HasPassword   bool          `json:"has_password"`
	Telegram      *telegramView `json:"telegram"`
	Name          string        `json:"name"`
	CreatedAt     time.Time     `json:"created_at"`
}

type telegramView struct {
	ID       int64   `json:"id"`
	Username *string `json:"username"`
}

func viewAccount(a store.Account) accountView {
	v := accountView{
		ID:            a.ID,
		Email:         a.Email,
		EmailVerified: a.EmailVerifiedAt != nil,
		HasPassword:   a.PasswordHash != nil,
		Name:          a.Name,
		CreatedAt:     a.CreatedAt,
	}
	if a.TelegramID != nil {
		v.Telegram = &telegramView{ID: *a.TelegramID, Username: a.TelegramUsername}
	}
	return v
}

// sessionView is returned on sign-in and by GET /v1/account: the dashboard sends
// csrf_token back in the X-CSRF-Token header.
type sessionView struct {
	Account   accountView `json:"account"`
	CSRFToken string      `json:"csrf_token"`
}

type projectView struct {
	ID        int32                 `json:"id"`
	Name      string                `json:"name"`
	BotID     *int64                `json:"bot_id"`
	Plan      string                `json:"plan"`
	Settings  store.ProjectSettings `json:"settings"`
	Role      string                `json:"role"`
	CreatedAt time.Time             `json:"created_at"`
}

func viewProject(p store.Project) projectView {
	return projectView{
		ID: p.ID, Name: p.Name, BotID: p.BotID, Plan: p.Plan,
		Settings: p.Settings, Role: p.Role, CreatedAt: p.CreatedAt,
	}
}

type memberView struct {
	AccountID        int64     `json:"account_id"`
	Email            *string   `json:"email"`
	TelegramUsername *string   `json:"telegram_username"`
	Name             string    `json:"name"`
	Role             string    `json:"role"`
	AddedAt          time.Time `json:"added_at"`
}

func viewMember(m store.Member) memberView {
	return memberView{
		AccountID: m.AccountID, Email: m.Email, TelegramUsername: m.TelegramUsername,
		Name: m.Name, Role: m.Role, AddedAt: m.AddedAt,
	}
}

type keyView struct {
	ID        int32      `json:"id"`
	Prefix    string     `json:"prefix"`
	Name      string     `json:"name"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at"`
	// Key is the full key, present only in the response that created it.
	Key string `json:"key,omitempty"`
}

func viewKey(k store.APIKey) keyView {
	return keyView{ID: k.ID, Prefix: k.Prefix, Name: k.Name, CreatedAt: k.CreatedAt, RevokedAt: k.RevokedAt}
}

type funnelView struct {
	ID         int32            `json:"id"`
	Name       string           `json:"name"`
	Definition FunnelDefinition `json:"definition"`
	CreatedAt  time.Time        `json:"created_at"`
	UpdatedAt  time.Time        `json:"updated_at"`
}

func viewFunnel(f store.Funnel) (funnelView, error) {
	v := funnelView{ID: f.ID, Name: f.Name, CreatedAt: f.CreatedAt, UpdatedAt: f.UpdatedAt}
	err := json.Unmarshal(f.Definition, &v.Definition)
	return v, err
}

type adSpendView struct {
	ID        int32       `json:"id"`
	Source    string      `json:"source"`
	From      string      `json:"from"`
	To        string      `json:"to"`
	AmountRub json.Number `json:"amount_rub"`
	CreatedAt time.Time   `json:"created_at"`
}

func viewAdSpend(a store.AdSpend) adSpendView {
	return adSpendView{
		ID: a.ID, Source: a.Source,
		From: a.From.Format(dateLayout), To: a.To.Format(dateLayout),
		AmountRub: formatRubles(a.AmountKopek), CreatedAt: a.CreatedAt,
	}
}
