package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Account struct {
	ID               int64
	Email            *string
	EmailVerifiedAt  *time.Time
	PasswordHash     *string
	TelegramID       *int64
	TelegramUsername *string
	Name             string
	CreatedAt        time.Time
}

const accountColumns = `id, email, email_verified_at, password_hash, telegram_id, telegram_username, name, created_at`

func scanAccount(row interface{ Scan(...any) error }) (Account, error) {
	var a Account
	err := row.Scan(&a.ID, &a.Email, &a.EmailVerifiedAt, &a.PasswordHash, &a.TelegramID,
		&a.TelegramUsername, &a.Name, &a.CreatedAt)
	return a, mapErr(err)
}

// CreateAccount registers an account with email and password. ErrConflict if the email is taken.
func (s *Store) CreateAccount(ctx context.Context, email, passwordHash, name string) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`INSERT INTO accounts (email, password_hash, name) VALUES ($1, $2, $3)
		 RETURNING `+accountColumns, email, passwordHash, name))
}

func (s *Store) AccountByID(ctx context.Context, id int64) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE id = $1`, id))
}

func (s *Store) AccountByEmail(ctx context.Context, email string) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`SELECT `+accountColumns+` FROM accounts WHERE email = $1`, email))
}

// UpsertTelegramAccount finds the account bound to a Telegram user or creates one,
// refreshing the username on every sign-in.
func (s *Store) UpsertTelegramAccount(ctx context.Context, telegramID int64, username, name string) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`INSERT INTO accounts (telegram_id, telegram_username, name) VALUES ($1, NULLIF($2, ''), $3)
		 ON CONFLICT (telegram_id) DO UPDATE SET telegram_username = EXCLUDED.telegram_username
		 RETURNING `+accountColumns, telegramID, username, name))
}

// LinkTelegram binds a Telegram user to an account. ErrConflict if it is bound to another account.
func (s *Store) LinkTelegram(ctx context.Context, accountID, telegramID int64, username string) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`UPDATE accounts SET telegram_id = $2, telegram_username = NULLIF($3, '')
		 WHERE id = $1 RETURNING `+accountColumns, accountID, telegramID, username))
}

func (s *Store) UpdateAccountName(ctx context.Context, accountID int64, name string) (Account, error) {
	return scanAccount(s.pool.QueryRow(ctx,
		`UPDATE accounts SET name = $2 WHERE id = $1 RETURNING `+accountColumns, accountID, name))
}

func (s *Store) SetPassword(ctx context.Context, accountID int64, passwordHash string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE accounts SET password_hash = $2 WHERE id = $1 AND email IS NOT NULL`, accountID, passwordHash)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkEmailVerified confirms the email only if it has not changed since the link was sent.
func (s *Store) MarkEmailVerified(ctx context.Context, accountID int64, email string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE accounts SET email_verified_at = COALESCE(email_verified_at, now())
		 WHERE id = $1 AND email = $2`, accountID, email)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Token purposes for account_tokens.
const (
	TokenVerifyEmail   = "verify_email"
	TokenResetPassword = "reset_password"
)

func (s *Store) CreateToken(ctx context.Context, hash []byte, accountID int64, purpose, email string, ttl time.Duration) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO account_tokens (token_hash, account_id, purpose, email, expires_at)
		 VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5))`,
		hash, accountID, purpose, email, ttl.Seconds())
	return mapErr(err)
}

// ConsumeToken marks a valid, unused, unexpired token as used and invalidates the
// account's other tokens of the same purpose. ErrNotFound if the token is not usable.
func (s *Store) ConsumeToken(ctx context.Context, hash []byte, purpose string) (accountID int64, email string, err error) {
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`UPDATE account_tokens SET used_at = now()
			 WHERE token_hash = $1 AND purpose = $2 AND used_at IS NULL AND expires_at > now()
			 RETURNING account_id, email`, hash, purpose).Scan(&accountID, &email); err != nil {
			return mapErr(err)
		}
		_, err := tx.Exec(ctx,
			`UPDATE account_tokens SET used_at = now()
			 WHERE account_id = $1 AND purpose = $2 AND used_at IS NULL`, accountID, purpose)
		return err
	})
	return accountID, email, err
}
