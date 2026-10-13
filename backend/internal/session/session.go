// Package session keeps dashboard sessions in Redis (Botsight.md, 12.5).
//
// The cookie holds a random token; Redis stores the session under the token's
// SHA-256, so a leaked Redis dump does not contain usable session tokens.
package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
)

var ErrNotFound = errors.New("session: not found")

type Session struct {
	AccountID int64
	CSRFToken string
	CreatedAt time.Time
	key       string // Redis key, session:<sha256(token)>
}

type Store struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

func NewStore(rdb redis.UniversalClient, ttl time.Duration) *Store {
	return &Store{rdb: rdb, ttl: ttl}
}

func sessionKey(token string) string {
	return "session:" + hex.EncodeToString(auth.HashToken(token))
}

func accountKey(accountID int64) string {
	return "account_sessions:" + strconv.FormatInt(accountID, 10)
}

// Create starts a session and returns the token for the cookie.
func (s *Store) Create(ctx context.Context, accountID int64) (string, Session, error) {
	token, _, err := auth.NewToken()
	if err != nil {
		return "", Session{}, err
	}
	csrf := make([]byte, 24)
	if _, err := rand.Read(csrf); err != nil {
		return "", Session{}, err
	}
	sess := Session{
		AccountID: accountID,
		CSRFToken: base64.RawURLEncoding.EncodeToString(csrf),
		CreatedAt: time.Now().UTC().Truncate(time.Second),
		key:       sessionKey(token),
	}
	_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, sess.key,
			"account_id", accountID,
			"csrf", sess.CSRFToken,
			"created_at", sess.CreatedAt.Unix())
		p.Expire(ctx, sess.key, s.ttl)
		p.SAdd(ctx, accountKey(accountID), sess.key)
		p.Expire(ctx, accountKey(accountID), s.ttl)
		return nil
	})
	if err != nil {
		return "", Session{}, err
	}
	return token, sess, nil
}

// Get loads a session and extends it: the session expires after ttl without activity.
func (s *Store) Get(ctx context.Context, token string) (Session, error) {
	key := sessionKey(token)
	var fields *redis.MapStringStringCmd
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		fields = p.HGetAll(ctx, key)
		p.Expire(ctx, key, s.ttl)
		return nil
	})
	if err != nil {
		return Session{}, err
	}
	m := fields.Val()
	accountID, err := strconv.ParseInt(m["account_id"], 10, 64)
	if err != nil || m["csrf"] == "" {
		return Session{}, ErrNotFound
	}
	created, _ := strconv.ParseInt(m["created_at"], 10, 64)
	s.rdb.Expire(ctx, accountKey(accountID), s.ttl)
	return Session{
		AccountID: accountID,
		CSRFToken: m["csrf"],
		CreatedAt: time.Unix(created, 0).UTC(),
		key:       key,
	}, nil
}

// Delete ends one session.
func (s *Store) Delete(ctx context.Context, sess Session) error {
	_, err := s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, sess.key)
		p.SRem(ctx, accountKey(sess.AccountID), sess.key)
		return nil
	})
	return err
}

// DeleteAll ends every session of the account except keep (zero Session ends all).
// Used after a password change or reset.
func (s *Store) DeleteAll(ctx context.Context, accountID int64, keep Session) error {
	keys, err := s.rdb.SMembers(ctx, accountKey(accountID)).Result()
	if err != nil {
		return err
	}
	var drop []string
	for _, k := range keys {
		if k != keep.key {
			drop = append(drop, k)
		}
	}
	if len(drop) == 0 {
		return nil
	}
	_, err = s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, drop...)
		members := make([]any, len(drop))
		for i, k := range drop {
			members[i] = k
		}
		p.SRem(ctx, accountKey(accountID), members...)
		return nil
	})
	return err
}
