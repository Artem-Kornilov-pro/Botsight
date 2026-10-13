// Package ratelimit limits request rates with counters in Redis.
package ratelimit

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Window is a fixed-window counter: at most Limit hits per Period for a key.
// Used for sign-in, registration and password reset (Botsight.md, 19.5).
type Window struct {
	rdb redis.UniversalClient
}

func NewWindow(rdb redis.UniversalClient) *Window {
	return &Window{rdb: rdb}
}

// Rule is a limit for one kind of action.
type Rule struct {
	Name   string
	Limit  int64
	Period time.Duration
}

// Allow counts a hit for rule and key. When the limit is exceeded it returns
// false and how long to wait. On Redis errors it allows the request: a broken
// limiter must not lock everyone out of the dashboard.
func (w *Window) Allow(ctx context.Context, rule Rule, key string) (bool, time.Duration, error) {
	redisKey := "rl:" + rule.Name + ":" + key
	var count *redis.IntCmd
	var ttl *redis.DurationCmd
	_, err := w.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		count = p.Incr(ctx, redisKey)
		p.ExpireNX(ctx, redisKey, rule.Period)
		ttl = p.PTTL(ctx, redisKey)
		return nil
	})
	if err != nil {
		return true, 0, err
	}
	if count.Val() > rule.Limit {
		wait := ttl.Val()
		if wait <= 0 {
			wait = rule.Period
		}
		return false, wait, nil
	}
	return true, 0, nil
}
