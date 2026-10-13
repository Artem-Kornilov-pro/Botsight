package ingest

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

var ErrInvalidKey = errors.New("ingest: invalid API key")

// KeyLoader reads keys from PostgreSQL on a cache miss.
type KeyLoader interface {
	IngestKeyByPrefix(ctx context.Context, prefix string) (store.IngestKey, error)
}

// Project is what a batch request is authorized for.
type Project struct {
	ID          int32
	Prefix      string
	CaptureText bool
	BotIDKnown  bool
}

// KeyCache keeps API keys in memory so the hot path never queries PostgreSQL
// (Botsight.md, 10.2). Entries live TTL; unknown prefixes are cached for a shorter
// time so random keys cannot hammer the database. Revocations arrive via Redis
// Pub/Sub and drop entries immediately.
type KeyCache struct {
	loader      KeyLoader
	ttl         time.Duration
	negativeTTL time.Duration
	maxEntries  int
	metrics     *Metrics

	mu      sync.RWMutex
	entries map[string]*cacheEntry
	loads   singleflight.Group
}

type cacheEntry struct {
	key        store.IngestKey
	found      bool
	expires    time.Time
	botIDKnown atomic.Bool
}

func NewKeyCache(loader KeyLoader, ttl, negativeTTL time.Duration, maxEntries int, m *Metrics) *KeyCache {
	return &KeyCache{
		loader: loader, ttl: ttl, negativeTTL: negativeTTL, maxEntries: maxEntries, metrics: m,
		entries: map[string]*cacheEntry{},
	}
}

// Lookup authorizes a full API key. ErrInvalidKey for malformed, unknown, revoked or
// wrong keys; other errors mean PostgreSQL is unavailable on a cache miss.
func (c *KeyCache) Lookup(ctx context.Context, key string) (Project, error) {
	prefix, err := auth.ParseAPIKeyPrefix(key)
	if err != nil {
		return Project{}, ErrInvalidKey
	}
	e, err := c.entry(ctx, prefix)
	if err != nil {
		return Project{}, err
	}
	if !e.found || e.key.Revoked || !auth.VerifyAPIKey(key, e.key.SecretHash) {
		return Project{}, ErrInvalidKey
	}
	return Project{
		ID: e.key.ProjectID, Prefix: prefix, CaptureText: e.key.CaptureText,
		BotIDKnown: e.key.BotIDKnown || e.botIDKnown.Load(),
	}, nil
}

func (c *KeyCache) entry(ctx context.Context, prefix string) (*cacheEntry, error) {
	c.mu.RLock()
	e := c.entries[prefix]
	c.mu.RUnlock()
	if e != nil && time.Now().Before(e.expires) {
		c.metrics.KeyCacheResults.WithLabelValues("hit").Inc()
		return e, nil
	}
	// One PostgreSQL query per prefix however many requests miss at once; detached
	// from the request so one cancelled client does not fail the others.
	v, err, _ := c.loads.Do(prefix, func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		k, err := c.loader.IngestKeyByPrefix(loadCtx, prefix)
		ne := &cacheEntry{key: k, found: err == nil, expires: time.Now().Add(c.ttl)}
		switch {
		case errors.Is(err, store.ErrNotFound):
			ne.expires = time.Now().Add(c.negativeTTL)
		case err != nil:
			return nil, err
		}
		c.put(prefix, ne)
		return ne, nil
	})
	if err != nil {
		c.metrics.KeyCacheResults.WithLabelValues("error").Inc()
		return nil, err
	}
	c.metrics.KeyCacheResults.WithLabelValues("miss").Inc()
	return v.(*cacheEntry), nil
}

func (c *KeyCache) put(prefix string, e *cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxEntries {
		now := time.Now()
		for p, old := range c.entries {
			if now.After(old.expires) {
				delete(c.entries, p)
			}
		}
		// still full: drop arbitrary entries; they are reloaded on demand
		for p := range c.entries {
			if len(c.entries) < c.maxEntries*9/10 {
				break
			}
			delete(c.entries, p)
		}
	}
	c.entries[prefix] = e
}

// Invalidate drops a key, e.g. after revocation.
func (c *KeyCache) Invalidate(prefix string) {
	c.mu.Lock()
	delete(c.entries, prefix)
	c.mu.Unlock()
}

// MarkBotIDKnown avoids repeating the bot_id update for every batch until the entry expires.
func (c *KeyCache) MarkBotIDKnown(prefix string) {
	c.mu.RLock()
	if e := c.entries[prefix]; e != nil {
		e.botIDKnown.Store(true)
	}
	c.mu.RUnlock()
}

// ListenRevocations drops revoked keys from the cache until ctx ends. If Redis is
// unreachable, revocations still take effect when entries expire (TTL).
func (c *KeyCache) ListenRevocations(ctx context.Context, rdb redis.UniversalClient, log *slog.Logger) {
	sub := rdb.Subscribe(ctx, auth.KeyRevocationChannel)
	defer sub.Close()
	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.Invalidate(msg.Payload)
			log.Info("API key revoked, dropped from cache", "prefix", msg.Payload)
		}
	}
}
