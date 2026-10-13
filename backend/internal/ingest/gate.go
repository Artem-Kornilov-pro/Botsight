package ingest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Gate admits batches: deduplication (Botsight.md, 10.4) and the per-project token
// bucket (10.2) in one Lua script, so a batch costs one Redis round trip. Keys share
// the {project_id} hash tag and stay in one slot under Redis Cluster.
type Gate struct {
	rdb           redis.UniversalClient
	rate          float64 // events per second per project
	burst         float64
	processingTTL time.Duration
	doneTTL       time.Duration
}

func NewGate(rdb redis.UniversalClient, ratePerSecond, burst float64) *Gate {
	return &Gate{rdb: rdb, rate: ratePerSecond, burst: burst, processingTTL: 120 * time.Second, doneTTL: 24 * time.Hour}
}

type Admission int

const (
	Admitted   Admission = iota
	Duplicate            // accepted before; Accepted holds the event count
	InProgress           // the same batch is being processed by another request
	Limited              // over the project's rate; see RetryAfter
)

type GateResult struct {
	Admission  Admission
	Accepted   int
	RetryAfter time.Duration
}

func batchKey(projectID int32, batchID string) string {
	return fmt.Sprintf("batch:{%d}:%s", projectID, strings.ToLower(batchID))
}

func bucketKey(projectID int32) string {
	return fmt.Sprintf("rl:{%d}", projectID)
}

// KEYS[1] batch key, KEYS[2] bucket key.
// ARGV: rate per second, burst, cost, processing TTL in seconds.
// Returns {0} admitted, {1, state} batch seen before, {2, wait_ms} rate limited.
var admitScript = redis.NewScript(`
local state = redis.call('GET', KEYS[1])
if state then
  return {1, state}
end
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost = tonumber(ARGV[3])
local b = redis.call('HMGET', KEYS[2], 'tokens', 'ts')
local tokens = tonumber(b[1])
local ts = tonumber(b[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts = now
end
tokens = math.min(burst, tokens + math.max(0, now - ts) * rate / 1000)
local ttl = math.ceil(burst * 1000 / rate) + 1000
if tokens < cost then
  redis.call('HSET', KEYS[2], 'tokens', tostring(tokens), 'ts', tostring(now))
  redis.call('PEXPIRE', KEYS[2], ttl)
  return {2, tostring(math.ceil((cost - tokens) * 1000 / rate))}
end
redis.call('HSET', KEYS[2], 'tokens', tostring(tokens - cost), 'ts', tostring(now))
redis.call('PEXPIRE', KEYS[2], ttl)
redis.call('SET', KEYS[1], 'processing', 'EX', ARGV[4])
return {0}
`)

// Admit checks the batch and takes cost tokens from the project's bucket.
func (g *Gate) Admit(ctx context.Context, projectID int32, batchID string, cost int) (GateResult, error) {
	res, err := admitScript.Run(ctx, g.rdb,
		[]string{batchKey(projectID, batchID), bucketKey(projectID)},
		g.rate, g.burst, cost, int(g.processingTTL.Seconds())).Slice()
	if err != nil {
		return GateResult{}, err
	}
	code, _ := res[0].(int64)
	switch code {
	case 0:
		return GateResult{Admission: Admitted}, nil
	case 1:
		state, _ := res[1].(string)
		if n, ok := strings.CutPrefix(state, "done:"); ok {
			accepted, _ := strconv.Atoi(n)
			return GateResult{Admission: Duplicate, Accepted: accepted}, nil
		}
		return GateResult{Admission: InProgress}, nil
	case 2:
		ms, _ := strconv.ParseInt(fmt.Sprint(res[1]), 10, 64)
		return GateResult{Admission: Limited, RetryAfter: time.Duration(ms) * time.Millisecond}, nil
	}
	return GateResult{}, fmt.Errorf("ingest: unexpected gate result %v", res)
}

// Done marks the batch accepted for 24 hours: retries get 202 "duplicate".
func (g *Gate) Done(ctx context.Context, projectID int32, batchID string, accepted int) error {
	return g.rdb.Set(ctx, batchKey(projectID, batchID), "done:"+strconv.Itoa(accepted), g.doneTTL).Err()
}

// Release forgets a batch that failed to reach Kafka, so the SDK retry is processed again.
func (g *Gate) Release(ctx context.Context, projectID int32, batchID string) error {
	return g.rdb.Del(ctx, batchKey(projectID, batchID)).Err()
}
