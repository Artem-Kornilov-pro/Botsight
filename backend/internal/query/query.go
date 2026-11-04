// Package query builds the analytics queries of the dashboard (Botsight.md, 13) from typed
// definitions and runs them in ClickHouse.
//
// Values never go into the SQL text: every one of them is a server-side query parameter
// {name:Type}, so a funnel step, a source label or a time zone cannot change the query.
// project_id comes from the caller after the membership check.
package query

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
)

// MaxRange limits the period of one query: events live 13 months (Botsight.md, 12.1).
const MaxRange = 400 * 24 * time.Hour

// ErrTooHeavy: the query hit a ClickHouse limit (time, rows, memory); a narrower range helps.
var ErrTooHeavy = errors.New("query exceeded the analytics limits")

// Range is a half-open period [From, To) and the time zone that splits it into days.
type Range struct {
	From, To time.Time
	TZ       string // IANA name, e.g. Europe/Moscow
}

// Runner executes queries with per-query limits and caches heavy results in Redis.
type Runner struct {
	Conn driver.Conn
	// Cache holds results for TTL under qcache:{hash} (Botsight.md, 12.5); nil disables it.
	Cache redis.UniversalClient
	TTL   time.Duration
	// MaxExecutionTime is max_execution_time of every query; 0 means 20 s.
	MaxExecutionTime time.Duration
	// Metrics records query times and cache hits; nil records nothing.
	Metrics *Metrics
}

func (r *Runner) ctx(ctx context.Context) context.Context {
	limit := r.MaxExecutionTime
	if limit <= 0 {
		limit = 20 * time.Second
	}
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_execution_time": int(limit.Seconds()),
		// the dashboard reads aggregates: a runaway query must not take the cluster's memory
		"max_memory_usage": int64(4 << 30),
	}))
}

// query runs q and maps ClickHouse limit errors to ErrTooHeavy.
func (r *Runner) query(ctx context.Context, q Query) (driver.Rows, error) {
	rows, err := r.Conn.Query(r.ctx(ctx), q.SQL, q.Args()...)
	return rows, mapErr(err)
}

func mapErr(err error) error {
	var ex *clickhouse.Exception
	if errors.As(err, &ex) {
		switch ex.Code {
		case 158, 159, 160, 241: // TOO_MANY_ROWS, TIMEOUT_EXCEEDED, TOO_SLOW, MEMORY_LIMIT_EXCEEDED
			return fmt.Errorf("%w: %s", ErrTooHeavy, ex.Message)
		}
	}
	return err
}

// cached returns the result of kind for key from Redis, reporting a hit, or computes and
// stores it. Cache failures are not errors: the query just runs.
func cached[T any](ctx context.Context, r *Runner, kind string, key any, compute func(context.Context) (T, error)) (T, bool, error) {
	var zero T
	compute = timed(r, kind, compute)
	if r.Cache == nil || r.TTL <= 0 {
		out, err := compute(ctx)
		return out, false, err
	}
	raw, err := json.Marshal(struct {
		Kind string
		Key  any
	}{kind, key})
	if err != nil {
		return zero, false, err
	}
	sum := sha256.Sum256(raw)
	redisKey := "qcache:" + hex.EncodeToString(sum[:])
	if b, err := r.Cache.Get(ctx, redisKey).Bytes(); err == nil {
		var out T
		if json.Unmarshal(b, &out) == nil {
			r.Metrics.cache(kind, true)
			return out, true, nil
		}
	}
	r.Metrics.cache(kind, false)
	out, err := compute(ctx)
	if err != nil {
		return zero, false, err
	}
	if b, err := json.Marshal(out); err == nil {
		_ = r.Cache.Set(ctx, redisKey, b, r.TTL).Err()
	}
	return out, false, nil
}

// timed records how long the queries of a kind take in ClickHouse.
func timed[T any](r *Runner, kind string, compute func(context.Context) (T, error)) func(context.Context) (T, error) {
	return func(ctx context.Context) (T, error) {
		defer r.Metrics.took(kind, time.Now())
		return compute(ctx)
	}
}

// Query is SQL with {name:Type} placeholders and their values.
type Query struct {
	Params Params
	SQL    string
}

// Args turns the parameters into driver arguments in a stable order.
func (q Query) Args() []any {
	names := make([]string, 0, len(q.Params))
	for n := range q.Params {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]any, 0, len(names))
	for _, n := range names {
		out = append(out, clickhouse.Named(n, q.Params[n]))
	}
	return out
}

// Params collects parameter values while SQL is assembled.
type Params map[string]any

// P stores the value and returns its placeholder. The native protocol sends parameters
// next to settings, so a name must not be a setting of ClickHouse: "limit" is one.
func (p Params) P(name, typ string, value any) string {
	p[name] = value
	return "{" + name + ":" + typ + "}"
}

const dt64 = "DateTime64(3, 'UTC')"

// scope is the common filter: the project and the period.
func scope(p Params, projectID uint32, rg Range) string {
	return "project_id = " + p.P("pid", "UInt32", projectID) +
		" AND ts >= " + p.P("from", dt64, rg.From) +
		" AND ts < " + p.P("to", dt64, rg.To)
}

// --- steps: the building blocks of funnels and conversion goals ---

// Step is one action of a user: a command, a button, entering an FSM state or a manual event.
type Step struct {
	Kind  string `json:"kind"` // command, callback_data, state, custom
	Value string `json:"value"`
}

var StepKinds = []string{"command", "callback_data", "state", "custom"}

// ParseStep reads "kind:value", e.g. custom:purchase or state:Form:confirm.
func ParseStep(s string) (Step, error) {
	kind, value, ok := strings.Cut(s, ":")
	st := Step{Kind: kind, Value: strings.TrimSpace(value)}
	if kind == "command" {
		st.Value = strings.TrimPrefix(st.Value, "/")
	}
	if !ok || st.Value == "" || !validKind(kind) {
		return Step{}, fmt.Errorf("step is kind:value with kind %s", strings.Join(StepKinds, ", "))
	}
	return st, nil
}

func validKind(k string) bool {
	for _, v := range StepKinds {
		if v == k {
			return true
		}
	}
	return false
}

// cond is the SQL condition of the step; the value goes into a parameter named name.
func (s Step) cond(p Params, name string) (string, error) {
	v := p.P(name, "String", s.Value)
	switch s.Kind {
	case "command":
		return "(type = 'update' AND command = " + v + ")", nil
	case "callback_data":
		return "(type = 'update' AND name = 'callback_query' AND callback_data = " + v + ")", nil
	case "state":
		return "(type = 'step' AND error = '' AND state_to = " + v + ")", nil
	case "custom":
		return "(type = 'custom' AND name = " + v + ")", nil
	}
	return "", fmt.Errorf("unknown step kind %q", s.Kind)
}

// blockedUser is the user who blocked the bot, or 0 (Botsight.md, 8.6): a 403 bot_blocked
// on a message to a private chat (its id is the user id) or my_chat_member -> kicked.
const blockedUser = `multiIf(
    type = 'outgoing' AND error = 'bot_blocked' AND chat_id > 0, toUInt64(chat_id),
    type = 'update' AND name = 'my_chat_member' AND member_status = 'kicked' AND chat_type = 'private', user_id,
    0)`

func scanAll[T any](rows driver.Rows, scan func(*T) []any) ([]T, error) {
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		var v T
		if err := rows.Scan(scan(&v)...); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, mapErr(rows.Err())
}
