package query

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// Errors is the errors screen (Botsight.md, 16.1): exceptions in handlers, Bot API errors
// (429 included), slow handlers and updates no handler took.
type Errors struct {
	Handlers    []HandlerError `json:"handlers"`
	API         []APIError     `json:"api"`
	RateLimited uint64         `json:"rate_limited"`
	Slow        []SlowHandler  `json:"slow"`
	Unhandled   []Unhandled    `json:"unhandled"`
}

type HandlerError struct {
	Handler  string    `json:"handler"`
	Error    string    `json:"error"`
	Count    uint64    `json:"count"`
	Users    uint64    `json:"users"`
	LastSeen time.Time `json:"last_seen"`
}

type APIError struct {
	Method   string    `json:"method"`
	Code     uint16    `json:"code"`
	Error    string    `json:"error"`
	Count    uint64    `json:"count"`
	Chats    uint64    `json:"chats"`
	LastSeen time.Time `json:"last_seen"`
}

type SlowHandler struct {
	Handler string    `json:"handler"`
	Calls   uint64    `json:"calls"`
	MS      Quantiles `json:"ms"`
	MaxMS   uint32    `json:"max_ms"`
}

type Unhandled struct {
	Name        string `json:"name"` // message, callback_query, ...
	ContentType string `json:"content_type"`
	Count       uint64 `json:"count"`
	Users       uint64 `json:"users"`
}

func errorQueries(projectID uint32, rg Range) (handlers, api, slow, unhandled Query) {
	p := Params{}
	where := scope(p, projectID, rg)
	handlers = Query{p, `
SELECT handler, error, count() AS n, uniqExact(user_id), max(ts)
FROM events
WHERE ` + where + ` AND type = 'step' AND error != ''
GROUP BY handler, error
ORDER BY n DESC, handler, error
LIMIT 100`}
	api = Query{p, `
SELECT name, error_code, error, count() AS n, uniqExact(chat_id), max(ts)
FROM events
WHERE ` + where + ` AND type = 'outgoing' AND ok = 0
GROUP BY name, error_code, error
ORDER BY n DESC, name, error
LIMIT 100`}
	// p95 over handlers with enough calls to mean something
	slow = Query{p, `
SELECT handler, count() AS calls, quantiles(0.5, 0.95, 0.99)(duration_ms) AS q, max(duration_ms)
FROM events
WHERE ` + where + ` AND type = 'step' AND handler != ''
GROUP BY handler
HAVING calls >= 5
ORDER BY q[2] DESC, handler
LIMIT 20`}
	unhandled = Query{p, `
SELECT name, content_type, count() AS n, uniqExact(user_id)
FROM events
WHERE ` + where + ` AND type = 'update' AND handled = 0
GROUP BY name, content_type
ORDER BY n DESC, name, content_type
LIMIT 50`}
	return
}

func (r *Runner) Errors(ctx context.Context, projectID uint32, rg Range) (Errors, bool, error) {
	return cached(ctx, r, "errors", []any{projectID, rg}, func(ctx context.Context) (Errors, error) {
		qh, qa, qs, qu := errorQueries(projectID, rg)
		var out Errors
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			rows, err := r.query(ctx, qh)
			if err != nil {
				return err
			}
			out.Handlers, err = scanAll(rows, func(v *HandlerError) []any {
				return []any{&v.Handler, &v.Error, &v.Count, &v.Users, &v.LastSeen}
			})
			return err
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qa)
			if err != nil {
				return err
			}
			out.API, err = scanAll(rows, func(v *APIError) []any {
				return []any{&v.Method, &v.Code, &v.Error, &v.Count, &v.Chats, &v.LastSeen}
			})
			return err
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qs)
			if err != nil {
				return err
			}
			type row struct {
				SlowHandler
				q []float64
			}
			raw, err := scanAll(rows, func(v *row) []any { return []any{&v.Handler, &v.Calls, &v.q, &v.MaxMS} })
			out.Slow = make([]SlowHandler, len(raw))
			for i, v := range raw {
				v.MS = quantiles(v.q)
				out.Slow[i] = v.SlowHandler
			}
			return err
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qu)
			if err != nil {
				return err
			}
			out.Unhandled, err = scanAll(rows, func(v *Unhandled) []any {
				return []any{&v.Name, &v.ContentType, &v.Count, &v.Users}
			})
			return err
		})
		if err := g.Wait(); err != nil {
			return Errors{}, err
		}
		for _, e := range out.API {
			if e.Code == 429 {
				out.RateLimited += e.Count
			}
		}
		return out, nil
	})
}
