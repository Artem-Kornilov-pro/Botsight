package query

import (
	"context"
	"math"
	"time"

	"golang.org/x/sync/errgroup"
)

// Overview is the main screen (Botsight.md, 16.1): audience, blocks, errors, handler
// speed and deploy marks over a period, with a series by day.
type Overview struct {
	Users         uint64    `json:"users"` // with at least one update in the period
	NewUsers      uint64    `json:"new_users"`
	WAU           uint64    `json:"wau"` // 7 and 30 days before the end of the period
	MAU           uint64    `json:"mau"`
	Blocked       uint64    `json:"blocked"`
	Updates       uint64    `json:"updates"`
	Unhandled     uint64    `json:"unhandled"`
	HandlerErrors uint64    `json:"handler_errors"`
	APIErrors     uint64    `json:"api_errors"`
	HandlerMS     Quantiles `json:"handler_ms"`
	Days          []Day     `json:"days"`
	Deploys       []Deploy  `json:"deploys"`
}

type Quantiles struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
}

func quantiles(q []float64) Quantiles {
	clean := func(i int) float64 {
		if i >= len(q) || math.IsNaN(q[i]) || math.IsInf(q[i], 0) { // no steps in the period
			return 0
		}
		return math.Round(q[i]*10) / 10
	}
	return Quantiles{P50: clean(0), P95: clean(1), P99: clean(2)}
}

type Day struct {
	Day           string `json:"day"` // YYYY-MM-DD in the requested time zone
	DAU           uint64 `json:"dau"`
	NewUsers      uint64 `json:"new_users"`
	Blocked       uint64 `json:"blocked"`
	Updates       uint64 `json:"updates"`
	Unhandled     uint64 `json:"unhandled"`
	HandlerErrors uint64 `json:"handler_errors"`
	APIErrors     uint64 `json:"api_errors"`
}

// Deploy is a bot start (lifecycle bot_started): a mark on the charts.
type Deploy struct {
	TS               time.Time `json:"ts"`
	SDKVersion       string    `json:"sdk_version"`
	Framework        string    `json:"framework"`
	FrameworkVersion string    `json:"framework_version"`
}

const counters = `
    countIf(type = 'update') AS updates,
    countIf(type = 'update' AND handled = 0) AS unhandled,
    uniqExactIf(` + blockedUser + ` AS blocked_user, blocked_user != 0) AS blocked,
    countIf(type = 'step' AND error != '') AS handler_errors,
    countIf(type = 'outgoing' AND ok = 0) AS api_errors`

func overviewQueries(projectID uint32, rg Range) (daily, totals, newUsers, active, deploys Query) {
	p := Params{}
	tz := p.P("tz", "String", rg.TZ)
	where := scope(p, projectID, rg)

	daily = Query{p, `
SELECT toString(toDate(ts, ` + tz + `)) AS day,
    uniqExactIf(user_id, type = 'update' AND user_id != 0) AS dau,` + counters + `
FROM events
WHERE ` + where + `
GROUP BY day
ORDER BY day`}

	totals = Query{p, `
SELECT uniqExactIf(user_id, type = 'update' AND user_id != 0) AS users,` + counters + `,
    quantilesIf(0.5, 0.95, 0.99)(duration_ms, type = 'step') AS handler_ms
FROM events
WHERE ` + where}

	// a user is new on the day of their first update ever, not of the period
	newUsers = Query{p, `
SELECT toString(toDate(first_seen, ` + tz + `)) AS day, count() AS users
FROM
(
    SELECT user_id, min(ts) AS first_seen
    FROM events
    WHERE project_id = {pid:UInt32} AND type = 'update' AND user_id != 0 AND ts < {to:` + dt64 + `}
    GROUP BY user_id
    HAVING first_seen >= {from:` + dt64 + `}
)
GROUP BY day
ORDER BY day`}

	active = Query{p, `
SELECT uniqExactIf(user_id, ts >= {to:` + dt64 + `} - INTERVAL 7 DAY) AS wau, uniqExact(user_id) AS mau
FROM events
WHERE project_id = {pid:UInt32} AND type = 'update' AND user_id != 0
  AND ts >= {to:` + dt64 + `} - INTERVAL 30 DAY AND ts < {to:` + dt64 + `}`}

	deploys = Query{p, `
SELECT ts, props_str['sdk_version'], props_str['framework'], props_str['framework_version']
FROM events
WHERE ` + where + ` AND type = 'lifecycle' AND name = 'bot_started'
ORDER BY ts
LIMIT 500`}
	return
}

// Overview runs the five queries of the screen in parallel; the result is cached.
func (r *Runner) Overview(ctx context.Context, projectID uint32, rg Range) (Overview, bool, error) {
	return cached(ctx, r, "overview", []any{projectID, rg}, func(ctx context.Context) (Overview, error) {
		qDaily, qTotals, qNew, qActive, qDeploys := overviewQueries(projectID, rg)
		var (
			out   Overview
			days  []Day
			fresh []Day
		)
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			rows, err := r.query(ctx, qDaily)
			if err != nil {
				return err
			}
			days, err = scanAll(rows, func(d *Day) []any {
				return []any{&d.Day, &d.DAU, &d.Updates, &d.Unhandled, &d.Blocked, &d.HandlerErrors, &d.APIErrors}
			})
			return err
		})
		g.Go(func() error {
			var q []float64
			err := r.Conn.QueryRow(r.ctx(ctx), qTotals.SQL, qTotals.Args()...).Scan(
				&out.Users, &out.Updates, &out.Unhandled, &out.Blocked, &out.HandlerErrors, &out.APIErrors, &q)
			out.HandlerMS = quantiles(q)
			return mapErr(err)
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qNew)
			if err != nil {
				return err
			}
			fresh, err = scanAll(rows, func(d *Day) []any { return []any{&d.Day, &d.NewUsers} })
			return err
		})
		g.Go(func() error {
			return mapErr(r.Conn.QueryRow(r.ctx(ctx), qActive.SQL, qActive.Args()...).Scan(&out.WAU, &out.MAU))
		})
		g.Go(func() error {
			rows, err := r.query(ctx, qDeploys)
			if err != nil {
				return err
			}
			out.Deploys, err = scanAll(rows, func(d *Deploy) []any {
				return []any{&d.TS, &d.SDKVersion, &d.Framework, &d.FrameworkVersion}
			})
			return err
		})
		if err := g.Wait(); err != nil {
			return Overview{}, err
		}
		out.Days, out.NewUsers = fillDays(rg, days, fresh)
		return out, nil
	})
}

// fillDays returns every day of the period, with zeros where nothing happened, and the
// number of new users. Charts need the gaps: a day without events is a real zero.
func fillDays(rg Range, days, fresh []Day) ([]Day, uint64) {
	loc, err := time.LoadLocation(rg.TZ)
	if err != nil {
		loc = time.UTC
	}
	byDay := make(map[string]*Day, len(days))
	for i := range days {
		byDay[days[i].Day] = &days[i]
	}
	var total uint64
	for _, f := range fresh {
		total += f.NewUsers
		if d, ok := byDay[f.Day]; ok {
			d.NewUsers = f.NewUsers
		} else {
			// unreachable in practice: a first update is an event of that day
			days = append(days, f)
			byDay[f.Day] = &days[len(days)-1]
		}
	}
	first := rg.From.In(loc)
	last := rg.To.Add(-time.Millisecond).In(loc)
	start := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, time.UTC)
	end := time.Date(last.Year(), last.Month(), last.Day(), 0, 0, 0, 0, time.UTC)
	out := make([]Day, 0, int(end.Sub(start).Hours()/24)+1)
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		key := d.Format(time.DateOnly)
		if v, ok := byDay[key]; ok {
			out = append(out, *v)
		} else {
			out = append(out, Day{Day: key})
		}
	}
	return out, total
}
