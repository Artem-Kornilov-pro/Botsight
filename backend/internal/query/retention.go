package query

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// MaxRetentionWeeks: retention() takes up to 32 conditions, a half year is enough for a chart.
const MaxRetentionWeeks = 26

// Cohort is the users whose first update was in the week, and how many of them came back
// in each following week. Active[0] is the cohort week itself; weeks that have not
// started yet are left out, so rows of later cohorts are shorter.
type Cohort struct {
	Week   string    `json:"week"` // Monday, YYYY-MM-DD
	Users  uint64    `json:"users"`
	Active []uint64  `json:"active"`
	Share  []float64 `json:"share"`
}

type Retention struct {
	Weeks   int      `json:"weeks"`
	TZ      string   `json:"tz"`
	Cohorts []Cohort `json:"cohorts"`
}

// monday is the Monday of t's week in loc, as a date at UTC midnight.
func monday(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	return d.AddDate(0, 0, -((int(d.Weekday()) + 6) % 7))
}

func retentionQuery(projectID uint32, weeks int, tz string, now time.Time) (Query, time.Time, error) {
	if weeks < 1 || weeks > MaxRetentionWeeks {
		return Query{}, time.Time{}, fmt.Errorf("weeks must be 1 to %d", MaxRetentionWeeks)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return Query{}, time.Time{}, err
	}
	thisWeek := monday(now, loc)
	start := thisWeek.AddDate(0, 0, -7*(weeks-1))
	startLocal := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, loc)

	p := Params{}
	tzp := p.P("tz", "String", tz)
	pid := p.P("pid", "UInt32", projectID)
	week := "toMonday(e.ts, " + tzp + ")"
	conds := make([]string, weeks)
	for i := range conds {
		conds[i] = week + " = c.cohort"
		if i > 0 {
			conds[i] += " + INTERVAL " + strconv.Itoa(i) + " WEEK"
		}
	}
	// retention(c0, c1, ...) gives r[k] = 1 when the user was active in the cohort week
	// and in week k (Botsight.md, 13.2); sumForEach adds the arrays of a cohort
	return Query{p, `
SELECT toString(cohort) AS week, count() AS users, sumForEach(r) AS active
FROM
(
    SELECT e.user_id, c.cohort AS cohort,
        retention(
            ` + strings.Join(conds, ",\n            ") + `) AS r
    FROM events AS e
    INNER JOIN
    (
        SELECT user_id, toMonday(min(ts), ` + tzp + `) AS cohort
        FROM events
        WHERE project_id = ` + pid + ` AND type = 'update' AND user_id != 0
        GROUP BY user_id
        HAVING cohort >= ` + p.P("start", "Date", start.Format(time.DateOnly)) + `
    ) AS c ON c.user_id = e.user_id
    WHERE e.project_id = ` + pid + ` AND e.type = 'update' AND e.user_id != 0
      AND e.ts >= ` + p.P("start_ts", dt64, startLocal) + `
    GROUP BY e.user_id, cohort
)
GROUP BY cohort
ORDER BY cohort`}, thisWeek, nil
}

// Retention builds weekly cohorts of the last weeks, the current one included.
func (r *Runner) Retention(ctx context.Context, projectID uint32, weeks int, tz string, now time.Time) (Retention, bool, error) {
	q, thisWeek, err := retentionQuery(projectID, weeks, tz, now)
	if err != nil {
		return Retention{}, false, err
	}
	// the key holds the current week: cohorts change only with new events, and the TTL bounds that
	return cached(ctx, r, "retention", []any{projectID, weeks, tz, thisWeek}, func(ctx context.Context) (Retention, error) {
		rows, err := r.query(ctx, q)
		if err != nil {
			return Retention{}, err
		}
		type row struct {
			week   string
			users  uint64
			active []uint64
		}
		raw, err := scanAll(rows, func(v *row) []any { return []any{&v.week, &v.users, &v.active} })
		if err != nil {
			return Retention{}, err
		}
		out := Retention{Weeks: weeks, TZ: tz, Cohorts: make([]Cohort, 0, len(raw))}
		for _, v := range raw {
			out.Cohorts = append(out.Cohorts, cohort(v.week, v.users, v.active, thisWeek))
		}
		return out, nil
	})
}

// cohort trims the weeks that have not begun and computes the shares.
func cohort(week string, users uint64, active []uint64, thisWeek time.Time) Cohort {
	c := Cohort{Week: week, Users: users, Active: active}
	if w, err := time.Parse(time.DateOnly, week); err == nil {
		seen := int(thisWeek.Sub(w).Hours()/(24*7)) + 1
		if seen < len(c.Active) {
			c.Active = c.Active[:max(seen, 0)]
		}
	}
	c.Share = make([]float64, len(c.Active))
	for i, a := range c.Active {
		c.Share[i] = share(a, users)
	}
	return c
}
