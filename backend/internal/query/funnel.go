package query

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// FunnelDefinition is a funnel built in the dashboard (Botsight.md, 13.2, 16.1).
type FunnelDefinition struct {
	Steps         []Step `json:"steps"`
	WindowSeconds int    `json:"window_seconds"`
	Breakdown     string `json:"breakdown"` // "" or "source"
}

type Funnel struct {
	Steps     []FunnelStep    `json:"steps"`
	Breakdown []FunnelSegment `json:"breakdown,omitempty"`
}

// FunnelStep: users who reached the step in order within the window.
type FunnelStep struct {
	Step      Step    `json:"step"`
	Users     uint64  `json:"users"`
	FromPrev  float64 `json:"from_prev"`  // share of the previous step
	FromFirst float64 `json:"from_first"` // share of the first step
}

type FunnelSegment struct {
	Source string       `json:"source"` // first /start label, "" for none
	Steps  []FunnelStep `json:"steps"`
}

// maxSegments keeps the breakdown readable: the rest of the sources are dropped.
const maxSegments = 50

func funnelQuery(projectID uint32, rg Range, def FunnelDefinition) (Query, error) {
	if len(def.Steps) < 2 || len(def.Steps) > 20 {
		return Query{}, fmt.Errorf("a funnel has 2 to 20 steps")
	}
	if def.WindowSeconds <= 0 {
		return Query{}, fmt.Errorf("window must be positive")
	}
	p := Params{}
	where := scope(p, projectID, rg)
	conds := make([]string, len(def.Steps))
	for i, st := range def.Steps {
		c, err := st.cond(p, "s"+strconv.Itoa(i))
		if err != nil {
			return Query{}, err
		}
		conds[i] = c
	}
	// windowFunnel returns the number of the last step reached in order (Botsight.md, 13.2);
	// only events of the steps are read
	inner := `
    SELECT user_id, windowFunnel(` + p.P("window", "UInt64", uint64(def.WindowSeconds)) + `)(toDateTime(ts),
        ` + strings.Join(conds, ",\n        ") + `) AS level
    FROM events
    WHERE ` + where + ` AND user_id != 0
      AND (` + strings.Join(conds, " OR ") + `)
    GROUP BY user_id`

	if def.Breakdown != "source" {
		return Query{p, `
SELECT '' AS source, level, count() AS users
FROM
(` + inner + `
)
WHERE level > 0
GROUP BY level`}, nil
	}
	return Query{p, `
SELECT s.source AS source, f.level AS level, count() AS users
FROM
(` + inner + `
) AS f
LEFT JOIN
(
    SELECT user_id, argMinMerge(source) AS source
    FROM user_first_start
    WHERE project_id = {pid:UInt32}
    GROUP BY user_id
) AS s ON s.user_id = f.user_id
WHERE f.level > 0
GROUP BY source, level`}, nil
}

type levelCount struct {
	Source string
	Level  uint8
	Users  uint64
}

// Funnel counts the users of every step; with the source breakdown also per source.
func (r *Runner) Funnel(ctx context.Context, projectID uint32, rg Range, def FunnelDefinition) (Funnel, bool, error) {
	q, err := funnelQuery(projectID, rg, def)
	if err != nil {
		return Funnel{}, false, err
	}
	return cached(ctx, r, "funnel", []any{projectID, rg, def}, func(ctx context.Context) (Funnel, error) {
		rows, err := r.query(ctx, q)
		if err != nil {
			return Funnel{}, err
		}
		levels, err := scanAll(rows, func(l *levelCount) []any { return []any{&l.Source, &l.Level, &l.Users} })
		if err != nil {
			return Funnel{}, err
		}
		return buildFunnel(def, levels), nil
	})
}

// buildFunnel turns "users whose last step is k" into "users who reached step k":
// the sum over all levels >= k.
func buildFunnel(def FunnelDefinition, levels []levelCount) Funnel {
	n := len(def.Steps)
	total := make([]uint64, n)
	bySource := map[string][]uint64{}
	for _, l := range levels {
		if l.Level == 0 || int(l.Level) > n {
			continue
		}
		seg := bySource[l.Source]
		if seg == nil {
			seg = make([]uint64, n)
			bySource[l.Source] = seg
		}
		for k := 0; k < int(l.Level); k++ {
			total[k] += l.Users
			seg[k] += l.Users
		}
	}
	out := Funnel{Steps: funnelSteps(def.Steps, total)}
	if def.Breakdown != "source" {
		return out
	}
	out.Breakdown = []FunnelSegment{}
	for src, counts := range bySource {
		out.Breakdown = append(out.Breakdown, FunnelSegment{Source: src, Steps: funnelSteps(def.Steps, counts)})
	}
	sort.Slice(out.Breakdown, func(i, j int) bool {
		a, b := out.Breakdown[i], out.Breakdown[j]
		if a.Steps[0].Users != b.Steps[0].Users {
			return a.Steps[0].Users > b.Steps[0].Users
		}
		return a.Source < b.Source
	})
	if len(out.Breakdown) > maxSegments {
		out.Breakdown = out.Breakdown[:maxSegments]
	}
	return out
}

func funnelSteps(steps []Step, reached []uint64) []FunnelStep {
	out := make([]FunnelStep, len(steps))
	for i, st := range steps {
		out[i] = FunnelStep{Step: st, Users: reached[i]}
		if i > 0 {
			out[i].FromPrev = share(reached[i], reached[i-1])
		} else if reached[0] > 0 {
			out[i].FromPrev = 1
		}
		out[i].FromFirst = share(reached[i], reached[0])
	}
	return out
}

// share is a/b rounded to 4 digits, 0 for an empty base.
func share(a, b uint64) float64 {
	if b == 0 {
		return 0
	}
	return math.Round(float64(a)/float64(b)*1e4) / 1e4
}
