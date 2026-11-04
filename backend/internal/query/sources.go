package query

import "context"

// Source is the audience of one deep link label by the first touch (Botsight.md, 13.2):
// users whose first /start came in the period, how many reached the goal and how many
// blocked the bot since then.
type Source struct {
	Source     string  `json:"source"` // "" for /start without a label
	Users      uint64  `json:"users"`
	Converted  uint64  `json:"converted"`
	Conversion float64 `json:"conversion"`
	Blocked    uint64  `json:"blocked"`
	BlockRate  float64 `json:"block_rate"`
}

func sourcesQuery(projectID uint32, rg Range, goal *Step) (Query, error) {
	p := Params{}
	pid := p.P("pid", "UInt32", projectID)
	from := p.P("from", dt64, rg.From)
	to := p.P("to", dt64, rg.To)

	converted, goalJoin := "toUInt64(0)", ""
	if goal != nil {
		cond, err := goal.cond(p, "goal")
		if err != nil {
			return Query{}, err
		}
		// a conversion counts at any time after the start of the period
		converted = "countIf(g.user_id != 0)"
		goalJoin = `
LEFT JOIN
(
    SELECT DISTINCT user_id
    FROM events
    WHERE project_id = ` + pid + ` AND ts >= ` + from + ` AND user_id != 0 AND ` + cond + `
) AS g ON g.user_id = u.user_id`
	}

	// first touch comes from the aggregate filled by user_first_start_mv (Botsight.md, 12.3)
	return Query{p, `
SELECT u.source AS source, count() AS users, ` + converted + ` AS converted, countIf(b.user_id != 0) AS blocked
FROM
(
    SELECT user_id, argMinMerge(source) AS source
    FROM user_first_start
    WHERE project_id = ` + pid + `
    GROUP BY user_id
    HAVING min(first_start) >= ` + from + ` AND min(first_start) < ` + to + `
) AS u` + goalJoin + `
LEFT JOIN
(
    SELECT DISTINCT blocked_user AS user_id
    FROM
    (
        SELECT ` + blockedUser + ` AS blocked_user
        FROM events
        WHERE project_id = ` + pid + ` AND ts >= ` + from + ` AND type IN ('outgoing', 'update')
    )
    WHERE blocked_user != 0
) AS b ON b.user_id = u.user_id
GROUP BY source
ORDER BY users DESC, source
LIMIT 500`}, nil
}

// Sources lists traffic sources by the number of users; goal may be nil.
func (r *Runner) Sources(ctx context.Context, projectID uint32, rg Range, goal *Step) ([]Source, bool, error) {
	q, err := sourcesQuery(projectID, rg, goal)
	if err != nil {
		return nil, false, err
	}
	return cached(ctx, r, "sources", []any{projectID, rg, goal}, func(ctx context.Context) ([]Source, error) {
		rows, err := r.query(ctx, q)
		if err != nil {
			return nil, err
		}
		out, err := scanAll(rows, func(s *Source) []any { return []any{&s.Source, &s.Users, &s.Converted, &s.Blocked} })
		for i := range out {
			out[i].Conversion = share(out[i].Converted, out[i].Users)
			out[i].BlockRate = share(out[i].Blocked, out[i].Users)
		}
		return out, err
	})
}
