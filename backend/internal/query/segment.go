package query

import (
	"context"
	"errors"
	"strconv"
	"time"
)

// Segment chooses the recipients of a broadcast (Botsight.md, 22) from the analytics:
// users who wrote to the bot in a private chat within ActiveDays, optionally quiet for
// InactiveDays (to bring them back), from one traffic source, who did or did not make a
// step. Users who blocked the bot after their last message are never included.
type Segment struct {
	ActiveDays   int     `json:"active_days"`
	InactiveDays int     `json:"inactive_days,omitempty"`
	Source       *string `json:"source,omitempty"` // first /start label; "" for none
	Reached      *Step   `json:"reached,omitempty"`
	NotReached   *Step   `json:"not_reached,omitempty"`
}

const (
	MaxSegmentDays = 365
	defaultActive  = 30
)

func (s *Segment) Normalize() error {
	if s.ActiveDays == 0 {
		s.ActiveDays = defaultActive
	}
	if s.ActiveDays < 1 || s.ActiveDays > MaxSegmentDays {
		return errors.New("active_days is 1 to 365")
	}
	if s.InactiveDays < 0 || s.InactiveDays >= s.ActiveDays {
		return errors.New("inactive_days must be less than active_days")
	}
	for _, st := range []*Step{s.Reached, s.NotReached} {
		if st != nil && (!validKind(st.Kind) || st.Value == "" || len(st.Value) > 256) {
			return errors.New("a step is kind:value with kind command, callback_data, state or custom")
		}
	}
	return nil
}

// segmentQuery selects chat ids, or counts them. A private chat id equals the user id,
// so the recipient is the user.
func segmentQuery(projectID uint32, s Segment, count bool, limit int) (Query, error) {
	p := Params{}
	pid := p.P("pid", "UInt32", projectID)
	where := ""
	if s.InactiveDays > 0 {
		where += "\n  AND u.last_seen < now64(3) - toIntervalDay(" + p.P("inactive", "UInt32", uint32(s.InactiveDays)) + ")"
	}
	if s.Source != nil {
		where += `
  AND u.user_id IN (
      SELECT user_id FROM user_first_start WHERE project_id = ` + pid + `
      GROUP BY user_id HAVING argMinMerge(source) = ` + p.P("source", "String", *s.Source) + `)`
	}
	for i, st := range []*Step{s.Reached, s.NotReached} {
		if st == nil {
			continue
		}
		cond, err := st.cond(p, "step"+strconv.Itoa(i))
		if err != nil {
			return Query{}, err
		}
		op := "IN"
		if i == 1 {
			op = "NOT IN"
		}
		where += "\n  AND u.user_id " + op + " (SELECT DISTINCT user_id FROM events WHERE project_id = " + pid + " AND user_id != 0 AND " + cond + ")"
	}
	sel, tail := "toInt64(u.user_id)", "\nORDER BY u.user_id\nLIMIT "+p.P("max_rows", "UInt32", uint32(limit))
	if count {
		sel, tail = "count()", ""
	}
	return Query{p, `
SELECT ` + sel + `
FROM
(
    SELECT user_id, max(ts) AS last_seen
    FROM events
    WHERE project_id = ` + pid + ` AND type = 'update' AND chat_type = 'private' AND user_id != 0
      AND chat_id = toInt64(user_id) AND ts >= now64(3) - toIntervalDay(` + p.P("active", "UInt32", uint32(s.ActiveDays)) + `)
    GROUP BY user_id
) AS u
LEFT JOIN
(
    SELECT blocked_user AS user_id, max(ts) AS blocked_at
    FROM
    (
        SELECT ` + blockedUser + ` AS blocked_user, ts
        FROM events
        WHERE project_id = ` + pid + ` AND type IN ('outgoing', 'update')
    )
    WHERE blocked_user != 0
    GROUP BY blocked_user
) AS b ON b.user_id = u.user_id
WHERE b.blocked_at < u.last_seen` + where + tail}, nil
}

// SegmentSize counts the recipients; not cached, the owner checks it right before sending.
func (r *Runner) SegmentSize(ctx context.Context, projectID uint32, s Segment) (uint64, error) {
	defer r.Metrics.took("segment", time.Now())
	q, err := segmentQuery(projectID, s, true, 0)
	if err != nil {
		return 0, err
	}
	var n uint64
	err = r.Conn.QueryRow(r.ctx(ctx), q.SQL, q.Args()...).Scan(&n)
	return n, mapErr(err)
}

// SegmentChats returns up to limit chat ids of the segment.
func (r *Runner) SegmentChats(ctx context.Context, projectID uint32, s Segment, limit int) ([]int64, error) {
	defer r.Metrics.took("segment", time.Now())
	q, err := segmentQuery(projectID, s, false, limit)
	if err != nil {
		return nil, err
	}
	rows, err := r.query(ctx, q)
	if err != nil {
		return nil, err
	}
	return scanAll(rows, func(v *int64) []any { return []any{v} })
}
