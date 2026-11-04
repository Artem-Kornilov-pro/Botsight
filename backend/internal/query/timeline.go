package query

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// TimelineWindow: how far back one page of a user's timeline looks from its cursor.
const TimelineWindow = 90 * 24 * time.Hour

// TimelineEvent is one event of a user, newest first (Botsight.md, 16.1). Bot API
// requests have no user, they are matched by the private chat with the user.
type TimelineEvent struct {
	EventID      string             `json:"event_id"`
	TS           time.Time          `json:"ts"`
	Type         string             `json:"type"`
	Name         string             `json:"name"`
	ChatID       int64              `json:"chat_id,omitempty"`
	ContentType  string             `json:"content_type,omitempty"`
	Command      string             `json:"command,omitempty"`
	StartParam   string             `json:"start_param,omitempty"`
	CallbackData string             `json:"callback_data,omitempty"`
	State        string             `json:"state,omitempty"`
	Handler      string             `json:"handler,omitempty"`
	StateFrom    string             `json:"state_from,omitempty"`
	StateTo      string             `json:"state_to,omitempty"`
	MemberStatus string             `json:"member_status,omitempty"`
	Handled      bool               `json:"handled"`
	OK           bool               `json:"ok"`
	DurationMS   uint32             `json:"duration_ms"`
	ErrorCode    uint16             `json:"error_code,omitempty"`
	Error        string             `json:"error,omitempty"`
	PropsStr     map[string]string  `json:"props_str,omitempty"`
	PropsNum     map[string]float64 `json:"props_num,omitempty"`
}

type Timeline struct {
	Events []TimelineEvent `json:"events"`
	// NextCursor continues the list with older events; empty when the window is over.
	NextCursor string `json:"next_cursor,omitempty"`
}

// Cursor is the position after the last returned event: (ts, event_id) is unique, so
// events of the same millisecond are neither lost nor repeated between pages.
type Cursor struct {
	TS      time.Time
	EventID uuid.UUID
}

func (c Cursor) String() string {
	return strconv.FormatInt(c.TS.UnixMilli(), 10) + "_" + c.EventID.String()
}

var ErrBadCursor = errors.New("invalid cursor")

func ParseCursor(s string) (Cursor, error) {
	ms, id, ok := strings.Cut(s, "_")
	n, err := strconv.ParseInt(ms, 10, 64)
	if !ok || err != nil || n < 0 {
		return Cursor{}, ErrBadCursor
	}
	u, err := uuid.Parse(id)
	if err != nil {
		return Cursor{}, ErrBadCursor
	}
	return Cursor{TS: time.UnixMilli(n).UTC(), EventID: u}, nil
}

func timelineQuery(projectID uint32, userID uint64, after *Cursor, now time.Time, limit int) Query {
	p := Params{}
	who := "user_id = " + p.P("uid", "UInt64", userID)
	if userID <= math.MaxInt64 {
		who = "(" + who + " OR (type = 'outgoing' AND chat_id = " + p.P("chat", "Int64", int64(userID)) + "))"
	}
	before := now
	page := ""
	if after != nil {
		before = after.TS.Add(time.Millisecond)
		page = " AND (ts, event_id) < (" + p.P("cur_ts", dt64, after.TS) + ", " + p.P("cur_id", "UUID", after.EventID.String()) + ")"
	}
	return Query{p, `
SELECT toString(event_id), ts, type, name, chat_id, content_type, command, start_param, callback_data,
    state, handler, state_from, state_to, member_status, handled, ok, duration_ms, error_code, error,
    props_str, props_num
FROM events
WHERE project_id = ` + p.P("pid", "UInt32", projectID) + `
  AND ts >= ` + p.P("from", dt64, before.Add(-TimelineWindow)) + ` AND ts < ` + p.P("to", dt64, before) + `
  AND ` + who + page + `
ORDER BY ts DESC, event_id DESC
LIMIT ` + p.P("page_size", "UInt32", uint32(limit))}
}

// Timeline returns up to limit events of the user before the cursor, or before now.
// It reads a narrow slice of the sort key and is not cached.
func (r *Runner) Timeline(ctx context.Context, projectID uint32, userID uint64, after *Cursor, now time.Time, limit int) (Timeline, error) {
	defer r.Metrics.took("timeline", time.Now())
	rows, err := r.query(ctx, timelineQuery(projectID, userID, after, now, limit))
	if err != nil {
		return Timeline{}, err
	}
	events, err := scanAll(rows, func(e *TimelineEvent) []any {
		return []any{&e.EventID, &e.TS, &e.Type, &e.Name, &e.ChatID, &e.ContentType, &e.Command, &e.StartParam,
			&e.CallbackData, &e.State, &e.Handler, &e.StateFrom, &e.StateTo, &e.MemberStatus, &e.Handled, &e.OK,
			&e.DurationMS, &e.ErrorCode, &e.Error, &e.PropsStr, &e.PropsNum}
	})
	if err != nil {
		return Timeline{}, err
	}
	out := Timeline{Events: events}
	if len(events) == limit {
		last := events[len(events)-1]
		id, _ := uuid.Parse(last.EventID)
		out.NextCursor = Cursor{TS: last.TS, EventID: id}.String()
	}
	return out, nil
}
