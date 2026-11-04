package store

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Broadcasts (Botsight.md, 22): recipients are a job queue in PostgreSQL. The SDK leases
// a slice of pending rows with FOR UPDATE SKIP LOCKED, so replicas of one bot never get
// the same recipient; a lease expires by itself if the bot dies before reporting.

// MaxRecipients bounds one broadcast; larger segments are split by the owner.
const MaxRecipients = 100_000

// ErrNotSending: the broadcast is already done or canceled.
var ErrNotSending = errors.New("store: broadcast is not sending")

type Broadcast struct {
	ID         int32
	Text       string
	ParseMode  string
	Segment    []byte
	Status     string // sending, done, canceled
	Total      int32
	CreatedAt  time.Time
	FinishedAt *time.Time
	Stats      BroadcastStats
}

type BroadcastStats struct {
	Pending, Sent, Blocked, Failed int64
	Errors                         map[string]int64 // failure reasons, filled for one broadcast
}

// CreateBroadcast stores the broadcast and its recipients in one transaction; the
// recipients go in with COPY, which writes 100 000 rows in well under a second.
func (s *Store) CreateBroadcast(ctx context.Context, projectID int32, text, parseMode string, segment []byte, chatIDs []int64, createdBy int64) (Broadcast, error) {
	var b Broadcast
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx,
			`INSERT INTO broadcasts (project_id, text, parse_mode, segment, total, created_by)
			 VALUES ($1, $2, $3, $4, $5, NULLIF($6, 0)) RETURNING `+broadcastColumns,
			projectID, text, parseMode, segment, len(chatIDs), createdBy).
			Scan(&b.ID, &b.Text, &b.ParseMode, &b.Segment, &b.Status, &b.Total, &b.CreatedAt, &b.FinishedAt)
		if err != nil {
			return err
		}
		_, err = tx.CopyFrom(ctx, pgx.Identifier{"broadcast_recipients"}, []string{"broadcast_id", "chat_id"},
			pgx.CopyFromSlice(len(chatIDs), func(i int) ([]any, error) { return []any{b.ID, chatIDs[i]}, nil }))
		return err
	})
	b.Stats.Pending = int64(b.Total)
	return b, mapErr(err)
}

const broadcastColumns = `id, text, parse_mode, segment, status, total, created_at, finished_at`

const statsColumns = `
    count(*) FILTER (WHERE r.status = 'pending'),
    count(*) FILTER (WHERE r.status = 'sent'),
    count(*) FILTER (WHERE r.status = 'blocked'),
    count(*) FILTER (WHERE r.status = 'failed')`

func scanBroadcast(row pgx.Row) (Broadcast, error) {
	var b Broadcast
	err := row.Scan(&b.ID, &b.Text, &b.ParseMode, &b.Segment, &b.Status, &b.Total, &b.CreatedAt, &b.FinishedAt,
		&b.Stats.Pending, &b.Stats.Sent, &b.Stats.Blocked, &b.Stats.Failed)
	return b, mapErr(err)
}

// Broadcasts lists the latest broadcasts of the project with their progress.
func (s *Store) Broadcasts(ctx context.Context, projectID int32) ([]Broadcast, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT b.id, b.text, b.parse_mode, b.segment, b.status, b.total, b.created_at, b.finished_at,`+statsColumns+`
		FROM broadcasts b LEFT JOIN broadcast_recipients r ON r.broadcast_id = b.id
		WHERE b.project_id = $1
		GROUP BY b.id ORDER BY b.id DESC LIMIT 50`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Broadcast{}
	for rows.Next() {
		b, err := scanBroadcast(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// Broadcast returns one broadcast with its progress and failure reasons.
func (s *Store) Broadcast(ctx context.Context, projectID, id int32) (Broadcast, error) {
	b, err := scanBroadcast(s.pool.QueryRow(ctx, `
		SELECT b.id, b.text, b.parse_mode, b.segment, b.status, b.total, b.created_at, b.finished_at,`+statsColumns+`
		FROM broadcasts b LEFT JOIN broadcast_recipients r ON r.broadcast_id = b.id
		WHERE b.project_id = $1 AND b.id = $2
		GROUP BY b.id`, projectID, id))
	if err != nil {
		return b, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT error, count(*) FROM broadcast_recipients
		WHERE broadcast_id = $1 AND status IN ('blocked', 'failed') AND error != ''
		GROUP BY error ORDER BY count(*) DESC LIMIT 20`, id)
	if err != nil {
		return b, err
	}
	defer rows.Close()
	b.Stats.Errors = map[string]int64{}
	for rows.Next() {
		var reason string
		var n int64
		if err := rows.Scan(&reason, &n); err != nil {
			return b, err
		}
		b.Stats.Errors[reason] = n
	}
	return b, rows.Err()
}

// CancelBroadcast stops leasing; recipients already leased may still get the message.
func (s *Store) CancelBroadcast(ctx context.Context, projectID, id int32) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE broadcasts SET status = 'canceled', finished_at = now()
		 WHERE project_id = $1 AND id = $2 AND status = 'sending'`, projectID, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM broadcasts WHERE project_id = $1 AND id = $2)`,
		projectID, id).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrNotFound
	}
	return ErrNotSending
}

// Lease is a slice of recipients given to one SDK instance until Until.
type Lease struct {
	BroadcastID int32
	Text        string
	ParseMode   string
	ChatIDs     []int64
	Until       time.Time
}

// LeaseRecipients gives up to limit free recipients of the oldest sending broadcast of
// the project. A recipient leased maxAttempts times without a report is failed: the bot
// keeps dying on it, or its messages never get confirmed. Broadcasts with nothing left
// are finished on the way. active reports whether the project still has broadcasts
// sending, so the caller can stop asking PostgreSQL.
func (s *Store) LeaseRecipients(ctx context.Context, projectID int32, limit int, lease time.Duration, maxAttempts int) (l *Lease, active bool, err error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, text, parse_mode FROM broadcasts WHERE project_id = $1 AND status = 'sending' ORDER BY id LIMIT 5`,
		projectID)
	if err != nil {
		return nil, false, err
	}
	var sending []Lease
	for rows.Next() {
		var c Lease
		if err := rows.Scan(&c.BroadcastID, &c.Text, &c.ParseMode); err != nil {
			rows.Close()
			return nil, false, err
		}
		sending = append(sending, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	for _, c := range sending {
		var until time.Time
		rows, err := s.pool.Query(ctx, `
			WITH picked AS (
				SELECT chat_id FROM broadcast_recipients
				WHERE broadcast_id = $1 AND status = 'pending' AND attempts < $4
				  AND (leased_until IS NULL OR leased_until < now())
				ORDER BY chat_id
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			)
			UPDATE broadcast_recipients r
			SET leased_until = now() + make_interval(secs => $3), attempts = r.attempts + 1
			FROM picked
			WHERE r.broadcast_id = $1 AND r.chat_id = picked.chat_id
			RETURNING r.chat_id, r.leased_until`, c.BroadcastID, limit, lease.Seconds(), maxAttempts)
		if err != nil {
			return nil, false, err
		}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id, &until); err != nil {
				rows.Close()
				return nil, false, err
			}
			c.ChatIDs = append(c.ChatIDs, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, false, err
		}
		if len(c.ChatIDs) > 0 {
			c.Until = until
			return &c, true, nil
		}
		done, err := s.finishBroadcast(ctx, c.BroadcastID, maxAttempts)
		if err != nil {
			return nil, false, err
		}
		if !done {
			active = true // others hold leases on what is left
		}
	}
	return nil, active || len(sending) == 5, nil
}

// finishBroadcast fails the recipients out of attempts and marks the broadcast done
// when nothing is pending.
func (s *Store) finishBroadcast(ctx context.Context, id int32, maxAttempts int) (bool, error) {
	if _, err := s.pool.Exec(ctx,
		`UPDATE broadcast_recipients SET status = 'failed', error = 'not_confirmed', updated_at = now()
		 WHERE broadcast_id = $1 AND status = 'pending' AND attempts >= $2 AND leased_until < now()`,
		id, maxAttempts); err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE broadcasts SET status = 'done', finished_at = now()
		 WHERE id = $1 AND status = 'sending'
		   AND NOT EXISTS (SELECT 1 FROM broadcast_recipients WHERE broadcast_id = $1 AND status = 'pending')`, id)
	return tag.RowsAffected() == 1, err
}

// DeliveryResult is the outcome of one message as the SDK reports it.
type DeliveryResult struct {
	ChatID int64
	Status string // sent, blocked, failed
	Error  string
}

// ReportResults records outcomes. Only pending rows change, so a repeated report is
// harmless; rows of another project's broadcast are never touched. Returns the rows
// updated.
func (s *Store) ReportResults(ctx context.Context, projectID, broadcastID int32, results []DeliveryResult) (int64, error) {
	ids := make([]int64, len(results))
	statuses := make([]string, len(results))
	errs := make([]string, len(results))
	for i, r := range results {
		ids[i], statuses[i], errs[i] = r.ChatID, r.Status, r.Error
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE broadcast_recipients r
		SET status = x.status, error = x.error, leased_until = NULL, updated_at = now()
		FROM unnest($3::bigint[], $4::text[], $5::text[]) AS x(chat_id, status, error)
		WHERE r.broadcast_id = $2 AND r.chat_id = x.chat_id AND r.status = 'pending'
		  AND EXISTS (SELECT 1 FROM broadcasts b WHERE b.id = $2 AND b.project_id = $1)`,
		projectID, broadcastID, ids, statuses, errs)
	if err != nil {
		return 0, mapErr(err)
	}
	return tag.RowsAffected(), nil
}

// ActiveBroadcastsKey is a Redis flag: the project has broadcasts sending. The api sets
// it on create, the collector drops it when leasing finds nothing, so thousands of idle
// bots polling for work never reach PostgreSQL.
func ActiveBroadcastsKey(projectID int32) string {
	return "bc:active:" + strconv.Itoa(int(projectID))
}
