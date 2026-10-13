package store

import (
	"context"
	"encoding/json"
	"time"
)

// --- saved funnels ---

type Funnel struct {
	ID         int32
	ProjectID  int32
	Name       string
	Definition json.RawMessage
	CreatedBy  *int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

const funnelColumns = `id, project_id, name, definition, created_by, created_at, updated_at`

func scanFunnel(row interface{ Scan(...any) error }) (Funnel, error) {
	var f Funnel
	err := row.Scan(&f.ID, &f.ProjectID, &f.Name, &f.Definition, &f.CreatedBy, &f.CreatedAt, &f.UpdatedAt)
	return f, mapErr(err)
}

func (s *Store) Funnels(ctx context.Context, projectID int32) ([]Funnel, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+funnelColumns+` FROM saved_funnels WHERE project_id = $1 ORDER BY id`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Funnel{}
	for rows.Next() {
		f, err := scanFunnel(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *Store) Funnel(ctx context.Context, projectID, funnelID int32) (Funnel, error) {
	return scanFunnel(s.pool.QueryRow(ctx,
		`SELECT `+funnelColumns+` FROM saved_funnels WHERE project_id = $1 AND id = $2`, projectID, funnelID))
}

func (s *Store) CreateFunnel(ctx context.Context, projectID int32, name string, definition []byte, createdBy int64) (Funnel, error) {
	return scanFunnel(s.pool.QueryRow(ctx,
		`INSERT INTO saved_funnels (project_id, name, definition, created_by) VALUES ($1, $2, $3, $4)
		 RETURNING `+funnelColumns, projectID, name, definition, createdBy))
}

func (s *Store) UpdateFunnel(ctx context.Context, projectID, funnelID int32, name string, definition []byte) (Funnel, error) {
	return scanFunnel(s.pool.QueryRow(ctx,
		`UPDATE saved_funnels SET name = $3, definition = $4, updated_at = now()
		 WHERE project_id = $1 AND id = $2 RETURNING `+funnelColumns, projectID, funnelID, name, definition))
}

func (s *Store) DeleteFunnel(ctx context.Context, projectID, funnelID int32) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM saved_funnels WHERE project_id = $1 AND id = $2`, projectID, funnelID)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// --- ad spend ---

// AdSpend is spend on one traffic source over a period of whole days, both ends inclusive.
type AdSpend struct {
	ID          int32
	Source      string
	From, To    time.Time // dates, UTC midnight
	AmountKopek int64
	CreatedAt   time.Time
}

const adSpendColumns = `id, source, lower(period), upper(period) - 1, (amount_rub * 100)::bigint, created_at`

func scanAdSpend(row interface{ Scan(...any) error }) (AdSpend, error) {
	var a AdSpend
	err := row.Scan(&a.ID, &a.Source, &a.From, &a.To, &a.AmountKopek, &a.CreatedAt)
	return a, mapErr(err)
}

// AdSpendList returns spend whose period intersects [from, to]; zero times mean unbounded.
func (s *Store) AdSpendList(ctx context.Context, projectID int32, from, to time.Time) ([]AdSpend, error) {
	var fromArg, toArg any
	if !from.IsZero() {
		fromArg = from
	}
	if !to.IsZero() {
		toArg = to
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+adSpendColumns+` FROM ad_spend
		 WHERE project_id = $1 AND period && daterange($2::date, $3::date, '[]')
		 ORDER BY lower(period), source, id`, projectID, fromArg, toArg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AdSpend{}
	for rows.Next() {
		a, err := scanAdSpend(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// CreateAdSpend stores spend. ErrOverlap if the source already has spend on any of these days.
func (s *Store) CreateAdSpend(ctx context.Context, projectID int32, source string, from, to time.Time, amountKopek int64) (AdSpend, error) {
	return scanAdSpend(s.pool.QueryRow(ctx,
		`INSERT INTO ad_spend (project_id, source, period, amount_rub)
		 VALUES ($1, $2, daterange($3::date, $4::date, '[]'), $5::numeric / 100)
		 RETURNING `+adSpendColumns, projectID, source, from, to, amountKopek))
}

func (s *Store) DeleteAdSpend(ctx context.Context, projectID, id int32) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM ad_spend WHERE project_id = $1 AND id = $2`, projectID, id)
	if err != nil {
		return mapErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
