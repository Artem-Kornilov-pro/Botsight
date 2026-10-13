package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// Roles in a project, from most to least privileged.
const (
	RoleOwner  = "owner"
	RoleEditor = "editor"
	RoleViewer = "viewer"
)

// RoleAtLeast reports whether role grants at least the rights of min.
func RoleAtLeast(role, min string) bool {
	return roleRank(role) >= roleRank(min)
}

func roleRank(role string) int {
	switch role {
	case RoleOwner:
		return 3
	case RoleEditor:
		return 2
	case RoleViewer:
		return 1
	}
	return 0
}

// ProjectSettings is stored in projects.settings (JSONB).
type ProjectSettings struct {
	CaptureText     bool     `json:"capture_text"`
	NeutralHandlers []string `json:"neutral_handlers"`
}

type Project struct {
	ID        int32
	Name      string
	BotID     *int64
	Plan      string
	Settings  ProjectSettings
	CreatedAt time.Time
	Role      string // role of the requesting account
}

type APIKey struct {
	ID         int32
	ProjectID  int32
	Prefix     string
	Name       string
	CreatedBy  *int64
	CreatedAt  time.Time
	RevokedAt  *time.Time
	secretHash []byte
}

// SecretHash is set only by KeyByPrefix.
func (k APIKey) SecretHash() []byte { return k.secretHash }

// NewKey is a key to store: prefix and SHA-256 of the full key.
type NewKey struct {
	Prefix string
	Hash   []byte
	Name   string
}

func scanProject(row interface{ Scan(...any) error }) (Project, error) {
	var p Project
	var settings []byte
	err := row.Scan(&p.ID, &p.Name, &p.BotID, &p.Plan, &settings, &p.CreatedAt, &p.Role)
	if err != nil {
		return p, mapErr(err)
	}
	if err := json.Unmarshal(settings, &p.Settings); err != nil {
		return p, err
	}
	if p.Settings.NeutralHandlers == nil {
		p.Settings.NeutralHandlers = []string{}
	}
	return p, nil
}

const projectColumns = `p.id, p.name, p.bot_id, p.plan, p.settings, p.created_at, m.role`

// CreateProject creates a project with the creator as owner and its first API key (Botsight.md, 16.2).
func (s *Store) CreateProject(ctx context.Context, name string, ownerID int64, key NewKey) (Project, APIKey, error) {
	var project Project
	var apiKey APIKey
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		project, err = scanProject(tx.QueryRow(ctx,
			`WITH p AS (
			     INSERT INTO projects (name, created_by) VALUES ($1, $2)
			     RETURNING id, name, bot_id, plan, settings, created_at
			 ), m AS (
			     INSERT INTO project_members (project_id, account_id, role)
			     SELECT id, $2, 'owner' FROM p RETURNING role
			 )
			 SELECT `+projectColumns+` FROM p, m`, name, ownerID))
		if err != nil {
			return err
		}
		apiKey, err = insertKey(ctx, tx, project.ID, key, ownerID)
		return err
	})
	return project, apiKey, err
}

// Projects lists the projects the account is a member of.
func (s *Store) Projects(ctx context.Context, accountID int64) ([]Project, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+projectColumns+` FROM projects p
		 JOIN project_members m ON m.project_id = p.id AND m.account_id = $1
		 ORDER BY p.id`, accountID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Project{}
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// Project returns the project as seen by a member. ErrNotFound if the account
// is not a member: outsiders cannot tell a foreign project from a missing one.
func (s *Store) Project(ctx context.Context, projectID int32, accountID int64) (Project, error) {
	return scanProject(s.pool.QueryRow(ctx,
		`SELECT `+projectColumns+` FROM projects p
		 JOIN project_members m ON m.project_id = p.id AND m.account_id = $2
		 WHERE p.id = $1`, projectID, accountID))
}

// UpdateProject changes the name and/or settings; nil means "keep".
func (s *Store) UpdateProject(ctx context.Context, projectID int32, accountID int64, name *string, settings *ProjectSettings) (Project, error) {
	var settingsJSON []byte
	if settings != nil {
		var err error
		if settingsJSON, err = json.Marshal(settings); err != nil {
			return Project{}, err
		}
	}
	return scanProject(s.pool.QueryRow(ctx,
		`WITH p AS (
		     UPDATE projects SET name = COALESCE($2, name), settings = COALESCE($3::jsonb, settings)
		     WHERE id = $1
		     RETURNING id, name, bot_id, plan, settings, created_at
		 )
		 SELECT `+projectColumns+` FROM p JOIN project_members m ON m.project_id = p.id AND m.account_id = $4`,
		projectID, name, settingsJSON, accountID))
}

// DeleteProject removes the project with all its data and returns the prefixes
// of keys that were still active, so collectors can drop them from caches.
func (s *Store) DeleteProject(ctx context.Context, projectID int32) ([]string, error) {
	var prefixes []string
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx,
			`SELECT prefix FROM api_keys WHERE project_id = $1 AND revoked_at IS NULL`, projectID)
		if err != nil {
			return err
		}
		prefixes, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM projects WHERE id = $1`, projectID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return nil
	})
	return prefixes, err
}

// --- members ---

type Member struct {
	AccountID        int64
	Email            *string
	TelegramUsername *string
	Name             string
	Role             string
	AddedAt          time.Time
}

func (s *Store) Members(ctx context.Context, projectID int32) ([]Member, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT a.id, a.email, a.telegram_username, a.name, m.role, m.created_at
		 FROM project_members m JOIN accounts a ON a.id = m.account_id
		 WHERE m.project_id = $1
		 ORDER BY m.created_at, a.id`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Member{}
	for rows.Next() {
		var m Member
		if err := rows.Scan(&m.AccountID, &m.Email, &m.TelegramUsername, &m.Name, &m.Role, &m.AddedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// AddMember adds an account to the project. ErrConflict if it is already a member.
func (s *Store) AddMember(ctx context.Context, projectID int32, accountID int64, role string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO project_members (project_id, account_id, role) VALUES ($1, $2, $3)`,
		projectID, accountID, role)
	return mapErr(err)
}

// SetMemberRole changes a role. ErrLastOwner if it would leave the project without owners.
func (s *Store) SetMemberRole(ctx context.Context, projectID int32, accountID int64, role string) error {
	return s.changeMembers(ctx, projectID, accountID, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx,
			`UPDATE project_members SET role = $3 WHERE project_id = $1 AND account_id = $2`,
			projectID, accountID, role)
		return tag.RowsAffected(), err
	})
}

// RemoveMember removes an account. ErrLastOwner if it is the only owner.
func (s *Store) RemoveMember(ctx context.Context, projectID int32, accountID int64) error {
	return s.changeMembers(ctx, projectID, accountID, func(tx pgx.Tx) (int64, error) {
		tag, err := tx.Exec(ctx,
			`DELETE FROM project_members WHERE project_id = $1 AND account_id = $2`,
			projectID, accountID)
		return tag.RowsAffected(), err
	})
}

// changeMembers serializes membership changes per project (row lock on the
// project) and checks that at least one owner remains.
func (s *Store) changeMembers(ctx context.Context, projectID int32, accountID int64, change func(pgx.Tx) (int64, error)) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT 1 FROM projects WHERE id = $1 FOR UPDATE`, projectID); err != nil {
			return err
		}
		affected, err := change(tx)
		if err != nil {
			return mapErr(err)
		}
		if affected == 0 {
			return ErrNotFound
		}
		var owners int
		if err := tx.QueryRow(ctx,
			`SELECT count(*) FROM project_members WHERE project_id = $1 AND role = 'owner'`,
			projectID).Scan(&owners); err != nil {
			return err
		}
		if owners == 0 {
			return ErrLastOwner
		}
		return nil
	})
}

// --- API keys ---

const keyColumns = `id, project_id, prefix, name, created_by, created_at, revoked_at`

func scanKey(row interface{ Scan(...any) error }) (APIKey, error) {
	var k APIKey
	err := row.Scan(&k.ID, &k.ProjectID, &k.Prefix, &k.Name, &k.CreatedBy, &k.CreatedAt, &k.RevokedAt)
	return k, mapErr(err)
}

func insertKey(ctx context.Context, q querier, projectID int32, key NewKey, createdBy int64) (APIKey, error) {
	return scanKey(q.QueryRow(ctx,
		`INSERT INTO api_keys (project_id, prefix, secret_hash, name, created_by)
		 VALUES ($1, $2, $3, $4, $5) RETURNING `+keyColumns,
		projectID, key.Prefix, key.Hash, key.Name, createdBy))
}

// CreateKey stores a new key. ErrConflict on a prefix collision (generate a new key and retry).
func (s *Store) CreateKey(ctx context.Context, projectID int32, key NewKey, createdBy int64) (APIKey, error) {
	return insertKey(ctx, s.pool, projectID, key, createdBy)
}

func (s *Store) Keys(ctx context.Context, projectID int32) ([]APIKey, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+keyColumns+` FROM api_keys WHERE project_id = $1 ORDER BY id`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []APIKey{}
	for rows.Next() {
		k, err := scanKey(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// RevokeKey revokes a key of the project. Revoking an already revoked key is a
// no-op; newlyRevoked tells whether collectors must be notified.
func (s *Store) RevokeKey(ctx context.Context, projectID, keyID int32) (prefix string, newlyRevoked bool, err error) {
	err = s.pool.QueryRow(ctx,
		`UPDATE api_keys SET revoked_at = now()
		 WHERE project_id = $1 AND id = $2 AND revoked_at IS NULL RETURNING prefix`,
		projectID, keyID).Scan(&prefix)
	if err == nil {
		return prefix, true, nil
	}
	if err = mapErr(err); err != ErrNotFound {
		return "", false, err
	}
	err = s.pool.QueryRow(ctx,
		`SELECT prefix FROM api_keys WHERE project_id = $1 AND id = $2`, projectID, keyID).Scan(&prefix)
	return prefix, false, mapErr(err)
}

// KeyByPrefix is the collector lookup (Botsight.md, 10.2): the caller compares
// the presented key with SecretHash() via auth.VerifyAPIKey and checks RevokedAt.
func (s *Store) KeyByPrefix(ctx context.Context, prefix string) (APIKey, error) {
	var k APIKey
	err := s.pool.QueryRow(ctx,
		`SELECT `+keyColumns+`, secret_hash FROM api_keys WHERE prefix = $1`, prefix).
		Scan(&k.ID, &k.ProjectID, &k.Prefix, &k.Name, &k.CreatedBy, &k.CreatedAt, &k.RevokedAt, &k.secretHash)
	return k, mapErr(err)
}
