package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/antifraud"
)

// Antifraud results of the latest scoring run (Botsight.md, 15): source_risk is written
// for every project with active users on each run, so its latest updated_at marks the run.

const latestRun = `(SELECT max(updated_at) FROM source_risk WHERE project_id = ?)`

type sourceRiskView struct {
	Source     string            `json:"source"`
	Users      uint32            `json:"users"`
	Suspicious uint32            `json:"suspicious"`
	Share      float64           `json:"share"`
	AvgScore   float32           `json:"avg_score"`
	Reasons    map[string]uint32 `json:"reasons"`
}

func reasonTexts() map[string]string {
	out := make(map[string]string, len(antifraud.Reasons))
	for _, r := range antifraud.Reasons {
		out[r.Code] = r.Text
	}
	return out
}

// antifraudSummary: share of suspicious users per traffic source over the last 24 hours.
func (s *Server) antifraudSummary(w http.ResponseWriter, r *http.Request) {
	if !s.requireClickHouse(w) {
		return
	}
	pid := uint32(projectFrom(r).ID)
	rows, err := s.ClickHouse.Query(r.Context(),
		`SELECT source, users, suspicious, avg_score, reasons, updated_at FROM source_risk FINAL
		 WHERE project_id = ? AND updated_at = `+latestRun+`
		 ORDER BY suspicious DESC, users DESC, source LIMIT 500`, pid, pid)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()
	sources := []sourceRiskView{}
	var updated *time.Time
	var users, suspicious uint32
	for rows.Next() {
		var v sourceRiskView
		var at time.Time
		if err := rows.Scan(&v.Source, &v.Users, &v.Suspicious, &v.AvgScore, &v.Reasons, &at); err != nil {
			s.internalError(w, r, err)
			return
		}
		if v.Users > 0 {
			v.Share = float64(v.Suspicious) / float64(v.Users)
		}
		users += v.Users
		suspicious += v.Suspicious
		updated = &at
		sources = append(sources, v)
	}
	if err := rows.Err(); err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"updated_at":   updated,
		"window_hours": int(antifraud.Window.Hours()),
		"threshold":    antifraud.SuspiciousScore,
		"users":        users,
		"suspicious":   suspicious,
		"sources":      sources,
		"reason_texts": reasonTexts(),
	})
}

type userRiskView struct {
	UserID  string    `json:"user_id"` // a string: pseudonymized ids exceed JavaScript's 2^53
	Source  string    `json:"source"`
	Joined  time.Time `json:"joined"`
	Score   float32   `json:"score"`
	Reasons []string  `json:"reasons"`
}

// antifraudUsers: suspicious users of the latest run, optionally of one source.
func (s *Server) antifraudUsers(w http.ResponseWriter, r *http.Request) {
	if !s.requireClickHouse(w) {
		return
	}
	pid := uint32(projectFrom(r).ID)
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeInvalid(w, invalid("limit", "limit is 1 to 1000"))
			return
		}
		limit = n
	}
	query := `SELECT toString(user_id), source, joined, score, reasons FROM user_risk FINAL
		WHERE project_id = ? AND updated_at = ` + latestRun
	args := []any{pid, pid}
	if src, ok := r.URL.Query()["source"]; ok {
		query += ` AND source = ?`
		args = append(args, src[0])
	}
	query += ` ORDER BY score DESC, joined DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.ClickHouse.Query(r.Context(), query, args...)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()
	out := []userRiskView{}
	for rows.Next() {
		var v userRiskView
		if err := rows.Scan(&v.UserID, &v.Source, &v.Joined, &v.Score, &v.Reasons); err != nil {
			s.internalError(w, r, err)
			return
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}
