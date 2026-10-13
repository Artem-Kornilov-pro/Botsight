package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
	"unicode/utf8"
)

// AI data lives in ClickHouse, written by ai-worker and ch-writer (Botsight.md, 14).
// Members of any role may read it and ask questions.

const (
	codeAnalyticsOff = "analytics_unavailable"
	codeAIOff        = "ai_unavailable"
)

type topicView struct {
	ID             uint32    `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	Keywords       []string  `json:"keywords"`
	Size           uint32    `json:"unique_texts"`
	Messages7d     uint32    `json:"messages_7d"`
	MessagesPrev7d uint32    `json:"messages_prev_7d"`
	Share7d        float64   `json:"share_7d"`
	SharePrev7d    float64   `json:"share_prev_7d"`
	Growth         float64   `json:"growth"`
	NamedBy        string    `json:"named_by"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (s *Server) requireClickHouse(w http.ResponseWriter) bool {
	if s.ClickHouse == nil {
		writeError(w, http.StatusServiceUnavailable, codeAnalyticsOff, "analytics storage is not configured")
		return false
	}
	return true
}

// listTopics: current topics of the project's messages with their trends.
func (s *Server) listTopics(w http.ResponseWriter, r *http.Request) {
	if !s.requireClickHouse(w) {
		return
	}
	rows, err := s.ClickHouse.Query(r.Context(),
		`SELECT topic_id, name, description, keywords, size, messages_7d, messages_prev_7d,
		        share_7d, share_prev_7d, growth, named_by, updated_at
		 FROM topics FINAL WHERE project_id = ? AND active = 1
		 ORDER BY messages_7d DESC, topic_id LIMIT 100`, uint32(projectFrom(r).ID))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()
	out := []topicView{}
	for rows.Next() {
		var t topicView
		if err := rows.Scan(&t.ID, &t.Name, &t.Description, &t.Keywords, &t.Size, &t.Messages7d,
			&t.MessagesPrev7d, &t.Share7d, &t.SharePrev7d, &t.Growth, &t.NamedBy, &t.UpdatedAt); err != nil {
			s.internalError(w, r, err)
			return
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"topics": out})
}

type messageView struct {
	EventID string    `json:"event_id"`
	TS      time.Time `json:"ts"`
	Text    string    `json:"text"`
}

// topicExamples: typical messages of a topic. Texts are personal data: they are shown
// only here, to members of the project (Botsight.md, 12.2).
func (s *Server) topicExamples(w http.ResponseWriter, r *http.Request) {
	if !s.requireClickHouse(w) {
		return
	}
	topicID, ok := pathID(r, "topicID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "topic not found")
		return
	}
	pid := uint32(projectFrom(r).ID)
	var found uint64
	if err := s.ClickHouse.QueryRow(r.Context(),
		`SELECT count() FROM topics FINAL WHERE project_id = ? AND topic_id = ? AND active = 1`,
		pid, uint32(topicID)).Scan(&found); err != nil {
		s.internalError(w, r, err)
		return
	}
	if found == 0 {
		writeError(w, http.StatusNotFound, codeNotFound, "topic not found")
		return
	}
	rows, err := s.ClickHouse.Query(r.Context(),
		`SELECT toString(event_id), ts, text FROM messages_text
		 WHERE project_id = ? AND event_id IN (
		     SELECT arrayJoin(example_event_ids) FROM topics FINAL WHERE project_id = ? AND topic_id = ?)
		 ORDER BY ts DESC LIMIT 20`, pid, pid, uint32(topicID))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()
	out := []messageView{}
	for rows.Next() {
		var m messageView
		if err := rows.Scan(&m.EventID, &m.TS, &m.Text); err != nil {
			s.internalError(w, r, err)
			return
		}
		out = append(out, m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

type alertView struct {
	ID        string          `json:"id"`
	Kind      string          `json:"kind"`
	CreatedAt time.Time       `json:"created_at"`
	Title     string          `json:"title"`
	Text      string          `json:"text"`
	Details   json.RawMessage `json:"details"`
}

// listAlerts: topic trends and anomalies, newest first.
func (s *Server) listAlerts(w http.ResponseWriter, r *http.Request) {
	if !s.requireClickHouse(w) {
		return
	}
	rows, err := s.ClickHouse.Query(r.Context(),
		`SELECT toString(alert_id), kind, created_at, title, text, details
		 FROM alerts FINAL WHERE project_id = ? ORDER BY created_at DESC LIMIT 100`,
		uint32(projectFrom(r).ID))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	defer rows.Close()
	out := []alertView{}
	for rows.Next() {
		var a alertView
		var details string
		if err := rows.Scan(&a.ID, &a.Kind, &a.CreatedAt, &a.Title, &a.Text, &details); err != nil {
			s.internalError(w, r, err)
			return
		}
		a.Details = json.RawMessage("{}")
		if json.Valid([]byte(details)) {
			a.Details = json.RawMessage(details)
		}
		out = append(out, a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": out})
}

// ask forwards a question to ai-worker (Botsight.md, 14.2). Membership is checked here,
// ai-worker trusts the project_id because only the api knows the internal token.
func (s *Server) ask(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Question string `json:"question"`
	}
	if !decode(w, r, &req) {
		return
	}
	if n := utf8.RuneCountInString(req.Question); n == 0 || n > 500 {
		writeInvalid(w, invalid("question", "a question is 1 to 500 characters"))
		return
	}
	if s.Options.AIURL == "" {
		writeError(w, http.StatusServiceUnavailable, codeAIOff, "the AI module is not configured")
		return
	}
	if !s.allow(w, r, s.Options.Limits.AskAccount, strconv.FormatInt(sessionFrom(r).AccountID, 10)) {
		return
	}
	body, _ := json.Marshal(map[string]any{"project_id": projectFrom(r).ID, "question": req.Question})
	upstream, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		s.Options.AIURL+"/internal/ask", bytes.NewReader(body))
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	upstream.Header.Set("Content-Type", "application/json")
	upstream.Header.Set("X-Internal-Token", s.Options.AIToken)
	resp, err := s.HTTPClient.Do(upstream)
	if err != nil {
		s.Logger.Warn("ai-worker unavailable", "err", err)
		writeError(w, http.StatusBadGateway, codeAIOff, "the AI module is unavailable")
		return
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil || !json.Valid(payload) {
		writeError(w, http.StatusBadGateway, codeAIOff, "invalid answer from the AI module")
		return
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusBadRequest, http.StatusUnprocessableEntity,
		http.StatusBadGateway, http.StatusServiceUnavailable:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(payload)
	default: // 401 means a token mismatch between services: our problem, not the user's
		s.Logger.Error("ai-worker rejected the request", "status", resp.StatusCode)
		writeError(w, http.StatusBadGateway, codeAIOff, "the AI module is unavailable")
	}
}
