package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/query"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

// Broadcasts to a segment (Botsight.md, 22). The dashboard picks the users from the
// analytics; the SDK inside the bot sends the messages, so the bot token stays with the
// owner. Members of any role see broadcasts, editors create and cancel them.

const (
	codeEmptySegment    = "empty_segment"
	codeSegmentTooLarge = "segment_too_large"
	codeNotSending      = "not_sending"
)

var parseModes = map[string]bool{"": true, "HTML": true, "MarkdownV2": true}

type broadcastStatsView struct {
	Pending int64            `json:"pending"`
	Sent    int64            `json:"sent"`
	Blocked int64            `json:"blocked"`
	Failed  int64            `json:"failed"`
	Errors  map[string]int64 `json:"errors,omitempty"`
}

type broadcastView struct {
	ID         int32              `json:"id"`
	Text       string             `json:"text"`
	ParseMode  string             `json:"parse_mode"`
	Segment    json.RawMessage    `json:"segment"`
	Status     string             `json:"status"`
	Total      int32              `json:"total"`
	Stats      broadcastStatsView `json:"stats"`
	CreatedAt  time.Time          `json:"created_at"`
	FinishedAt *time.Time         `json:"finished_at"`
}

func viewBroadcast(b store.Broadcast) broadcastView {
	return broadcastView{
		ID: b.ID, Text: b.Text, ParseMode: b.ParseMode, Segment: b.Segment, Status: b.Status, Total: b.Total,
		Stats: broadcastStatsView{
			Pending: b.Stats.Pending, Sent: b.Stats.Sent, Blocked: b.Stats.Blocked, Failed: b.Stats.Failed, Errors: b.Stats.Errors,
		},
		CreatedAt: b.CreatedAt, FinishedAt: b.FinishedAt,
	}
}

func readSegment(seg *query.Segment) *fieldError {
	for _, st := range []*query.Step{seg.Reached, seg.NotReached} {
		if st != nil {
			st.Value = strings.TrimSpace(st.Value)
			if st.Kind == "command" {
				st.Value = strings.TrimPrefix(st.Value, "/")
			}
		}
	}
	if seg.Source != nil && *seg.Source != "" && !sourceRe.MatchString(*seg.Source) {
		return invalid("segment.source", "source is a deep link label: A-Z, a-z, 0-9, _ and -, up to 64")
	}
	if err := seg.Normalize(); err != nil {
		return invalid("segment", err.Error())
	}
	return nil
}

func (s *Server) previewBroadcast(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	var req struct {
		Segment query.Segment `json:"segment"`
	}
	if !decode(w, r, &req) {
		return
	}
	if ferr := readSegment(&req.Segment); ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	n, err := s.queries.SegmentSize(r.Context(), uint32(projectFrom(r).ID), req.Segment)
	if err != nil {
		s.writeAnalytics(w, r, nil, false, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"segment": req.Segment, "recipients": n, "max_recipients": store.MaxRecipients})
}

func (s *Server) createBroadcast(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	var req struct {
		Text      string        `json:"text"`
		ParseMode string        `json:"parse_mode"`
		Segment   query.Segment `json:"segment"`
	}
	if !decode(w, r, &req) {
		return
	}
	if n := utf8.RuneCountInString(strings.TrimSpace(req.Text)); n == 0 || n > 4096 {
		writeInvalid(w, invalid("text", "text is 1 to 4096 characters, the limit of a Telegram message"))
		return
	}
	if !parseModes[req.ParseMode] {
		writeInvalid(w, invalid("parse_mode", "parse_mode is empty, HTML or MarkdownV2"))
		return
	}
	if ferr := readSegment(&req.Segment); ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	p := projectFrom(r)
	chats, err := s.queries.SegmentChats(r.Context(), uint32(p.ID), req.Segment, store.MaxRecipients+1)
	if err != nil {
		s.writeAnalytics(w, r, nil, false, err)
		return
	}
	switch {
	case len(chats) == 0:
		writeError(w, http.StatusUnprocessableEntity, codeEmptySegment, "nobody in the segment")
		return
	case len(chats) > store.MaxRecipients:
		writeError(w, http.StatusUnprocessableEntity, codeSegmentTooLarge, "a broadcast has at most 100 000 recipients, narrow the segment")
		return
	}
	segment, _ := json.Marshal(req.Segment)
	b, err := s.Store.CreateBroadcast(r.Context(), p.ID, req.Text, req.ParseMode, segment, chats, sessionFrom(r).AccountID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	// The flag lets bots find the broadcast at once. It is only a shortcut: without it the
	// collector still checks PostgreSQL once a minute per project, so a failure here
	// delays the broadcast and never loses it.
	if err := s.Redis.Set(r.Context(), store.ActiveBroadcastsKey(p.ID), 1, 0).Err(); err != nil {
		s.Logger.Warn("broadcast flag", "project_id", p.ID, "err", err)
	}
	writeJSON(w, http.StatusCreated, viewBroadcast(b))
}

func (s *Server) listBroadcasts(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Broadcasts(r.Context(), projectFrom(r).ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]broadcastView, len(list))
	for i, b := range list {
		out[i] = viewBroadcast(b)
	}
	writeJSON(w, http.StatusOK, map[string]any{"broadcasts": out})
}

func (s *Server) getBroadcast(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "broadcastID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "broadcast not found")
		return
	}
	b, err := s.Store.Broadcast(r.Context(), projectFrom(r).ID, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "broadcast not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, viewBroadcast(b))
}

func (s *Server) cancelBroadcast(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "broadcastID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "broadcast not found")
		return
	}
	switch err := s.Store.CancelBroadcast(r.Context(), projectFrom(r).ID, id); {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, codeNotFound, "broadcast not found")
	case errors.Is(err, store.ErrNotSending):
		writeError(w, http.StatusConflict, codeNotSending, "the broadcast is already finished or canceled")
	case err != nil:
		s.internalError(w, r, err)
	default:
		s.getBroadcast(w, r)
	}
}
