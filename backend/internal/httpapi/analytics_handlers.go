package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/query"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

// Analytics of a project from the events in ClickHouse (Botsight.md, 13.3), any role.
// Heavy answers are cached for a minute; X-Cache tells whether this one was.

const codeQueryTooHeavy = "query_too_heavy"

const defaultDays = 30

var tzRe = regexp.MustCompile(`^[A-Za-z]+(?:/[A-Za-z0-9_+-]+){0,2}$`)

// parseRange reads ?from&to&tz. from and to are dates YYYY-MM-DD in tz (to inclusive)
// or RFC 3339 instants. By default: the last 30 days, today included. Minutes are
// rounded so that repeated requests of the dashboard share the cache.
func parseRange(r *http.Request, now time.Time) (query.Range, *fieldError) {
	q := r.URL.Query()
	tz := q.Get("tz")
	if tz == "" {
		tz = "UTC"
	}
	loc, err := time.LoadLocation(tz)
	if err != nil || !tzRe.MatchString(tz) {
		return query.Range{}, invalid("tz", "tz must be an IANA time zone, e.g. Europe/Moscow")
	}
	parse := func(field string, endOfDay bool) (time.Time, *fieldError) {
		raw := q.Get(field)
		if d, err := time.ParseInLocation(dateLayout, raw, loc); err == nil {
			if endOfDay {
				d = d.AddDate(0, 0, 1)
			}
			return d.UTC(), nil
		}
		t, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return time.Time{}, invalid(field, field+" must be a date YYYY-MM-DD or an RFC 3339 time")
		}
		return t.UTC(), nil
	}
	rg := query.Range{TZ: tz}
	var ferr *fieldError
	if q.Has("to") {
		if rg.To, ferr = parse("to", true); ferr != nil {
			return query.Range{}, ferr
		}
	} else {
		rg.To = now.UTC().Truncate(time.Minute).Add(time.Minute)
	}
	if q.Has("from") {
		if rg.From, ferr = parse("from", false); ferr != nil {
			return query.Range{}, ferr
		}
	} else {
		last := rg.To.Add(-time.Millisecond).In(loc)
		rg.From = time.Date(last.Year(), last.Month(), last.Day()-(defaultDays-1), 0, 0, 0, 0, loc).UTC()
	}
	rg.From = rg.From.Truncate(time.Minute)
	if rem := rg.To.Truncate(time.Minute); !rem.Equal(rg.To) {
		rg.To = rem.Add(time.Minute)
	}
	if !rg.From.Before(rg.To) {
		return query.Range{}, invalid("from", "from must be before to")
	}
	if rg.To.Sub(rg.From) > query.MaxRange {
		return query.Range{}, invalid("from", "the period is at most 400 days")
	}
	return rg, nil
}

func (s *Server) analytics(w http.ResponseWriter) bool {
	if s.queries == nil {
		writeError(w, http.StatusServiceUnavailable, codeAnalyticsOff, "analytics storage is not configured")
		return false
	}
	return true
}

func (s *Server) writeAnalytics(w http.ResponseWriter, r *http.Request, v any, hit bool, err error) {
	if errors.Is(err, query.ErrTooHeavy) {
		writeError(w, http.StatusUnprocessableEntity, codeQueryTooHeavy, "the query is too heavy, narrow the period")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if hit {
		w.Header().Set("X-Cache", "hit")
	} else {
		w.Header().Set("X-Cache", "miss")
	}
	writeJSON(w, http.StatusOK, v)
}

// rangeView echoes the period actually used, after defaults and rounding.
type rangeView struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	TZ   string    `json:"tz"`
}

func viewRange(rg query.Range) rangeView { return rangeView{rg.From, rg.To, rg.TZ} }

func (s *Server) overview(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	rg, ferr := parseRange(r, time.Now())
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	o, hit, err := s.queries.Overview(r.Context(), uint32(projectFrom(r).ID), rg)
	s.writeAnalytics(w, r, struct {
		Range rangeView `json:"range"`
		query.Overview
	}{viewRange(rg), o}, hit, err)
}

func (s *Server) fsmGraph(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	rg, ferr := parseRange(r, time.Now())
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	p := projectFrom(r)
	g, hit, err := s.queries.Graph(r.Context(), uint32(p.ID), rg, p.Settings.NeutralHandlers)
	s.writeAnalytics(w, r, struct {
		Range rangeView `json:"range"`
		query.Graph
	}{viewRange(rg), g}, hit, err)
}

func (s *Server) errorsReport(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	rg, ferr := parseRange(r, time.Now())
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	e, hit, err := s.queries.Errors(r.Context(), uint32(projectFrom(r).ID), rg)
	s.writeAnalytics(w, r, struct {
		Range rangeView `json:"range"`
		query.Errors
	}{viewRange(rg), e}, hit, err)
}

// queryFunnel computes a funnel by its definition; it reads only, so any role may.
func (s *Server) queryFunnel(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	var req struct {
		Definition FunnelDefinition `json:"definition"`
	}
	if !decode(w, r, &req) {
		return
	}
	if ferr := validateFunnel(&req.Definition); ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	rg, ferr := parseRange(r, time.Now())
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	def := query.FunnelDefinition{WindowSeconds: req.Definition.WindowSeconds, Breakdown: req.Definition.Breakdown}
	for _, st := range req.Definition.Steps {
		def.Steps = append(def.Steps, query.Step{Kind: st.Kind, Value: st.Value})
	}
	f, hit, err := s.queries.Funnel(r.Context(), uint32(projectFrom(r).ID), rg, def)
	s.writeAnalytics(w, r, struct {
		Range rangeView `json:"range"`
		query.Funnel
	}{viewRange(rg), f}, hit, err)
}

type sourceView struct {
	query.Source
	SpendRub          *json.Number `json:"spend_rub"`
	CostPerUserRub    *json.Number `json:"cost_per_user_rub"`
	CostPerConversion *json.Number `json:"cost_per_conversion_rub"`
}

// sources: first touch attribution with conversion to the goal and the cost of a user
// from the ad spend (Botsight.md, 13.1).
func (s *Server) sources(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	rg, ferr := parseRange(r, time.Now())
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	var goal *query.Step
	if raw := r.URL.Query().Get("goal"); raw != "" {
		st, err := query.ParseStep(raw)
		if err != nil || len(st.Value) > 256 {
			writeInvalid(w, invalid("goal", "goal is kind:value, kind is command, callback_data, state or custom"))
			return
		}
		goal = &st
	}
	p := projectFrom(r)
	list, hit, err := s.queries.Sources(r.Context(), uint32(p.ID), rg, goal)
	if err != nil {
		s.writeAnalytics(w, r, nil, false, err)
		return
	}
	first, last := rangeDays(rg)
	spend, err := s.Store.AdSpendList(r.Context(), p.ID, first, last)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	s.writeAnalytics(w, r, map[string]any{
		"range":   viewRange(rg),
		"goal":    goal,
		"sources": withSpend(list, spend, first, last),
	}, hit, nil)
}

// rangeDays is the first and the last calendar day of the period in its time zone, as
// UTC midnights like the dates of ad spend.
func rangeDays(rg query.Range) (time.Time, time.Time) {
	loc, err := time.LoadLocation(rg.TZ)
	if err != nil {
		loc = time.UTC
	}
	day := func(t time.Time) time.Time {
		t = t.In(loc)
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	}
	return day(rg.From), day(rg.To.Add(-time.Millisecond))
}

// withSpend adds the spend of every source over the days of the period. Spend whose
// dates only partly fall in the period counts in proportion to the days. A source with
// spend and no users is listed too: money went there and brought nobody.
func withSpend(list []query.Source, spend []store.AdSpend, first, last time.Time) []sourceView {
	bySource := map[string]int64{}
	for _, a := range spend {
		from, to := maxTime(a.From, first), minTime(a.To, last)
		if to.Before(from) {
			continue
		}
		overlap := int64(to.Sub(from).Hours()/24) + 1
		total := int64(a.To.Sub(a.From).Hours()/24) + 1
		bySource[a.Source] += (a.AmountKopek*overlap + total/2) / total
	}
	out := make([]sourceView, 0, len(list)+len(bySource))
	seen := map[string]bool{}
	for _, src := range list {
		seen[src.Source] = true
		v := sourceView{Source: src}
		if k, ok := bySource[src.Source]; ok {
			v.SpendRub = rubles(k)
			if src.Users > 0 {
				v.CostPerUserRub = rubles((k + int64(src.Users)/2) / int64(src.Users))
			}
			if src.Converted > 0 {
				v.CostPerConversion = rubles((k + int64(src.Converted)/2) / int64(src.Converted))
			}
		}
		out = append(out, v)
	}
	var orphans []string
	for name := range bySource {
		if !seen[name] {
			orphans = append(orphans, name)
		}
	}
	sort.Strings(orphans)
	for _, name := range orphans {
		out = append(out, sourceView{Source: query.Source{Source: name}, SpendRub: rubles(bySource[name])})
	}
	return out
}

func rubles(kopeks int64) *json.Number {
	n := formatRubles(kopeks)
	return &n
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Server) retention(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	q := r.URL.Query()
	weeks := 8
	if v := q.Get("weeks"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > query.MaxRetentionWeeks {
			writeInvalid(w, invalid("weeks", "weeks is 1 to 26"))
			return
		}
		weeks = n
	}
	tz := q.Get("tz")
	if tz == "" {
		tz = "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil || !tzRe.MatchString(tz) {
		writeInvalid(w, invalid("tz", "tz must be an IANA time zone, e.g. Europe/Moscow"))
		return
	}
	ret, hit, err := s.queries.Retention(r.Context(), uint32(projectFrom(r).ID), weeks, tz, time.Now())
	s.writeAnalytics(w, r, ret, hit, err)
}

// timeline: the events of one user, newest first, for debugging a complaint.
func (s *Server) timeline(w http.ResponseWriter, r *http.Request) {
	if !s.analytics(w) {
		return
	}
	userID, err := strconv.ParseUint(chi.URLParam(r, "userID"), 10, 64)
	if err != nil || userID == 0 {
		writeError(w, http.StatusNotFound, codeNotFound, "user not found")
		return
	}
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeInvalid(w, invalid("limit", "limit is 1 to 500"))
			return
		}
		limit = n
	}
	var after *query.Cursor
	if v := q.Get("cursor"); v != "" {
		c, err := query.ParseCursor(v)
		if err != nil {
			writeInvalid(w, invalid("cursor", "invalid cursor"))
			return
		}
		after = &c
	}
	// an hour ahead: events of a bot whose clock runs fast are still shown
	tl, err := s.queries.Timeline(r.Context(), uint32(projectFrom(r).ID), userID, after, time.Now().Add(time.Hour), limit)
	if err != nil {
		s.writeAnalytics(w, r, nil, false, err)
		return
	}
	writeJSON(w, http.StatusOK, tl)
}
