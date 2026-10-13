package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

// --- saved funnels ---

type funnelRequest struct {
	Name       string           `json:"name"`
	Definition FunnelDefinition `json:"definition"`
}

// read validates the request and returns the name and the normalized definition JSON.
func (req *funnelRequest) read() (string, []byte, *fieldError) {
	name, ferr := cleanName("name", req.Name, true)
	if ferr != nil {
		return "", nil, ferr
	}
	if ferr := validateFunnel(&req.Definition); ferr != nil {
		return "", nil, ferr
	}
	def, err := json.Marshal(req.Definition)
	if err != nil {
		return "", nil, invalid("definition", err.Error())
	}
	return name, def, nil
}

func (s *Server) writeFunnel(w http.ResponseWriter, r *http.Request, status int, f store.Funnel, err error) {
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "funnel not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	view, err := viewFunnel(f)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, status, view)
}

func (s *Server) listFunnels(w http.ResponseWriter, r *http.Request) {
	funnels, err := s.Store.Funnels(r.Context(), projectFrom(r).ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]funnelView, 0, len(funnels))
	for _, f := range funnels {
		v, err := viewFunnel(f)
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, map[string]any{"funnels": out})
}

func (s *Server) createFunnel(w http.ResponseWriter, r *http.Request) {
	var req funnelRequest
	if !decode(w, r, &req) {
		return
	}
	name, def, ferr := req.read()
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	f, err := s.Store.CreateFunnel(r.Context(), projectFrom(r).ID, name, def, sessionFrom(r).AccountID)
	s.writeFunnel(w, r, http.StatusCreated, f, err)
}

func (s *Server) getFunnel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "funnelID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "funnel not found")
		return
	}
	f, err := s.Store.Funnel(r.Context(), projectFrom(r).ID, id)
	s.writeFunnel(w, r, http.StatusOK, f, err)
}

func (s *Server) updateFunnel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "funnelID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "funnel not found")
		return
	}
	var req funnelRequest
	if !decode(w, r, &req) {
		return
	}
	name, def, ferr := req.read()
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	f, err := s.Store.UpdateFunnel(r.Context(), projectFrom(r).ID, id, name, def)
	s.writeFunnel(w, r, http.StatusOK, f, err)
}

func (s *Server) deleteFunnel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "funnelID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "funnel not found")
		return
	}
	err := s.Store.DeleteFunnel(r.Context(), projectFrom(r).ID, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "funnel not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- ad spend ---

const maxSpendDays = 366

func (s *Server) listAdSpend(w http.ResponseWriter, r *http.Request) {
	var from, to time.Time
	q := r.URL.Query()
	if v := q.Get("from"); v != "" {
		var ferr *fieldError
		if from, ferr = parseDate("from", v); ferr != nil {
			writeInvalid(w, ferr)
			return
		}
	}
	if v := q.Get("to"); v != "" {
		var ferr *fieldError
		if to, ferr = parseDate("to", v); ferr != nil {
			writeInvalid(w, ferr)
			return
		}
	}
	items, err := s.Store.AdSpendList(r.Context(), projectFrom(r).ID, from, to)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]adSpendView, len(items))
	for i, a := range items {
		out[i] = viewAdSpend(a)
	}
	writeJSON(w, http.StatusOK, map[string]any{"ad_spend": out})
}

func (s *Server) createAdSpend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Source    string      `json:"source"`
		From      string      `json:"from"`
		To        string      `json:"to"`
		AmountRub json.Number `json:"amount_rub"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !sourceRe.MatchString(req.Source) {
		writeInvalid(w, invalid("source", "source is a deep link label: 1-64 of A-Z, a-z, 0-9, _ and -"))
		return
	}
	from, ferr := parseDate("from", req.From)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	to, ferr := parseDate("to", req.To)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	if to.Before(from) {
		writeInvalid(w, invalid("to", "to must not be before from"))
		return
	}
	if to.Sub(from) >= maxSpendDays*24*time.Hour {
		writeInvalid(w, invalid("to", "a period is at most 366 days"))
		return
	}
	kopeks, ferr := parseRubles(req.AmountRub)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	item, err := s.Store.CreateAdSpend(r.Context(), projectFrom(r).ID, req.Source, from, to, kopeks)
	if errors.Is(err, store.ErrOverlap) {
		writeError(w, http.StatusConflict, codePeriodOverlap, "this source already has spend on some of these days")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, viewAdSpend(item))
}

func (s *Server) deleteAdSpend(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(r, "spendID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "ad spend not found")
		return
	}
	err := s.Store.DeleteAdSpend(r.Context(), projectFrom(r).ID, id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "ad spend not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
