package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/auth"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

func (s *Server) listProjects(w http.ResponseWriter, r *http.Request) {
	projects, err := s.Store.Projects(r.Context(), sessionFrom(r).AccountID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]projectView, len(projects))
	for i, p := range projects {
		out[i] = viewProject(p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"projects": out})
}

// createProject also creates the first API key: the dashboard shows it once (Botsight.md, 16.2).
func (s *Server) createProject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if !decode(w, r, &req) {
		return
	}
	name, ferr := cleanName("name", req.Name, true)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	var project store.Project
	var apiKey store.APIKey
	plain, err := withNewKey(func(key store.NewKey) error {
		var err error
		project, apiKey, err = s.Store.CreateProject(r.Context(), name, sessionFrom(r).AccountID, key)
		return err
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	kv := viewKey(apiKey)
	kv.Key = plain
	writeJSON(w, http.StatusCreated, map[string]any{"project": viewProject(project), "api_key": kv})
}

// withNewKey generates a key and retries on the (unlikely) prefix collision.
func withNewKey(insert func(store.NewKey) error) (string, error) {
	for attempt := 0; ; attempt++ {
		key, err := auth.NewAPIKey()
		if err != nil {
			return "", err
		}
		err = insert(store.NewKey{Prefix: key.Prefix, Hash: key.Hash})
		if errors.Is(err, store.ErrConflict) && attempt < 3 {
			continue
		}
		return key.Plain, err
	}
}

func (s *Server) getProject(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, viewProject(projectFrom(r)))
}

func (s *Server) updateProject(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     *string                `json:"name"`
		Settings *store.ProjectSettings `json:"settings"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.Name == nil && req.Settings == nil {
		writeInvalid(w, invalid("name", "nothing to update: send name and/or settings"))
		return
	}
	if req.Name != nil {
		name, ferr := cleanName("name", *req.Name, true)
		if ferr != nil {
			writeInvalid(w, ferr)
			return
		}
		req.Name = &name
	}
	if req.Settings != nil {
		if ferr := validateSettings(req.Settings); ferr != nil {
			writeInvalid(w, ferr)
			return
		}
	}
	p := projectFrom(r)
	updated, err := s.Store.UpdateProject(r.Context(), p.ID, sessionFrom(r).AccountID, req.Name, req.Settings)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, viewProject(updated))
}

func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	prefixes, err := s.Store.DeleteProject(r.Context(), projectFrom(r).ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		s.internalError(w, r, err)
		return
	}
	s.publishRevoked(r.Context(), prefixes...)
	w.WriteHeader(http.StatusNoContent)
}

// publishRevoked tells collectors to drop keys from their caches (Botsight.md, 10.2).
// If Redis is down the collector cache expires on its own within its TTL.
func (s *Server) publishRevoked(ctx context.Context, prefixes ...string) {
	for _, p := range prefixes {
		if err := s.Redis.Publish(ctx, auth.KeyRevocationChannel, p).Err(); err != nil {
			s.Logger.Warn("failed to publish key revocation", "prefix", p, "err", err)
		}
	}
}

// --- members ---

func (s *Server) listMembers(w http.ResponseWriter, r *http.Request) {
	members, err := s.Store.Members(r.Context(), projectFrom(r).ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]memberView, len(members))
	for i, m := range members {
		out[i] = viewMember(m)
	}
	writeJSON(w, http.StatusOK, map[string]any{"members": out})
}

func (s *Server) memberView(ctx context.Context, projectID int32, accountID int64) (memberView, error) {
	members, err := s.Store.Members(ctx, projectID)
	if err != nil {
		return memberView{}, err
	}
	for _, m := range members {
		if m.AccountID == accountID {
			return viewMember(m), nil
		}
	}
	return memberView{}, store.ErrNotFound
}

// addMember adds an existing account by email. Invitations for people without
// an account are out of scope for now.
func (s *Server) addMember(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	if !decode(w, r, &req) {
		return
	}
	email, ferr := normalizeEmail(req.Email)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	if !validRole(req.Role) {
		writeInvalid(w, invalid("role", "role must be owner, editor or viewer"))
		return
	}
	account, err := s.Store.AccountByEmail(r.Context(), email)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeAccountNotFound, "no Botsight account with this email; ask them to sign up first")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	projectID := projectFrom(r).ID
	err = s.Store.AddMember(r.Context(), projectID, account.ID, req.Role)
	if errors.Is(err, store.ErrConflict) {
		writeError(w, http.StatusConflict, codeAlreadyMember, "this account is already a member")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	view, err := s.memberView(r.Context(), projectID, account.ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, view)
}

func (s *Server) updateMember(w http.ResponseWriter, r *http.Request) {
	accountID, ok := pathID64(r, "accountID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "member not found")
		return
	}
	var req struct {
		Role string `json:"role"`
	}
	if !decode(w, r, &req) {
		return
	}
	if !validRole(req.Role) {
		writeInvalid(w, invalid("role", "role must be owner, editor or viewer"))
		return
	}
	projectID := projectFrom(r).ID
	if !s.memberChangeOK(w, r, s.Store.SetMemberRole(r.Context(), projectID, accountID, req.Role)) {
		return
	}
	view, err := s.memberView(r.Context(), projectID, accountID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// removeMember: owners remove anyone; any member may leave the project.
func (s *Server) removeMember(w http.ResponseWriter, r *http.Request) {
	accountID, ok := pathID64(r, "accountID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "member not found")
		return
	}
	p := projectFrom(r)
	if accountID != sessionFrom(r).AccountID && !store.RoleAtLeast(p.Role, store.RoleOwner) {
		writeError(w, http.StatusForbidden, codeForbidden, "requires role owner")
		return
	}
	if !s.memberChangeOK(w, r, s.Store.RemoveMember(r.Context(), p.ID, accountID)) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) memberChangeOK(w http.ResponseWriter, r *http.Request, err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, codeNotFound, "member not found")
	case errors.Is(err, store.ErrLastOwner):
		writeError(w, http.StatusConflict, codeLastOwner, "a project must keep at least one owner")
	default:
		s.internalError(w, r, err)
	}
	return false
}

// --- API keys ---

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	keys, err := s.Store.Keys(r.Context(), projectFrom(r).ID)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	out := make([]keyView, len(keys))
	for i, k := range keys {
		out[i] = viewKey(k)
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": out})
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	if !decodeOptional(w, r, &req) {
		return
	}
	name, ferr := cleanName("name", req.Name, false)
	if ferr != nil {
		writeInvalid(w, ferr)
		return
	}
	var apiKey store.APIKey
	plain, err := withNewKey(func(key store.NewKey) error {
		key.Name = name
		var err error
		apiKey, err = s.Store.CreateKey(r.Context(), projectFrom(r).ID, key, sessionFrom(r).AccountID)
		return err
	})
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	kv := viewKey(apiKey)
	kv.Key = plain
	writeJSON(w, http.StatusCreated, kv)
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	keyID, ok := pathID(r, "keyID")
	if !ok {
		writeError(w, http.StatusNotFound, codeNotFound, "key not found")
		return
	}
	prefix, newly, err := s.Store.RevokeKey(r.Context(), projectFrom(r).ID, keyID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeNotFound, "key not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if newly {
		s.publishRevoked(r.Context(), prefix)
	}
	w.WriteHeader(http.StatusNoContent)
}
