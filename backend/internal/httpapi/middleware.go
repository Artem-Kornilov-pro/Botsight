package httpapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"mime"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/session"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

const (
	sessionCookie = "bs_session"
	csrfHeader    = "X-CSRF-Token"
)

type ctxKey int

const (
	ctxSession ctxKey = iota
	ctxProject
)

func sessionFrom(r *http.Request) session.Session {
	return r.Context().Value(ctxSession).(session.Session)
}

func projectFrom(r *http.Request) store.Project {
	return r.Context().Value(ctxProject).(store.Project)
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.Logger.Error("panic", "method", r.Method, "path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, codeInternal, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		route := ""
		if rc := chi.RouteContext(r.Context()); rc != nil {
			route = rc.RoutePattern()
		}
		s.Logger.Info("request",
			"method", r.Method, "route", route, "status", ww.Status(),
			"duration_ms", time.Since(start).Milliseconds())
	})
}

// cors lets the dashboard origin call the API with the session cookie.
func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		allowed := origin != "" && slices.Contains(s.Options.CORSOrigins, origin)
		if allowed {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Credentials", "true")
			h.Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			if allowed {
				h := w.Header()
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE")
				h.Set("Access-Control-Allow-Headers", "Content-Type, "+csrfHeader)
				h.Set("Access-Control-Max-Age", "600")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireJSON rejects request bodies that are not JSON. Together with SameSite
// cookies this keeps plain HTML forms from other sites out.
func (s *Server) requireJSON(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hasBody := r.ContentLength > 0 || len(r.TransferEncoding) > 0
		if hasBody && r.Method != http.MethodGet {
			mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || mt != "application/json" {
				writeError(w, http.StatusUnsupportedMediaType, codeUnsupportedMedia, "Content-Type must be application/json")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie(sessionCookie)
		if err != nil || c.Value == "" {
			writeError(w, http.StatusUnauthorized, codeUnauthorized, "sign in required")
			return
		}
		sess, err := s.Sessions.Get(r.Context(), c.Value)
		if errors.Is(err, session.ErrNotFound) {
			s.clearSessionCookie(w)
			writeError(w, http.StatusUnauthorized, codeUnauthorized, "session expired, sign in again")
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxSession, sess)))
	})
}

// checkCSRF requires the session's CSRF token in a header on every state-changing request.
func (s *Server) checkCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			got := r.Header.Get(csrfHeader)
			if got == "" || subtle.ConstantTimeCompare([]byte(got), []byte(sessionFrom(r).CSRFToken)) != 1 {
				writeError(w, http.StatusForbidden, codeCSRF, "missing or invalid "+csrfHeader+" header")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// loadProject resolves {projectID} for the signed-in account. Non-members get 404:
// project_id always comes from the path after a membership check (Botsight.md, 19.3).
func (s *Server) loadProject(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(r, "projectID")
		if !ok {
			writeError(w, http.StatusNotFound, codeNotFound, "project not found")
			return
		}
		p, err := s.Store.Project(r.Context(), id, sessionFrom(r).AccountID)
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, codeNotFound, "project not found")
			return
		}
		if err != nil {
			s.internalError(w, r, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxProject, p)))
	})
}

func (s *Server) requireRole(min string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !store.RoleAtLeast(projectFrom(r).Role, min) {
				writeError(w, http.StatusForbidden, codeForbidden, "requires role "+min)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func pathID(r *http.Request, name string) (int32, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, name), 10, 32)
	return int32(v), err == nil && v > 0
}

func pathID64(r *http.Request, name string) (int64, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, name), 10, 64)
	return v, err == nil && v > 0
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// allow applies a rate limit; when exceeded it writes 429 and returns false.
func (s *Server) allow(w http.ResponseWriter, r *http.Request, rule ratelimit.Rule, key string) bool {
	ok, wait, err := s.Limiter.Allow(r.Context(), rule, strings.ToLower(key))
	if err != nil {
		s.Logger.Warn("rate limiter unavailable, allowing request", "rule", rule.Name, "err", err)
	}
	if ok {
		return true
	}
	w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
	writeError(w, http.StatusTooManyRequests, codeRateLimited, "too many attempts, try again later")
	return false
}

func (s *Server) setSessionCookie(w http.ResponseWriter, token string) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    token,
		Path:     "/",
		MaxAge:   int(s.Options.SessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   s.Options.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookie,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   s.Options.CookieSecure,
		SameSite: http.SameSiteLaxMode,
	})
}
