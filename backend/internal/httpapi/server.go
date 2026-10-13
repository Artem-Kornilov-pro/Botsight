// Package httpapi is the REST API of the dashboard control plane (Botsight.md, 13.3):
// accounts and sign-in, projects, members, API keys, saved funnels and ad spend.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/redis/go-redis/v9"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/mail"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ratelimit"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/session"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/store"
)

type Options struct {
	DashboardURL  string   // base of links in emails
	CORSOrigins   []string // origins allowed to call the API with cookies
	CookieSecure  bool
	SessionTTL    time.Duration
	TrustProxy    bool   // client IP from X-Forwarded-For / X-Real-IP
	TelegramToken string // empty disables Telegram sign-in
	AIURL         string // ai-worker base URL; empty disables natural language questions
	AIToken       string // shared with ai-worker
	Limits        Limits
}

// Limits are the attempt limits for unauthenticated endpoints (Botsight.md, 19.5).
type Limits struct {
	LoginIP       ratelimit.Rule
	LoginAccount  ratelimit.Rule
	RegisterIP    ratelimit.Rule
	TelegramIP    ratelimit.Rule
	ResetIP       ratelimit.Rule
	ResetEmail    ratelimit.Rule
	VerifyAccount ratelimit.Rule
	AskAccount    ratelimit.Rule // questions to the LLM cost money
}

func DefaultLimits() Limits {
	return Limits{
		LoginIP:       ratelimit.Rule{Name: "login:ip", Limit: 20, Period: 15 * time.Minute},
		LoginAccount:  ratelimit.Rule{Name: "login:account", Limit: 10, Period: 15 * time.Minute},
		RegisterIP:    ratelimit.Rule{Name: "register:ip", Limit: 10, Period: time.Hour},
		TelegramIP:    ratelimit.Rule{Name: "telegram:ip", Limit: 30, Period: 15 * time.Minute},
		ResetIP:       ratelimit.Rule{Name: "reset:ip", Limit: 10, Period: time.Hour},
		ResetEmail:    ratelimit.Rule{Name: "reset:email", Limit: 3, Period: time.Hour},
		VerifyAccount: ratelimit.Rule{Name: "verify:account", Limit: 3, Period: time.Hour},
		AskAccount:    ratelimit.Rule{Name: "ask:account", Limit: 30, Period: time.Hour},
	}
}

type Deps struct {
	Store    *store.Store
	Sessions *session.Store
	Limiter  *ratelimit.Window
	Redis    redis.UniversalClient // key revocation Pub/Sub and readiness
	Mailer   mail.Mailer
	Logger   *slog.Logger
	// ClickHouse holds AI topics and alerts; nil turns those endpoints off (503).
	ClickHouse driver.Conn
	// HTTPClient calls ai-worker; nil means a client with a 60 s timeout.
	HTTPClient *http.Client
	Options    Options
}

type Server struct {
	Deps
	ready atomic.Bool
	jobs  sync.WaitGroup
}

func New(d Deps) *Server {
	if d.HTTPClient == nil {
		d.HTTPClient = &http.Client{Timeout: 60 * time.Second}
	}
	s := &Server{Deps: d}
	s.ready.Store(true)
	return s
}

// SetReady switches /readyz; set false on shutdown so the balancer stops sending traffic.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// Wait blocks until background jobs (emails) finish.
func (s *Server) Wait() { s.jobs.Wait() }

// background runs work that must not delay or leak through the response,
// e.g. sending an email after a password reset request.
func (s *Server) background(name string, fn func(ctx context.Context) error) {
	s.jobs.Add(1)
	go func() {
		defer s.jobs.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := fn(ctx); err != nil {
			s.Logger.Error("background job failed", "job", name, "err", err)
		}
	}()
}

func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Use(s.recoverer)
	if s.Options.TrustProxy {
		r.Use(chimw.RealIP)
	}
	r.Use(s.logRequests, s.cors)
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, codeNotFound, "not found")
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, codeMethodNotAllowed, "method not allowed")
	})

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)

	r.Route("/v1", func(r chi.Router) {
		r.Use(s.requireJSON)

		r.Post("/auth/register", s.register)
		r.Post("/auth/login", s.login)
		r.Post("/auth/telegram", s.telegramLogin)
		r.Post("/auth/password-reset", s.requestPasswordReset)
		r.Post("/auth/password-reset/confirm", s.confirmPasswordReset)
		r.Post("/auth/verify-email", s.verifyEmail)

		r.Group(func(r chi.Router) {
			r.Use(s.authenticate, s.checkCSRF)

			r.Post("/auth/logout", s.logout)
			r.Get("/account", s.getAccount)
			r.Patch("/account", s.updateAccount)
			r.Post("/account/password", s.changePassword)
			r.Post("/account/verify-email", s.resendVerification)
			r.Post("/account/telegram", s.linkTelegram)
			r.Delete("/account/sessions", s.logoutOtherSessions)

			r.Get("/projects", s.listProjects)
			r.Post("/projects", s.createProject)
			r.Route("/projects/{projectID}", func(r chi.Router) {
				r.Use(s.loadProject)
				editor := s.requireRole(store.RoleEditor)
				owner := s.requireRole(store.RoleOwner)

				r.Get("/", s.getProject)
				r.With(editor).Patch("/", s.updateProject)
				r.With(owner).Delete("/", s.deleteProject)

				r.Get("/members", s.listMembers)
				r.With(owner).Post("/members", s.addMember)
				r.With(owner).Patch("/members/{accountID}", s.updateMember)
				r.Delete("/members/{accountID}", s.removeMember) // owner, or a member leaving

				r.Get("/keys", s.listKeys)
				r.With(owner).Post("/keys", s.createKey)
				r.With(owner).Delete("/keys/{keyID}", s.revokeKey)

				r.Get("/funnels", s.listFunnels)
				r.With(editor).Post("/funnels", s.createFunnel)
				r.Get("/funnels/{funnelID}", s.getFunnel)
				r.With(editor).Put("/funnels/{funnelID}", s.updateFunnel)
				r.With(editor).Delete("/funnels/{funnelID}", s.deleteFunnel)

				r.Get("/ad-spend", s.listAdSpend)
				r.With(editor).Post("/ad-spend", s.createAdSpend)
				r.With(editor).Delete("/ad-spend/{spendID}", s.deleteAdSpend)

				r.Get("/topics", s.listTopics)
				r.Get("/topics/{topicID}/examples", s.topicExamples)
				r.Get("/alerts", s.listAlerts)
				r.Post("/ask", s.ask)

				r.Get("/antifraud", s.antifraudSummary)
				r.Get("/antifraud/users", s.antifraudUsers)
			})
		})
	})
	return r
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeError(w, http.StatusServiceUnavailable, codeUnavailable, "shutting down")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, codeUnavailable, "postgres is unavailable")
		return
	}
	if err := s.Redis.Ping(ctx).Err(); err != nil {
		writeError(w, http.StatusServiceUnavailable, codeUnavailable, "redis is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// internalError logs an unexpected error and answers 500 without details.
func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.Logger.Error("request failed", "method", r.Method, "path", r.URL.Path, "err", err)
	writeError(w, http.StatusInternalServerError, codeInternal, "internal error")
}
