package httpapi

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/query"
)

// Metrics of the api (Botsight.md, 17.1). Routes are chi patterns such as
// /v1/projects/{projectID}/overview, and /v1/* for unknown paths: bounded, unlike paths
// with ids or whatever a scanner sends. No project_id label:
// thousands of projects would explode cardinality.
type Metrics struct {
	Requests *prometheus.CounterVec
	Duration *prometheus.HistogramVec
	Queries  *query.Metrics
	gatherer prometheus.Gatherer
}

// NewMetrics registers the api's metrics; live reports the open /live streams (may be nil).
func NewMetrics(reg *prometheus.Registry, live func() int) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_api_requests_total", Help: "API requests by route pattern, method and status code.",
		}, []string{"route", "method", "code"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "API request duration by route pattern; /live streams are excluded.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"route"}),
		Queries:  query.NewMetrics(reg),
		gatherer: reg,
	}
	reg.MustRegister(m.Requests, m.Duration)
	if live != nil {
		reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "botsight_live_streams", Help: "Open /live streams of this instance.",
		}, func() float64 { return float64(live()) }))
	}
	return m
}

// observe is called by logRequests after every request.
func (m *Metrics) observe(r *http.Request, ww chimw.WrapResponseWriter, d time.Duration) {
	if m == nil {
		return
	}
	route := "unmatched"
	if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
		route = rc.RoutePattern()
	}
	m.Requests.WithLabelValues(route, r.Method, strconv.Itoa(ww.Status())).Inc()
	if !strings.HasSuffix(route, "/live") {
		m.Duration.WithLabelValues(route).Observe(d.Seconds())
	}
}

// metrics serves Prometheus; without metrics configured, an empty registry.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	var g prometheus.Gatherer = prometheus.NewRegistry()
	if s.Metrics != nil {
		g = s.Metrics.gatherer
	}
	promhttp.HandlerFor(g, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}
