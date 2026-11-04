package query

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics of the analytics queries: the SLO "dashboard p95 < 1 s" (Botsight.md, 17.2) is
// read from Duration, the value of the cache from Cache. A nil *Metrics records nothing.
type Metrics struct {
	Duration *prometheus.HistogramVec
	Cache    *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "botsight_query_duration_seconds",
			Help:    "Time of an analytics query in ClickHouse by kind (overview, funnel, ...), cache hits excluded.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 20},
		}, []string{"kind"}),
		Cache: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_query_cache_total", Help: "Cached analytics answers by kind: hit or miss.",
		}, []string{"kind", "result"}),
	}
	reg.MustRegister(m.Duration, m.Cache)
	return m
}

func (m *Metrics) took(kind string, start time.Time) {
	if m != nil {
		m.Duration.WithLabelValues(kind).Observe(time.Since(start).Seconds())
	}
}

func (m *Metrics) cache(kind string, hit bool) {
	if m != nil {
		result := "miss"
		if hit {
			result = "hit"
		}
		m.Cache.WithLabelValues(kind, result).Inc()
	}
}
