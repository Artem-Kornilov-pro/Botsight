package botproxy

import "github.com/prometheus/client_golang/prometheus"

// Metrics of the proxy (Botsight.md, 17.1). No bot or project labels: thousands of bots
// would explode cardinality. A nil *Metrics records nothing.
type Metrics struct {
	Requests   *prometheus.CounterVec
	Upstream   *prometheus.HistogramVec
	Events     *prometheus.CounterVec
	Batches    *prometheus.CounterVec
	Duplicates prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer, bots func() int) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_proxy_requests_total",
			Help: "Requests of bots by kind (polling, api, file, webhook) and the status answered to the bot.",
		}, []string{"kind", "code"}),
		Upstream: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "botsight_proxy_upstream_seconds",
			Help:    "Time of a request to Telegram or to the bot's webhook; long polling waits up to its timeout.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"kind"}),
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_proxy_events_total", Help: "Events queued for the collector or dropped on a full queue.",
		}, []string{"result"}),
		Batches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_proxy_batches_total", Help: "Batches sent to the collector: accepted, rejected or failed after retries.",
		}, []string{"result"}),
		Duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_proxy_duplicate_updates_total", Help: "Updates seen again after a bot restart or a webhook retry.",
		}),
	}
	reg.MustRegister(m.Requests, m.Upstream, m.Events, m.Batches, m.Duplicates,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "botsight_proxy_bots", Help: "Bots with a deduplication window in memory.",
		}, func() float64 { return float64(bots()) }))
	return m
}

func (m *Metrics) event(result string) {
	if m != nil {
		m.Events.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) batch(result string) {
	if m != nil {
		m.Batches.WithLabelValues(result).Inc()
	}
}

func (m *Metrics) duplicate() {
	if m != nil {
		m.Duplicates.Inc()
	}
}

func (m *Metrics) request(kind string, code int, seconds float64) {
	if m != nil {
		m.Requests.WithLabelValues(kind, codeClass(code)).Inc()
		m.Upstream.WithLabelValues(kind).Observe(seconds)
	}
}

func codeClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 200:
		return "2xx"
	}
	return "other"
}
