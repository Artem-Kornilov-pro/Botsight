package ingest

import "github.com/prometheus/client_golang/prometheus"

// Metrics of the collector (Botsight.md, 17.1). No project_id labels: thousands of
// projects would explode cardinality; per-project usage is counted in ClickHouse.
type Metrics struct {
	Requests        *prometheus.CounterVec
	Events          *prometheus.CounterVec
	Duration        prometheus.Histogram
	ProduceErrors   prometheus.Counter
	RateLimited     prometheus.Counter
	Duplicates      prometheus.Counter
	Overloaded      prometheus.Counter
	KeyCacheResults *prometheus.CounterVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_ingest_requests_total", Help: "Batch requests by HTTP status code.",
		}, []string{"code"}),
		Events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_ingest_events_total", Help: "Events in accepted batches: accepted or rejected by validation.",
		}, []string{"result"}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "botsight_ingest_duration_seconds",
			Help:    "Time to handle a batch request, including the Kafka acknowledgement.",
			Buckets: []float64{.002, .005, .01, .02, .03, .05, .075, .1, .2, .5, 1, 2.5},
		}),
		ProduceErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_kafka_produce_errors_total", Help: "Batches that failed to be written to Kafka.",
		}),
		RateLimited: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_ratelimited_total", Help: "Batches rejected by the per-project rate limit.",
		}),
		Duplicates: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_batch_duplicates_total", Help: "Batches already accepted before (SDK retries).",
		}),
		Overloaded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_ingest_overloaded_total", Help: "Requests rejected because the instance had too many in flight.",
		}),
		KeyCacheResults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_key_cache_total", Help: "API key lookups: hit, miss (loaded from PostgreSQL) or error.",
		}, []string{"result"}),
	}
	reg.MustRegister(m.Requests, m.Events, m.Duration, m.ProduceErrors, m.RateLimited, m.Duplicates, m.Overloaded, m.KeyCacheResults)
	return m
}
