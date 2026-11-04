package chwriter

import "github.com/prometheus/client_golang/prometheus"

// Metrics of ch-writer (Botsight.md, 17.1).
type Metrics struct {
	Rows           *prometheus.CounterVec
	InsertDuration *prometheus.HistogramVec
	InsertErrors   *prometheus.CounterVec
	DLQ            *prometheus.CounterVec
	Lag            *prometheus.GaugeVec
	Paused         prometheus.Gauge
	Freshness      *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Rows: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_ch_insert_rows_total", Help: "Rows inserted into ClickHouse.",
		}, []string{"table"}),
		InsertDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "botsight_ch_insert_duration_seconds", Help: "Duration of one batch insert.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"table"}),
		InsertErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_ch_insert_errors_total", Help: "Failed insert attempts (retried).",
		}, []string{"table"}),
		DLQ: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_dlq_total", Help: "Records sent to the dead letter queue.",
		}, []string{"topic"}),
		Lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "botsight_consumer_lag", Help: "Records not yet committed by the ch-writer group.",
		}, []string{"topic", "partition"}),
		Paused: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "botsight_ch_paused_partitions", Help: "Partitions paused by backpressure.",
		}),
		// SLO "freshness" (Botsight.md, 17.2): from the collector's write to Kafka until
		// the row can be queried. One observation per chunk, its oldest record: the
		// worst case of the chunk, cheap at any event rate.
		Freshness: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "botsight_ch_freshness_seconds",
			Help:    "Age of the oldest record of a chunk when its insert into ClickHouse finished.",
			Buckets: []float64{.25, .5, .75, 1, 1.25, 1.5, 2, 3, 5, 7.5, 10, 15, 30, 60, 300},
		}, []string{"table"}),
	}
	reg.MustRegister(m.Rows, m.InsertDuration, m.InsertErrors, m.DLQ, m.Lag, m.Paused, m.Freshness)
	return m
}
