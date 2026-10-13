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
	}
	reg.MustRegister(m.Rows, m.InsertDuration, m.InsertErrors, m.DLQ, m.Lag, m.Paused)
	return m
}
