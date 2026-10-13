package antifraud

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	Events         prometheus.Counter
	Users          prometheus.Gauge
	Evicted        prometheus.Counter
	FeaturesRows   prometheus.Counter
	WriteErrors    prometheus.Counter
	Runs           *prometheus.CounterVec
	RunSeconds     prometheus.Histogram
	Suspicious     prometheus.Gauge
	FlaggedSources prometheus.Gauge
	Alerts         prometheus.Counter
}

func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		Events: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_antifraud_events_total", Help: "User messages and button presses observed.",
		}),
		Users: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "botsight_antifraud_users", Help: "Users kept in memory by this instance.",
		}),
		Evicted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_antifraud_evicted_total", Help: "Users dropped from memory (idle or over the limit).",
		}),
		FeaturesRows: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_antifraud_features_rows_total", Help: "Rows written to user_features.",
		}),
		WriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_antifraud_write_errors_total", Help: "Failed writes of user_features.",
		}),
		Runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "botsight_antifraud_runs_total", Help: "Scoring runs by result.",
		}, []string{"result"}),
		RunSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "botsight_antifraud_run_seconds", Help: "Duration of a scoring run.",
			Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		}),
		Suspicious: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "botsight_antifraud_suspicious_users", Help: "Suspicious users found by the last run (all projects).",
		}),
		FlaggedSources: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "botsight_antifraud_flagged_sources", Help: "Sources over the alert threshold in the last run.",
		}),
		Alerts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "botsight_antifraud_alerts_total", Help: "Alerts about suspicious sources.",
		}),
	}
	reg.MustRegister(m.Events, m.Users, m.Evicted, m.FeaturesRows, m.WriteErrors, m.Runs, m.RunSeconds,
		m.Suspicious, m.FlaggedSources, m.Alerts)
	return m
}
