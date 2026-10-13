package antifraud

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// Scoring (Botsight.md, 15.2): weighted rules, a score from 0 to 1 per user. A user is
// suspicious from SuspiciousScore. Behavioural signals weigh most: a farm can buy
// accounts with usernames, but a script gives itself away by its timing.
const (
	SuspiciousScore = 0.5
	Window          = 24 * time.Hour

	weightFast   = 0.35 // most intervals between messages shorter than FastInterval
	weightSynced = 0.35 // the same timing signature as many other users of the project
	// A ring is flagged only when it is large and mostly scripted or a wide star of accounts
	// without usernames, so membership alone is enough, the inviter of a farm included.
	weightRing        = 0.5  // member of a suspicious referral component
	weightHomogeneous = 0.15 // a source of accounts without usernames, and this is one of them
	weightFresh       = 0.15 // a source of the newest accounts, and this is one of them
	weightSpike       = 0.15 // joined in a burst of /start of its source

	syncedMinUsers    = 10
	sourceMinUsers    = 30
	spikeMinUsers     = 20
	spikeFactor       = 10
	spikeHistoryHours = 6

	alertMinSuspicious = 30
	alertMinShare      = 0.5
)

// Reasons in the order of the score formula, with texts for alerts and the dashboard.
var Reasons = []struct{ Code, Text string }{
	{"fast_steps", "слишком быстрые ответы"},
	{"synced_timing", "одинаковые интервалы между сообщениями у многих аккаунтов"},
	{"referral_ring", "реферальная сеть из подозрительных аккаунтов"},
	{"homogeneous_source", "источник из аккаунтов без username"},
	{"fresh_accounts", "источник из самых новых аккаунтов Telegram"},
	{"start_spike", "всплеск /start по метке"},
}

// scoredSQL computes the score of every user active in the window, for all projects
// at once; params: since, at. Sources and projects are compared using users of all
// antifraud instances, which is why this runs in ClickHouse.
const scoredSQL = `
users AS (
    SELECT project_id, user_id, first_event, intervals, fast_intervals, signature, has_username
    FROM user_features FINAL
    WHERE last_seen >= {since:DateTime64(3, 'UTC')}
),
firsts AS (
    SELECT project_id, user_id, argMinMerge(source) AS source, min(first_start) AS first_start
    FROM user_first_start
    WHERE (project_id, user_id) IN (SELECT project_id, user_id FROM users)
    GROUP BY project_id, user_id
),
u AS (
    SELECT users.project_id AS project_id, users.user_id AS user_id, users.intervals AS intervals,
           users.fast_intervals AS fast_intervals, users.signature AS signature,
           users.has_username AS has_username, firsts.source AS source,
           if(firsts.first_start > toDateTime64(0, 3), least(firsts.first_start, users.first_event), users.first_event) AS joined
    FROM users LEFT JOIN firsts ON users.project_id = firsts.project_id AND users.user_id = firsts.user_id
),
fresh AS (SELECT quantileExact(0.95)(user_id) AS fresh_id FROM u),
proj AS (
    SELECT project_id, avg(has_username = 0) AS no_username, avg(user_id >= (SELECT fresh_id FROM fresh)) AS fresh_share
    FROM u GROUP BY project_id
),
src AS (
    SELECT project_id, source, count() AS users, avg(has_username = 0) AS no_username,
           avg(user_id >= (SELECT fresh_id FROM fresh)) AS fresh_share, min(joined) AS first_joined
    FROM u GROUP BY project_id, source
),
synced AS (
    SELECT project_id, signature FROM u WHERE signature != ''
    GROUP BY project_id, signature HAVING count() >= {synced_min:UInt32}
),
minutes AS (
    SELECT project_id, source, toStartOfMinute(joined) AS m, count() AS c
    FROM u WHERE joined >= {since:DateTime64(3, 'UTC')} GROUP BY project_id, source, m
),
spikes AS (
    SELECT minutes.project_id AS project_id, minutes.source AS source, minutes.m AS m
    FROM minutes INNER JOIN src ON minutes.project_id = src.project_id AND minutes.source = src.source
    WHERE src.first_joined <= {at:DateTime64(3, 'UTC')} - toIntervalHour({spike_history:UInt32})
      AND minutes.c >= greatest({spike_min:UInt32}, {spike_factor:UInt32} * src.users / 1440)
),
scored AS (
    SELECT u.project_id AS project_id, u.user_id AS user_id, u.source AS source, u.joined AS joined,
           u.intervals >= 4 AND u.fast_intervals >= 0.6 * u.intervals AS r_fast,
           (u.project_id, u.signature) IN (SELECT project_id, signature FROM synced) AS r_synced,
           (u.project_id, u.user_id) IN (SELECT project_id, user_id FROM referral_flags FINAL
                                         WHERE updated_at >= {since:DateTime64(3, 'UTC')}) AS r_ring,
           src.users >= {source_min:UInt32} AND src.no_username >= 0.7 AND src.no_username >= 2 * proj.no_username
               AND u.has_username = 0 AS r_homogeneous,
           src.users >= {source_min:UInt32} AND src.fresh_share >= greatest(0.4, 3 * proj.fresh_share)
               AND u.user_id >= (SELECT fresh_id FROM fresh) AS r_fresh,
           (u.project_id, u.source, toStartOfMinute(u.joined)) IN (SELECT project_id, source, m FROM spikes) AS r_spike,
           least(1.0, {w_fast:Float64} * r_fast + {w_synced:Float64} * r_synced + {w_ring:Float64} * r_ring
                      + {w_homogeneous:Float64} * r_homogeneous + {w_fresh:Float64} * r_fresh
                      + {w_spike:Float64} * r_spike) AS score
    FROM u
    INNER JOIN src ON u.project_id = src.project_id AND u.source = src.source
    INNER JOIN proj ON u.project_id = proj.project_id
)`

// scoreParams are server-side query parameters ({name:Type} placeholders), passed as strings.
func scoreParams(since, at time.Time) clickhouse.Parameters {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	u := func(v int) string { return strconv.Itoa(v) }
	return clickhouse.Parameters{
		"since": chTime(since), "at": chTime(at),
		"synced_min": u(syncedMinUsers), "source_min": u(sourceMinUsers),
		"spike_min": u(spikeMinUsers), "spike_factor": u(spikeFactor), "spike_history": u(spikeHistoryHours),
		"w_fast": f(weightFast), "w_synced": f(weightSynced), "w_ring": f(weightRing),
		"w_homogeneous": f(weightHomogeneous), "w_fresh": f(weightFresh), "w_spike": f(weightSpike),
		"suspicious": f(SuspiciousScore),
	}
}

func chTime(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000") }

// reasonsExpr builds the reasons array in the order of Reasons.
const reasonsExpr = `arrayFilter(x -> x != '', [if(r_fast, 'fast_steps', ''), if(r_synced, 'synced_timing', ''),
    if(r_ring, 'referral_ring', ''), if(r_homogeneous, 'homogeneous_source', ''),
    if(r_fresh, 'fresh_accounts', ''), if(r_spike, 'start_spike', '')])`

const reasonsCountExpr = `mapFilter((k, v) -> v > 0, map(
    'fast_steps', toUInt32(countIf(r_fast AND score >= {suspicious:Float64})),
    'synced_timing', toUInt32(countIf(r_synced AND score >= {suspicious:Float64})),
    'referral_ring', toUInt32(countIf(r_ring AND score >= {suspicious:Float64})),
    'homogeneous_source', toUInt32(countIf(r_homogeneous AND score >= {suspicious:Float64})),
    'fresh_accounts', toUInt32(countIf(r_fresh AND score >= {suspicious:Float64})),
    'start_spike', toUInt32(countIf(r_spike AND score >= {suspicious:Float64}))))`

// AlertSink sends alerts to the alerts topic.
type AlertSink interface {
	Send(ctx context.Context, a event.Alert) error
}

type KafkaAlerts struct{ Client *kgo.Client }

func (k KafkaAlerts) Send(ctx context.Context, a event.Alert) error {
	v, err := json.Marshal(a)
	if err != nil {
		return err
	}
	return k.Client.ProduceSync(ctx, &kgo.Record{
		Topic: kafka.TopicAlerts, Key: []byte(strconv.FormatUint(uint64(a.ProjectID), 10)), Value: v,
	}).FirstErr()
}

type Analyzer struct {
	ch      driver.Conn
	alerts  AlertSink
	log     *slog.Logger
	metrics *Metrics
	// GraphEvery: the referral graph is rebuilt on every n-th run (it scans 30 days).
	GraphEvery int
	runs       int
}

func NewAnalyzer(ch driver.Conn, alerts AlertSink, log *slog.Logger, m *Metrics) *Analyzer {
	return &Analyzer{ch: ch, alerts: alerts, log: log, metrics: m, GraphEvery: 5}
}

// RunResult is a summary of one run.
type RunResult struct {
	At         time.Time
	Suspicious uint64
	Rings      int
	Alerts     []event.Alert
}

// Run scores users active in the last 24 hours as of at.
func (a *Analyzer) Run(ctx context.Context, at time.Time) (RunResult, error) {
	started := time.Now()
	// Seconds: rows of one run are found by updated_at = at, and positional query
	// arguments carry time.Time with second precision.
	at = at.UTC().Truncate(time.Second)
	res := RunResult{At: at}
	var err error
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		a.metrics.Runs.WithLabelValues(result).Inc()
		a.metrics.RunSeconds.Observe(time.Since(started).Seconds())
	}()

	if a.GraphEvery <= 1 || a.runs%a.GraphEvery == 0 {
		if res.Rings, err = a.referralGraph(ctx, at); err != nil {
			return res, fmt.Errorf("referral graph: %w", err)
		}
	}
	a.runs++
	since := at.Add(-Window)
	qctx := clickhouse.Context(ctx, clickhouse.WithParameters(scoreParams(since, at)))

	if err = a.ch.Exec(qctx, `INSERT INTO user_risk (project_id, user_id, source, joined, score, reasons, updated_at)
		WITH `+scoredSQL+`
		SELECT project_id, user_id, source, joined, toFloat32(score), `+reasonsExpr+`, {at:DateTime64(3, 'UTC')}
		FROM scored WHERE score >= {suspicious:Float64}`); err != nil {
		return res, fmt.Errorf("user_risk: %w", err)
	}
	if err = a.ch.Exec(qctx, `INSERT INTO source_risk (project_id, source, users, suspicious, avg_score, reasons, updated_at)
		WITH `+scoredSQL+`
		SELECT project_id, source, toUInt32(count()), toUInt32(countIf(score >= {suspicious:Float64})),
		       toFloat32(avg(score)), `+reasonsCountExpr+`, {at:DateTime64(3, 'UTC')}
		FROM scored GROUP BY project_id, source`); err != nil {
		return res, fmt.Errorf("source_risk: %w", err)
	}
	if err = a.ch.QueryRow(ctx, `SELECT toUInt64(sum(suspicious)) FROM source_risk WHERE updated_at = ?`, at).
		Scan(&res.Suspicious); err != nil {
		return res, err
	}
	a.metrics.Suspicious.Set(float64(res.Suspicious))
	res.Alerts, err = a.raiseAlerts(ctx, at)
	return res, err
}

// referralGraph rebuilds the referral components of projects with ref_ labels in the
// last day and stores members of rings in referral_flags. Returns the number of flagged users.
func (a *Analyzer) referralGraph(ctx context.Context, at time.Time) (int, error) {
	since := at.Add(-Window)
	rows, err := a.ch.Query(ctx, `
		SELECT project_id, user_id, toUInt64OrZero(substring(source, 5)) AS inviter FROM (
		    SELECT project_id, user_id, argMinMerge(source) AS source FROM user_first_start
		    WHERE project_id IN (SELECT DISTINCT project_id FROM events
		                         WHERE ts >= ? AND type = 'update' AND command = 'start' AND startsWith(start_param, 'ref_'))
		    GROUP BY project_id, user_id
		) WHERE startsWith(source, 'ref_') AND inviter > 0 AND inviter != user_id
		LIMIT 5000000`, since)
	if err != nil {
		return 0, err
	}
	edges := map[uint32][]edge{}
	for rows.Next() {
		var pid uint32
		var e edge
		if err := rows.Scan(&pid, &e.invitee, &e.inviter); err != nil {
			rows.Close()
			return 0, err
		}
		edges[pid] = append(edges[pid], e)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(edges) == 0 {
		return 0, err
	}

	projects := make([]uint32, 0, len(edges))
	for pid := range edges {
		projects = append(projects, pid)
	}
	members := map[uint32]map[uint64]member{}
	mrows, err := a.ch.Query(ctx, `
		SELECT project_id, user_id, intervals >= 4 AND fast_intervals >= 0.6 * intervals AS fast, has_username
		FROM user_features FINAL WHERE project_id IN ? AND last_seen >= ?`, projects, at.Add(-30*24*time.Hour))
	if err != nil {
		return 0, err
	}
	for mrows.Next() {
		var pid uint32
		var uid uint64
		var fast bool
		var hasUsername uint8
		if err := mrows.Scan(&pid, &uid, &fast, &hasUsername); err != nil {
			mrows.Close()
			return 0, err
		}
		if members[pid] == nil {
			members[pid] = map[uint64]member{}
		}
		members[pid][uid] = member{suspicious: fast, hasUsername: hasUsername == 1, known: true}
	}
	mrows.Close()

	batch, err := a.ch.PrepareBatch(ctx, `INSERT INTO referral_flags (project_id, user_id, component_size, updated_at)`)
	if err != nil {
		return 0, err
	}
	flagged := 0
	for pid, es := range edges {
		for uid, size := range rings(es, members[pid]) {
			if err := batch.Append(pid, uid, uint32(size), at); err != nil {
				_ = batch.Abort()
				return 0, err
			}
			flagged++
		}
	}
	if flagged == 0 {
		_ = batch.Abort()
		return 0, nil
	}
	return flagged, batch.Send()
}

// raiseAlerts reports sources where many users look like a farm; one alert per source a day.
func (a *Analyzer) raiseAlerts(ctx context.Context, at time.Time) ([]event.Alert, error) {
	rows, err := a.ch.Query(ctx, `SELECT project_id, source, users, suspicious, reasons FROM source_risk
		WHERE updated_at = ? AND suspicious >= ? AND suspicious >= ? * users`, at, uint32(alertMinSuspicious), alertMinShare)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var alerts []event.Alert
	for rows.Next() {
		var pid, users, suspicious uint32
		var source string
		var reasons map[string]uint32
		if err := rows.Scan(&pid, &source, &users, &suspicious, &reasons); err != nil {
			return nil, err
		}
		alerts = append(alerts, sourceAlert(pid, source, users, suspicious, reasons, at))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	a.metrics.FlaggedSources.Set(float64(len(alerts)))
	for _, al := range alerts {
		if err := a.alerts.Send(ctx, al); err != nil {
			return alerts, fmt.Errorf("send alert: %w", err)
		}
		a.metrics.Alerts.Inc()
	}
	return alerts, nil
}

var alertNamespace = uuid.MustParse("5b3c2f1e-9a8d-4c7b-8e6f-1d2c3b4a5f60")

func sourceAlert(pid uint32, source string, users, suspicious uint32, reasons map[string]uint32, at time.Time) event.Alert {
	label := source
	if label == "" {
		label = "без метки"
	}
	share := float64(suspicious) / float64(users)
	var parts []string
	type rc struct {
		text  string
		count uint32
	}
	var top []rc
	for _, r := range Reasons {
		if n := reasons[r.Code]; n > 0 {
			top = append(top, rc{r.Text, n})
		}
	}
	sort.SliceStable(top, func(i, j int) bool { return top[i].count > top[j].count })
	for _, r := range top[:min(3, len(top))] {
		parts = append(parts, fmt.Sprintf("%s (%.0f%%)", r.text, 100*float64(r.count)/float64(suspicious)))
	}
	details, _ := json.Marshal(map[string]any{
		"source": source, "users": users, "suspicious": suspicious, "share": share, "reasons": reasons,
	})
	day := at.Format("2006-01-02")
	return event.Alert{
		ProjectID: pid,
		AlertID:   uuid.NewSHA1(alertNamespace, fmt.Appendf(nil, "%d:fraud:%s:%s", pid, source, day)).String(),
		Kind:      "fraud",
		CreatedAt: at.UnixMilli(),
		Title:     fmt.Sprintf("Источник «%s»: %.0f%% пользователей похожи на ботоферму", label, 100*share),
		// "пользователей: N" avoids Russian plural forms of numerals
		Text: fmt.Sprintf("Источник «%s» за сутки — пользователей: %d, из них подозрительных: %d. Признаки: %s.",
			label, users, suspicious, strings.Join(parts, "; ")),
		Details: string(details),
	}
}

// Scheduler runs the analysis once a minute on one instance (a Redis lock per minute).
type Scheduler struct {
	Analyzer *Analyzer
	Redis    redis.UniversalClient
	Every    time.Duration
	Log      *slog.Logger
}

func (s *Scheduler) Loop(ctx context.Context) {
	t := time.NewTicker(s.Every)
	defer t.Stop()
	for {
		now := time.Now().UTC()
		key := "antifraud:run:" + now.Truncate(s.Every).Format(time.RFC3339)
		if ok, err := s.Redis.SetNX(ctx, key, 1, 10*s.Every).Result(); err == nil && ok {
			res, err := s.Analyzer.Run(ctx, now)
			if err != nil {
				s.Log.Error("antifraud run failed", "err", err)
			} else {
				s.Log.Info("antifraud run", "suspicious", res.Suspicious, "ring_members", res.Rings, "alerts", len(res.Alerts))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
