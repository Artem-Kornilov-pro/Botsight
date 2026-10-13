package antifraud

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

type StreamConfig struct {
	Brokers []string
	Group   string
	Topic   string // events.raw by default; tests use their own topics
	// Replay: on assignment a partition is read from now-Replay, so the in-memory state is
	// rebuilt after a restart or a rebalance without storing it anywhere (Botsight.md, 15.2).
	Replay        time.Duration
	FlushInterval time.Duration // how often changed users go to user_features
	IdleTTL       time.Duration // users silent longer than this (by event time) are dropped
	MaxUsers      int           // per instance; above it idle users are dropped first
}

func (c *StreamConfig) defaults() {
	if c.Group == "" {
		c.Group = "antifraud"
	}
	if c.Topic == "" {
		c.Topic = kafka.TopicEvents
	}
	if c.Replay <= 0 {
		c.Replay = 24 * time.Hour
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 10 * time.Second
	}
	if c.IdleTTL <= 0 {
		c.IdleTTL = 24 * time.Hour
	}
	if c.MaxUsers <= 0 {
		c.MaxUsers = 5_000_000
	}
}

type partitionState struct {
	users     map[userKey]*userState
	watermark int64 // max event time seen, unix ms
}

// Stream keeps per-user behaviour of the partitions this instance owns.
type Stream struct {
	cfg     StreamConfig
	cl      *kgo.Client
	ch      driver.Conn
	log     *slog.Logger
	metrics *Metrics

	mu    sync.Mutex
	parts map[int32]*partitionState
	out   chan []Features
}

func NewStream(cfg StreamConfig, ch driver.Conn, log *slog.Logger, m *Metrics) (*Stream, error) {
	cfg.defaults()
	s := &Stream{cfg: cfg, ch: ch, log: log, metrics: m, parts: map[int32]*partitionState{}, out: make(chan []Features, 16)}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(), // offsets are never committed: state comes from the replay window
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxBytes(64<<20),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.revoked),
		kgo.OnPartitionsLost(s.revoked),
		kgo.AdjustFetchOffsetsFn(s.replayFrom),
	)
	if err != nil {
		return nil, err
	}
	s.cl = cl
	return s, nil
}

func (s *Stream) Ping(ctx context.Context) error { return s.cl.Ping(ctx) }

// replayFrom starts every assigned partition Replay ago (by record timestamp).
func (s *Stream) replayFrom(_ context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	from := time.Now().Add(-s.cfg.Replay).UnixMilli()
	for topic, parts := range offsets {
		for p := range parts {
			offsets[topic][p] = kgo.NewOffset().AfterMilli(from).WithEpoch(-1)
		}
	}
	return offsets, nil
}

func (s *Stream) assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, parts := range assigned {
		for _, p := range parts {
			s.parts[p] = &partitionState{users: map[userKey]*userState{}}
		}
	}
}

// revoked writes what changed and forgets the partition: its new owner replays it.
func (s *Stream) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.mu.Lock()
	var rows []Features
	for _, parts := range revoked {
		for _, p := range parts {
			if ps := s.parts[p]; ps != nil {
				rows = append(rows, dirtyRows(ps)...)
				delete(s.parts, p)
			}
		}
	}
	s.updateGauge()
	s.mu.Unlock()
	s.send(rows)
}

// Run consumes until ctx ends. Features are written by a separate goroutine so a slow
// ClickHouse never stops consumption for long: the queue is bounded, writes may be dropped
// and are repeated with the next change of the user.
func (s *Stream) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.writer()
	}()

	lastFlush := time.Now()
	for {
		// A bounded poll: with no new records the loop still wakes up to write changes.
		pctx, cancel := context.WithTimeout(ctx, s.cfg.FlushInterval)
		fetches := s.cl.PollRecords(pctx, 10_000)
		cancel()
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				s.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
			}
		})
		s.mu.Lock()
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			ps := s.parts[p.Partition]
			if ps == nil {
				return
			}
			for _, r := range p.Records {
				s.observe(ps, r)
			}
		})
		s.mu.Unlock()
		s.cl.AllowRebalance()

		if time.Since(lastFlush) >= s.cfg.FlushInterval {
			s.flush()
			lastFlush = time.Now()
		}
	}
	s.flush()
	close(s.out)
	wg.Wait()
	s.cl.AllowRebalance()
	s.cl.Close()
	return nil
}

func (s *Stream) observe(ps *partitionState, r *kgo.Record) {
	var ev event.Record
	if err := json.Unmarshal(r.Value, &ev); err != nil || !tracked(&ev) {
		return
	}
	k := userKey{ev.ProjectID, ev.UserID}
	st := ps.users[k]
	if st == nil {
		st = &userState{}
		ps.users[k] = st
	}
	st.observe(&ev)
	ps.watermark = max(ps.watermark, ev.TS)
	s.metrics.Events.Inc()
}

func dirtyRows(ps *partitionState) []Features {
	var rows []Features
	for k, st := range ps.users {
		if st.dirty {
			rows = append(rows, st.features(k))
			st.dirty = false
		}
	}
	return rows
}

// flush sends changed users to the writer and drops idle ones.
func (s *Stream) flush() {
	s.mu.Lock()
	var rows []Features
	total := 0
	for _, ps := range s.parts {
		rows = append(rows, dirtyRows(ps)...)
		cutoff := ps.watermark - s.cfg.IdleTTL.Milliseconds()
		for k, st := range ps.users {
			if st.last < cutoff {
				delete(ps.users, k)
				s.metrics.Evicted.Inc()
			}
		}
		total += len(ps.users)
	}
	if total > s.cfg.MaxUsers {
		s.shrink(total)
	}
	s.updateGauge()
	s.mu.Unlock()
	s.send(rows)
}

// shrink drops the least recently active users until the instance fits MaxUsers.
// Their features are already in ClickHouse; the memory bound protects the instance.
func (s *Stream) shrink(total int) {
	for _, step := range []time.Duration{6 * time.Hour, time.Hour, 10 * time.Minute, 0} {
		for _, ps := range s.parts {
			cutoff := ps.watermark - step.Milliseconds()
			for k, st := range ps.users {
				if total <= s.cfg.MaxUsers*9/10 {
					return
				}
				if step == 0 || st.last < cutoff {
					delete(ps.users, k)
					total--
					s.metrics.Evicted.Inc()
				}
			}
		}
	}
}

func (s *Stream) updateGauge() {
	n := 0
	for _, ps := range s.parts {
		n += len(ps.users)
	}
	s.metrics.Users.Set(float64(n))
}

func (s *Stream) send(rows []Features) {
	if len(rows) == 0 {
		return
	}
	select {
	case s.out <- rows:
	default:
		s.log.Warn("user_features queue full, dropping a write; the users are written again on their next change", "rows", len(rows))
		s.metrics.WriteErrors.Inc()
	}
}

func (s *Stream) writer() {
	for rows := range s.out {
		for attempt := 0; ; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := WriteFeatures(ctx, s.ch, rows, time.Now())
			cancel()
			if err == nil {
				s.metrics.FeaturesRows.Add(float64(len(rows)))
				break
			}
			s.metrics.WriteErrors.Inc()
			if attempt == 4 {
				s.log.Error("dropping user_features write", "rows", len(rows), "err", err)
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}
}

// WriteFeatures inserts feature rows; updated_at makes the latest row win.
func WriteFeatures(ctx context.Context, ch driver.Conn, rows []Features, at time.Time) error {
	batch, err := ch.PrepareBatch(ctx, `INSERT INTO user_features (project_id, user_id, first_event, last_seen,
		updates, intervals, fast_intervals, median_interval_ms, signature, has_username, is_premium, lang, updated_at)`)
	if err != nil {
		return err
	}
	for _, f := range rows {
		if err := batch.Append(f.ProjectID, f.UserID, f.FirstEvent, f.LastSeen, f.Updates, f.Intervals,
			f.FastIntervals, f.MedianIntervalMS, f.Signature, b2u(f.HasUsername), b2u(f.IsPremium), f.Lang, at); err != nil {
			_ = batch.Abort()
			return err
		}
	}
	return batch.Send()
}

func b2u(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}
