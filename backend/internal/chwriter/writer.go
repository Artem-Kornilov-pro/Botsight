// Package chwriter moves records from Kafka to ClickHouse (Botsight.md, 11.2-11.3).
//
// Every assigned partition has its own goroutine that collects records into a chunk
// (up to BatchRows or FlushInterval), inserts it in one INSERT and only then commits
// the Kafka offset. A crash between the insert and the commit makes the next owner
// read the same records again; it rebuilds exactly the same chunk from the plan saved
// in Redis before the insert and repeats the insert with the same
// insert_deduplication_token, which ClickHouse drops as a duplicate.
package chwriter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

type Config struct {
	Brokers       []string
	Group         string
	BatchRows     int           // rows per insert, default 50 000
	FlushInterval time.Duration // max age of the oldest buffered record, default 1 s
	// MaxBufferedRows per partition; above it fetching of the partition pauses.
	MaxBufferedRows int
	RetryMax        time.Duration // cap of the insert retry backoff
	PlanTTL         time.Duration // how long a chunk plan is kept in Redis
	// AfterInsert is a failure-injection hook for tests (Botsight.md, 18.3): an error
	// stops the writer as if the process died between the insert and the commit.
	AfterInsert func(topic string, partition int32, token string) error
}

func (c *Config) defaults() {
	if c.Group == "" {
		c.Group = "ch-writer"
	}
	if c.BatchRows <= 0 {
		c.BatchRows = 50_000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.MaxBufferedRows <= 0 {
		c.MaxBufferedRows = 4 * c.BatchRows
	}
	if c.RetryMax <= 0 {
		c.RetryMax = 30 * time.Second
	}
	if c.PlanTTL <= 0 {
		c.PlanTTL = 7 * 24 * time.Hour
	}
}

// ErrCrashed is returned by Run when AfterInsert simulated a crash.
var ErrCrashed = errors.New("chwriter: simulated crash")

type topicPartition struct {
	topic     string
	partition int32
}

type Writer struct {
	cfg     Config
	cl      *kgo.Client
	ch      driver.Conn
	rdb     redis.UniversalClient
	sinks   map[string]Sink
	log     *slog.Logger
	metrics *Metrics

	crash context.CancelCauseFunc // set by Run; used by the AfterInsert crash simulation

	mu      sync.Mutex
	workers map[topicPartition]*partitionWorker
}

func New(cfg Config, ch driver.Conn, rdb redis.UniversalClient, sinks []Sink, log *slog.Logger, m *Metrics) (*Writer, error) {
	cfg.defaults()
	w := &Writer{
		cfg: cfg, ch: ch, rdb: rdb, log: log, metrics: m,
		sinks: map[string]Sink{}, workers: map[topicPartition]*partitionWorker{},
	}
	topics := make([]string, 0, len(sinks))
	for _, s := range sinks {
		w.sinks[s.Topic] = s
		topics = append(topics, s.Topic)
	}
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(64 << 20),
		kgo.OnPartitionsAssigned(w.assigned),
		kgo.OnPartitionsRevoked(w.revoked),
		kgo.OnPartitionsLost(w.revoked),
	}, kafka.ProducerOptions()...) // the same client produces DLQ records
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	w.cl = cl
	return w, nil
}

// Ping checks Kafka for readiness probes.
func (w *Writer) Ping(ctx context.Context) error { return w.cl.Ping(ctx) }

// Run consumes until ctx ends, then flushes what it can and leaves the group.
func (w *Writer) Run(ctx context.Context) error {
	runCtx, crash := context.WithCancelCause(ctx)
	w.crash = crash
	go w.reportLag(runCtx)

	for {
		fetches := w.cl.PollRecords(runCtx, 10_000)
		if fetches.IsClientClosed() || runCtx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				w.log.Warn("fetch error", "topic", topic, "partition", p, "err", err)
			}
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) == 0 {
				return
			}
			if pw := w.worker(p.Topic, p.Partition); pw != nil {
				pw.add(p.Records)
			}
		})
		w.cl.AllowRebalance()
	}

	crashed := errors.Is(context.Cause(runCtx), ErrCrashed)
	// Workers drain while rebalancing is still blocked by the last poll, so the
	// partitions are ours until their commits are done.
	w.stopAll(!crashed)
	w.cl.AllowRebalance() // otherwise leaving the group waits for the blocked poll forever
	w.cl.Close()          // leaves the group; nothing is committed here
	if crashed {
		return ErrCrashed
	}
	return nil
}

func (w *Writer) worker(topic string, partition int32) *partitionWorker {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.workers[topicPartition{topic, partition}]
}

func (w *Writer) assigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		sink, ok := w.sinks[topic]
		if !ok {
			continue
		}
		for _, p := range partitions {
			pw := newPartitionWorker(w, sink, p)
			pw.plan = w.loadPlan(ctx, topic, p)
			w.mu.Lock()
			w.workers[topicPartition{topic, p}] = pw
			w.mu.Unlock()
			go pw.run()
		}
	}
}

// revoked stops the workers of partitions that moved to another member. A worker
// finishes its current insert and commit; buffered records are dropped without a
// commit and the new owner reads them again.
func (w *Writer) revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	var stopping []*partitionWorker
	w.mu.Lock()
	for topic, partitions := range revoked {
		for _, p := range partitions {
			tp := topicPartition{topic, p}
			if pw := w.workers[tp]; pw != nil {
				stopping = append(stopping, pw)
				delete(w.workers, tp)
			}
		}
	}
	w.mu.Unlock()
	for _, pw := range stopping {
		pw.stop(false)
	}
}

// stopAll stops every worker; with drain they first flush buffered records.
func (w *Writer) stopAll(drain bool) {
	w.mu.Lock()
	all := make([]*partitionWorker, 0, len(w.workers))
	for tp, pw := range w.workers {
		all = append(all, pw)
		delete(w.workers, tp)
	}
	w.mu.Unlock()
	var wg sync.WaitGroup
	for _, pw := range all {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pw.stop(drain)
		}()
	}
	wg.Wait()
}

// --- chunk plans in Redis ---

type plan struct{ first, last int64 }

func planKey(topic string, partition int32) string {
	return "writer:plan:" + topic + ":" + strconv.Itoa(int(partition))
}

func (w *Writer) loadPlan(ctx context.Context, topic string, partition int32) *plan {
	v, err := w.rdb.Get(ctx, planKey(topic, partition)).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			w.log.Warn("cannot load chunk plan, a crash before this point may duplicate one chunk",
				"topic", topic, "partition", partition, "err", err)
		}
		return nil
	}
	first, last, ok := strings.Cut(v, "-")
	f, err1 := strconv.ParseInt(first, 10, 64)
	l, err2 := strconv.ParseInt(last, 10, 64)
	if !ok || err1 != nil || err2 != nil || l < f {
		return nil
	}
	return &plan{first: f, last: l}
}

func (w *Writer) savePlan(ctx context.Context, topic string, partition int32, first, last int64) error {
	return w.rdb.Set(ctx, planKey(topic, partition), fmt.Sprintf("%d-%d", first, last), w.cfg.PlanTTL).Err()
}

// --- inserts ---

func (w *Writer) insert(ctx context.Context, sink Sink, token string, rows [][]any) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"insert_deduplicate":         1,
		"insert_deduplication_token": token,
		// dependent materialized views (daily aggregates) must not count a repeated block twice
		"deduplicate_blocks_in_dependent_materialized_views": 1,
	}))
	batch, err := w.ch.PrepareBatch(ctx, sink.insertSQL())
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append: %w", err)
		}
	}
	return batch.Send()
}

func (w *Writer) sendDLQ(ctx context.Context, recs []*kgo.Record) error {
	return w.cl.ProduceSync(ctx, recs...).FirstErr()
}

// reportLag exports the group lag per partition every 15 seconds.
func (w *Writer) reportLag(ctx context.Context) {
	adm := kadm.NewClient(w.cl)
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		lags, err := adm.Lag(ctx, w.cfg.Group)
		if err != nil {
			continue
		}
		lags.Each(func(l kadm.DescribedGroupLag) {
			for topic, partitions := range l.Lag {
				for p, gl := range partitions {
					w.metrics.Lag.WithLabelValues(topic, strconv.Itoa(int(p))).Set(float64(gl.Lag))
				}
			}
		})
	}
}

func backoff(attempt int, max time.Duration) time.Duration {
	d := 100 * time.Millisecond << min(attempt, 10)
	if d > max {
		d = max
	}
	return d/2 + rand.N(d/2+1)
}
