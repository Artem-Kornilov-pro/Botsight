package chwriter

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// planWait bounds how long a worker waits for the records of a planned chunk. Offsets
// may have gaps (transaction markers), so the exact last offset might never arrive.
const planWait = 30 * time.Second

type partitionWorker struct {
	w         *Writer
	sink      Sink
	topic     string
	partition int32

	mu      sync.Mutex
	buf     []*kgo.Record
	firstAt time.Time
	paused  bool

	plan      *plan // chunk boundaries to reproduce after a crash, see package doc
	planSince time.Time

	// ctx bounds inserts and commits: cancelled at once when the partition is revoked,
	// after drainTimeout on a graceful shutdown.
	ctx    context.Context
	cancel context.CancelFunc
	wake   chan struct{}
	quit   chan struct{}
	drain  bool
	done   chan struct{}
}

const drainTimeout = 10 * time.Second

func newPartitionWorker(w *Writer, sink Sink, partition int32) *partitionWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &partitionWorker{
		w: w, sink: sink, topic: sink.Topic, partition: partition,
		ctx: ctx, cancel: cancel,
		wake: make(chan struct{}, 1), quit: make(chan struct{}), done: make(chan struct{}),
		planSince: time.Now(),
	}
}

// add is called by the poll loop. Fetching pauses when too much is buffered
// (ClickHouse slow or down): the data waits in Kafka, not in memory.
func (pw *partitionWorker) add(recs []*kgo.Record) {
	pw.mu.Lock()
	if len(pw.buf) == 0 {
		pw.firstAt = time.Now()
	}
	pw.buf = append(pw.buf, recs...)
	full := len(pw.buf) >= pw.w.cfg.MaxBufferedRows && !pw.paused
	if full {
		pw.paused = true
	}
	pw.mu.Unlock()
	if full {
		pw.setPaused(true)
	}
	select {
	case pw.wake <- struct{}{}:
	default:
	}
}

func (pw *partitionWorker) setPaused(paused bool) {
	tp := map[string][]int32{pw.topic: {pw.partition}}
	if paused {
		pw.w.cl.PauseFetchPartitions(tp)
		pw.w.metrics.Paused.Inc()
	} else {
		pw.w.cl.ResumeFetchPartitions(tp)
		pw.w.metrics.Paused.Dec()
	}
}

// stop ends the worker. Without drain the current step is aborted and nothing more is
// committed; with drain buffered records are flushed within drainTimeout.
func (pw *partitionWorker) stop(drain bool) {
	pw.mu.Lock()
	pw.drain = drain
	pw.mu.Unlock()
	if drain {
		t := time.AfterFunc(drainTimeout, pw.cancel)
		defer t.Stop()
	} else {
		pw.cancel()
	}
	close(pw.quit)
	<-pw.done
	pw.cancel()
}

func (pw *partitionWorker) stopping() bool {
	select {
	case <-pw.quit:
		return true
	default:
		return false
	}
}

func (pw *partitionWorker) run() {
	defer close(pw.done)
	defer func() {
		pw.mu.Lock()
		paused := pw.paused
		pw.paused = false
		pw.mu.Unlock()
		if paused {
			pw.w.metrics.Paused.Dec()
		}
	}()
	tick := time.NewTicker(max(pw.w.cfg.FlushInterval/4, 10*time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-pw.quit:
			pw.mu.Lock()
			drain := pw.drain
			pw.mu.Unlock()
			if drain {
				pw.drainBuffered()
			}
			return
		case <-pw.wake:
		case <-tick.C:
		}
		for {
			chunk := pw.take(false)
			if chunk == nil {
				break
			}
			if !pw.flush(chunk) {
				return
			}
		}
	}
}

// drainBuffered flushes everything on a graceful shutdown, bounded by drainTimeout.
func (pw *partitionWorker) drainBuffered() {
	for {
		chunk := pw.take(true)
		if chunk == nil || !pw.flush(chunk) {
			return
		}
	}
}

// take returns the next chunk to insert, or nil if it is not time yet.
func (pw *partitionWorker) take(force bool) []*kgo.Record {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	if len(pw.buf) == 0 {
		return nil
	}
	n := 0
	if p := pw.plan; p != nil {
		switch {
		case pw.buf[0].Offset != p.first:
			pw.plan = nil // consumption did not restart at the planned chunk
		default:
			for n < len(pw.buf) && pw.buf[n].Offset <= p.last {
				n++
			}
			complete := n < len(pw.buf) || pw.buf[n-1].Offset == p.last
			if !complete && !force && time.Since(pw.planSince) < planWait {
				return nil
			}
		}
	}
	if pw.plan == nil {
		switch {
		case len(pw.buf) >= pw.w.cfg.BatchRows:
			n = pw.w.cfg.BatchRows
		case force || time.Since(pw.firstAt) >= pw.w.cfg.FlushInterval:
			n = len(pw.buf)
		default:
			return nil
		}
	}
	return append([]*kgo.Record(nil), pw.buf[:n]...)
}

// flush inserts one chunk and commits its offset. It returns false when the worker
// must stop (revoked, shutting down or simulated crash) without committing.
func (pw *partitionWorker) flush(chunk []*kgo.Record) bool {
	w, ctx := pw.w, pw.ctx
	first, last := chunk[0].Offset, chunk[len(chunk)-1].Offset
	token := fmt.Sprintf("%s/p%d/%d-%d", pw.topic, pw.partition, first, last)

	rows := make([][]any, 0, len(chunk))
	var dlq []*kgo.Record
	for _, r := range chunk {
		row, err := pw.sink.Row(r.Value)
		if err != nil {
			dlq = append(dlq, deadLetter(r, err))
			continue
		}
		rows = append(rows, row)
	}

	// The plan goes first: after a crash the chunk is rebuilt with the same boundaries.
	if !pw.retry("save plan", func() error { return w.savePlan(ctx, pw.topic, pw.partition, first, last) }) {
		return false
	}
	if len(dlq) > 0 {
		if !pw.retry("dlq", func() error { return w.sendDLQ(ctx, dlq) }) {
			return false
		}
		w.metrics.DLQ.WithLabelValues(pw.topic).Add(float64(len(dlq)))
		w.log.Warn("records sent to DLQ", "topic", pw.topic, "partition", pw.partition, "count", len(dlq))
	}
	if len(rows) > 0 {
		started := time.Now()
		ok := pw.retry("insert", func() error {
			err := w.insert(ctx, pw.sink, token, rows)
			if err != nil {
				w.metrics.InsertErrors.WithLabelValues(pw.sink.Table).Inc()
				pw.pauseWhileFailing()
			}
			return err
		})
		if !ok {
			return false
		}
		w.metrics.InsertDuration.WithLabelValues(pw.sink.Table).Observe(time.Since(started).Seconds())
		w.metrics.Rows.WithLabelValues(pw.sink.Table).Add(float64(len(rows)))
	}

	if hook := w.cfg.AfterInsert; hook != nil {
		if err := hook(pw.topic, pw.partition, token); err != nil {
			w.crash(ErrCrashed)
			return false
		}
	}

	// The offset is committed only after the insert (Botsight.md, 7.3).
	if !pw.retry("commit", func() error { return w.cl.CommitRecords(ctx, chunk[len(chunk)-1]) }) {
		return false
	}

	pw.mu.Lock()
	pw.buf = pw.buf[len(chunk):]
	if len(pw.buf) == 0 {
		pw.buf = nil // let the backing array go
	} else {
		pw.firstAt = time.Now()
	}
	pw.plan = nil
	resume := pw.paused && len(pw.buf) < pw.w.cfg.BatchRows
	if resume {
		pw.paused = false
	}
	pw.mu.Unlock()
	if resume {
		pw.setPaused(false)
	}
	return true
}

// pauseWhileFailing stops fetching the partition while ClickHouse rejects inserts.
func (pw *partitionWorker) pauseWhileFailing() {
	pw.mu.Lock()
	pause := !pw.paused
	pw.paused = true
	pw.mu.Unlock()
	if pause {
		pw.setPaused(true)
	}
}

// retry repeats fn with backoff until it succeeds or the worker context ends.
func (pw *partitionWorker) retry(what string, fn func() error) bool {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil {
			return true
		}
		if pw.ctx.Err() != nil {
			return false
		}
		pw.w.log.Warn("ch-writer step failed, retrying", "step", what, "topic", pw.topic,
			"partition", pw.partition, "attempt", attempt+1, "err", err)
		select {
		case <-pw.ctx.Done():
			return false
		case <-time.After(backoff(attempt, pw.w.cfg.RetryMax)):
		}
	}
}

func deadLetter(r *kgo.Record, cause error) *kgo.Record {
	headers := append([]kgo.RecordHeader{
		{Key: "original_topic", Value: []byte(r.Topic)},
		{Key: "original_partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
		{Key: "original_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
		{Key: "error", Value: []byte(cause.Error())},
	}, r.Headers...)
	return &kgo.Record{Topic: kafka.TopicDLQ, Key: r.Key, Value: r.Value, Headers: headers}
}
