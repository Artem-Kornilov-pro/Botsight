package ingest

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// TxnProducer writes the records of a batch in a Kafka transaction: consumers reading
// read_committed see all of them or none. Without it a broker failing in the middle of
// a batch left part of it in Kafka; the collector answered 503, the SDK retried, and the
// written part was written again (found by the chaos test kafka_stop, Botsight.md, 18.3).
//
// One transaction per HTTP request would cost a coordinator round trip per batch, so
// requests are committed in groups: a worker takes the first waiting request, gathers
// what else arrives within Linger, and commits them together; every request of the group
// gets the outcome. Several workers, each a producer with its own transactional id, run
// in parallel.
//
// A commit can also fail without an answer: the broker stops after the coordinator wrote
// PrepareCommit but before it replied. franz-go then cannot tell whether the transaction
// is committed, and treating it as failed duplicated whole batches on the SDK retry (the
// nightly kafka_stop run, #41). Such an outcome is read from Kafka itself: the coordinator
// ends every transaction with a COMMIT or ABORT marker in each of its partitions, so the
// worker reads a partition of the group from one of its records until the marker of its
// producer (resolve). Until then the requests do not know the outcome; see PendingError.
type TxnProducer struct {
	reqs    chan *txnReq
	workers []*txnWorker
	wg      sync.WaitGroup
	closed  chan struct{}
	once    sync.Once
}

type TxnConfig struct {
	Brokers []string
	// IDPrefix must be stable for an instance across restarts (the hostname): a restarted
	// collector then fences its predecessor and aborts the transaction left open by a crash.
	IDPrefix string
	Workers  int           // parallel transactions, default 4
	Linger   time.Duration // how long a group waits for more requests, default 2 ms
	MaxGroup int           // records in one transaction, default 20 000
	Timeout  time.Duration // one transaction, default 10 s
	// ResolveTimeout bounds reading the outcome of a commit that got no answer, default
	// 90 s: less than the gate's processing mark (2 min), which keeps retries out meanwhile.
	ResolveTimeout time.Duration
	Logger         *slog.Logger

	// endTxn replaces EndTransaction in tests (a commit that happens but whose answer is lost)
	endTxn func(ctx context.Context, cl *kgo.Client, try kgo.TransactionEndTry) error
}

// ErrOutcomeUnknown: the transaction may be committed or not, and reading its marker did
// not tell within ResolveTimeout. The batch must be neither confirmed nor released.
var ErrOutcomeUnknown = errors.New("ingest: kafka transaction outcome unknown")

// PendingError is returned when a request stopped waiting (its context ended) while its
// transaction was still running or being resolved. Outcome delivers the final result:
// nil (committed), an error wrapping ErrOutcomeUnknown, or another error (not committed).
type PendingError struct {
	Outcome <-chan error
}

func (e *PendingError) Error() string { return "ingest: kafka transaction outcome not known yet" }

type txnReq struct {
	records []*kgo.Record
	done    chan error
}

type txnWorker struct {
	id  string
	cfg TxnConfig
	cl  *kgo.Client
}

func NewTxnProducer(cfg TxnConfig) (*TxnProducer, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if cfg.Linger <= 0 {
		cfg.Linger = 2 * time.Millisecond
	}
	if cfg.MaxGroup <= 0 {
		cfg.MaxGroup = 20_000
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.ResolveTimeout <= 0 {
		cfg.ResolveTimeout = 90 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	p := &TxnProducer{reqs: make(chan *txnReq), closed: make(chan struct{})}
	for i := range cfg.Workers {
		w := &txnWorker{id: fmt.Sprintf("%s-collector-%d", cfg.IDPrefix, i), cfg: cfg}
		if err := w.connect(); err != nil {
			p.Close()
			return nil, err
		}
		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			w.loop(p.reqs, p.closed)
		}()
	}
	return p, nil
}

func (w *txnWorker) connect() error {
	opts := append([]kgo.Opt{
		kgo.SeedBrokers(w.cfg.Brokers...),
		kgo.TransactionalID(w.id),
		kgo.TransactionTimeout(w.cfg.Timeout + 20*time.Second), // the coordinator aborts it after a crash
	}, kafka.ProducerOptions()...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	w.cl = cl
	return nil
}

// ProduceSync commits the records in a transaction, shared with requests arriving at the
// same moment. Every record gets the same error: the batch is in Kafka whole or not at all.
// A *PendingError means the outcome is not known yet (see TxnProducer).
func (p *TxnProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	req := &txnReq{records: rs, done: make(chan error, 1)}
	var err error
	select {
	case p.reqs <- req:
		select {
		case err = <-req.done:
		case <-ctx.Done():
			// the transaction goes on and may commit: the caller must not release the batch
			err = &PendingError{Outcome: req.done}
		}
	case <-ctx.Done():
		err = ctx.Err()
	case <-p.closed:
		err = errors.New("ingest: producer closed")
	}
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r, Err: err}
	}
	return out
}

func (w *txnWorker) loop(reqs <-chan *txnReq, closed <-chan struct{}) {
	defer func() {
		if w.cl != nil {
			w.cl.Close()
		}
	}()
	for {
		var first *txnReq
		select {
		case first = <-reqs:
		case <-closed:
			return
		}
		group, n := []*txnReq{first}, len(first.records)
		linger := time.NewTimer(w.cfg.Linger)
	gather:
		for n < w.cfg.MaxGroup {
			select {
			case r := <-reqs:
				group = append(group, r)
				n += len(r.records)
			case <-linger.C:
				break gather
			}
		}
		linger.Stop()
		err := w.commit(group, closed)
		for _, r := range group {
			r.done <- err
		}
	}
}

// commit runs one transaction. A failure before the commit aborts it; if even that fails,
// the client is replaced, which bumps the producer epoch and fences whatever the broker
// still holds of it. A commit that got no answer is resolved by reading its marker.
func (w *txnWorker) commit(group []*txnReq, closed <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.Timeout)
	defer cancel()
	if w.cl == nil {
		if err := w.connect(); err != nil {
			return err
		}
	}
	if err := w.cl.BeginTransaction(); err != nil {
		w.reset(err)
		return err
	}
	var mu sync.Mutex
	var produceErr error
	var probe *kgo.Record // a written record: its partition gets the transaction's marker
	for _, r := range group {
		for _, rec := range r.records {
			w.cl.Produce(ctx, rec, func(rec *kgo.Record, err error) {
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err != nil && produceErr == nil:
					produceErr = err
				case err == nil && probe == nil:
					probe = rec
				}
			})
		}
	}
	flushErr := w.cl.Flush(ctx)
	mu.Lock()
	failed := produceErr
	mu.Unlock()
	if failed == nil {
		failed = flushErr
	}
	if failed != nil {
		w.abort(ctx, failed)
		return failed
	}
	end := w.cfg.endTxn
	if end == nil {
		end = func(ctx context.Context, cl *kgo.Client, try kgo.TransactionEndTry) error {
			return cl.EndTransaction(ctx, try)
		}
	}
	if err := end(ctx, w.cl, kgo.TryCommit); err != nil {
		if errors.Is(err, kerr.OperationNotAttempted) || probe == nil {
			// the commit was never sent: the transaction can only be aborted
			w.abort(ctx, err)
			return err
		}
		// The commit may have reached the coordinator. This client cannot tell, and a
		// new one fences whatever is still open; the marker in Kafka has the answer.
		w.reset(err)
		return w.resolve(probe, err, closed)
	}
	return nil
}

// resolve reads the outcome of a commit that got no answer: the partition of probe from
// probe on, uncommitted and with control records, up to the marker of probe's producer.
// The coordinator writes it once the transaction is decided: at once for a commit it had
// started; for a commit that never reached it, when it aborts the transaction on its
// timeout (Timeout + 20 s).
func (w *txnWorker) resolve(probe *kgo.Record, cause error, closed <-chan struct{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.cfg.ResolveTimeout)
	defer cancel()
	go func() {
		select {
		case <-closed:
			cancel()
		case <-ctx.Done():
		}
	}()
	committed, err := TxnCommitted(ctx, w.cfg.Brokers, probe)
	switch {
	case err != nil:
		w.cfg.Logger.Error("kafka transaction outcome unknown", "id", w.id, "err", errors.Join(cause, err))
		return fmt.Errorf("%w: %w", ErrOutcomeUnknown, errors.Join(cause, err))
	case committed:
		w.cfg.Logger.Warn("kafka commit got no answer but is committed", "id", w.id, "err", cause)
		return nil
	default:
		w.cfg.Logger.Warn("kafka commit got no answer and was aborted", "id", w.id, "err", cause)
		return fmt.Errorf("ingest: kafka transaction aborted: %w", cause)
	}
}

// TxnCommitted tells whether the transaction that wrote rec (a record returned by a
// transactional produce, with its offset and producer id) is committed: it reads rec's
// partition from rec on, uncommitted and with control records, until the COMMIT or ABORT
// marker of rec's producer. It waits for the marker until ctx ends.
func TxnCommitted(ctx context.Context, brokers []string, rec *kgo.Record) (bool, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			rec.Topic: {rec.Partition: kgo.NewOffset().At(rec.Offset)},
		}),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return false, err
	}
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return false, err
		}
		var found, commit bool
		fetches.EachRecord(func(r *kgo.Record) {
			if found || !r.Attrs.IsControl() || r.ProducerID != rec.ProducerID || r.Offset <= rec.Offset {
				return
			}
			// the key of a control record: version int16, type int16 (0 abort, 1 commit)
			if len(r.Key) >= 4 {
				found, commit = true, binary.BigEndian.Uint16(r.Key[2:4]) == 1
			}
		})
		if found {
			return commit, nil
		}
		if fetches.Err() != nil { // the broker is still down: poll again a bit later
			select {
			case <-ctx.Done():
				return false, ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}

func (w *txnWorker) abort(ctx context.Context, cause error) {
	err := w.cl.AbortBufferedRecords(ctx)
	if err == nil {
		err = w.cl.EndTransaction(ctx, kgo.TryAbort)
	}
	if err != nil {
		w.reset(errors.Join(cause, err))
		return
	}
	w.cfg.Logger.Warn("kafka transaction aborted", "id", w.id, "err", cause)
}

func (w *txnWorker) reset(cause error) {
	w.cfg.Logger.Warn("kafka transactional producer reset", "id", w.id, "err", cause)
	if w.cl != nil {
		w.cl.Close()
		w.cl = nil
	}
}

// Close stops the workers after the transactions in progress.
func (p *TxnProducer) Close() {
	p.once.Do(func() { close(p.closed) })
	p.wg.Wait()
}
