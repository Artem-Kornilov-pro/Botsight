package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
// The window left: a commit sent whose outcome is unknown (the coordinator failed exactly
// then). The request gets 503 and its retry may duplicate the batch; before, any broker
// failure in the middle of a write did.
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
	Logger   *slog.Logger
}

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
func (p *TxnProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	req := &txnReq{records: rs, done: make(chan error, 1)}
	var err error
	select {
	case p.reqs <- req:
		select {
		case err = <-req.done:
		case <-ctx.Done():
			// the transaction goes on; its outcome is unknown to this request: answer 503,
			// the retry is told the batch is done or still in progress by the gate
			err = ctx.Err()
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
		err := w.commit(group)
		for _, r := range group {
			r.done <- err
		}
	}
}

// commit runs one transaction. On any failure the transaction is aborted; if even that
// fails, the client is replaced, which bumps the producer epoch and fences whatever the
// broker still holds of it.
func (w *txnWorker) commit(group []*txnReq) error {
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
	for _, r := range group {
		for _, rec := range r.records {
			w.cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
				if err != nil {
					mu.Lock()
					if produceErr == nil {
						produceErr = err
					}
					mu.Unlock()
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
	if err := w.cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		// unknown or failed commit: abort what may be left; the requests get the error
		w.abort(ctx, err)
		return err
	}
	return nil
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
