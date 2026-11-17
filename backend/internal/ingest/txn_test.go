package ingest_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/ingest"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

func txnTopic(t *testing.T) string {
	t.Helper()
	topic := fmt.Sprintf("txn-test-%d", time.Now().UnixNano())
	if err := kafka.EnsureTopics(context.Background(), producer, 1, []kafka.Topic{{Name: topic, Partitions: 3, Retention: time.Hour}}); err != nil {
		t.Fatal(err)
	}
	return topic
}

// count reads the topic from the start for wait and returns the values seen.
func count(t *testing.T, topic string, level kgo.IsolationLevel, wait time.Duration) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.KafkaBrokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.FetchIsolationLevel(level))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	n := 0
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(*kgo.Record) { n++ })
	}
	return n
}

func newTxn(t *testing.T, workers int, linger time.Duration) *ingest.TxnProducer {
	t.Helper()
	p, err := ingest.NewTxnProducer(ingest.TxnConfig{
		Brokers: env.KafkaBrokers, IDPrefix: fmt.Sprintf("test-%d", time.Now().UnixNano()),
		Workers: workers, Linger: linger, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func records(topic string, n int, size int) []*kgo.Record {
	out := make([]*kgo.Record, n)
	for i := range out {
		out[i] = &kgo.Record{Topic: topic, Key: []byte(fmt.Sprint(i)), Value: bytes.Repeat([]byte("x"), size)}
	}
	return out
}

func TestTxnProducerCommitsGroups(t *testing.T) {
	requireEnv(t)
	topic := txnTopic(t)
	p := newTxn(t, 2, 5*time.Millisecond)
	var wg sync.WaitGroup
	for range 20 { // concurrent requests share transactions
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.ProduceSync(context.Background(), records(topic, 25, 100)...).FirstErr(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := count(t, topic, kgo.ReadCommitted(), 3*time.Second); n != 500 {
		t.Fatalf("committed records: %d, want 500", n)
	}
}

// One record of a group cannot be written: the whole transaction is aborted. The records
// that reached the broker are in the log, but a read_committed consumer, like ch-writer,
// never sees them, and every request of the group gets the error to retry.
func TestTxnProducerAllOrNothing(t *testing.T) {
	requireEnv(t)
	topic := txnTopic(t)
	// one worker: a second idle worker would take the second request in its own group
	p := newTxn(t, 1, 200*time.Millisecond)
	var wg sync.WaitGroup
	var good, bad error
	wg.Add(2)
	go func() {
		defer wg.Done()
		good = p.ProduceSync(context.Background(), records(topic, 50, 100)...).FirstErr()
	}()
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		// over the 1 MiB batch limit: the record fails, the others of the group were sent
		bad = p.ProduceSync(context.Background(), records(topic, 1, 2<<20)...).FirstErr()
	}()
	wg.Wait()
	if good == nil || bad == nil {
		t.Fatalf("both requests of the group fail: good=%v bad=%v", good, bad)
	}
	if n := count(t, topic, kgo.ReadUncommitted(), 3*time.Second); n != 50 {
		t.Fatalf("the healthy request's records reached the log before the abort: %d", n)
	}
	if n := count(t, topic, kgo.ReadCommitted(), 3*time.Second); n != 0 {
		t.Fatalf("read_committed saw %d records of an aborted transaction", n)
	}

	// the producer goes on with the next transaction
	if err := p.ProduceSync(context.Background(), records(topic, 10, 100)...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, topic, kgo.ReadCommitted(), 3*time.Second); n != 10 {
		t.Fatalf("after the abort: %d committed, want 10", n)
	}
}

// The collector on the transactional producer: an accepted batch is whole for
// read_committed consumers.
func TestCollectorWithTransactions(t *testing.T) {
	requireEnv(t)
	p := newTxn(t, 2, 2*time.Millisecond)
	h := newHarness(t, options{producer: p})
	events := make([]map[string]any, 30)
	for i := range events {
		events[i] = sdkEvent(map[string]any{"user_id": i + 1})
	}
	if r := h.send(sdkBatch(events...)); r.status != 202 {
		t.Fatalf("%d %v", r.status, r.body)
	}
	if got := consume(t, kafka.TopicEvents, h.project.ID); len(got) != 30 {
		t.Fatalf("events in Kafka: %d", len(got))
	}
}

// TxnCommitted reads the outcome of a transaction from its marker (#41): committed,
// aborted, or not decided yet, and it ignores the markers of other producers.
func TestTxnCommittedReadsTheMarker(t *testing.T) {
	requireEnv(t)
	topic := txnTopic(t)
	newProducer := func(id string) *kgo.Client {
		cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(env.KafkaBrokers...),
			kgo.TransactionalID(fmt.Sprintf("%s-%d", id, time.Now().UnixNano()))}, kafka.ProducerOptions()...)...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		return cl
	}
	write := func(cl *kgo.Client, end *kgo.TransactionEndTry) *kgo.Record {
		t.Helper()
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		rec := &kgo.Record{Topic: topic, Partition: 0, Value: []byte("x")}
		if err := cl.ProduceSync(context.Background(), rec).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if end != nil {
			if err := cl.EndTransaction(context.Background(), *end); err != nil {
				t.Fatal(err)
			}
		}
		return rec
	}
	commit, abort := kgo.TryCommit, kgo.TryAbort
	a, b := newProducer("a"), newProducer("b")

	committed := write(a, &commit)
	aborted := write(a, &abort)
	open := write(a, nil)
	write(b, &commit) // another producer's marker after the open transaction

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, c := range []struct {
		name string
		rec  *kgo.Record
		want bool
	}{{"committed", committed, true}, {"aborted", aborted, false}} {
		got, err := ingest.TxnCommitted(ctx, env.KafkaBrokers, c.rec)
		if err != nil || got != c.want {
			t.Fatalf("%s: committed=%v err=%v", c.name, got, err)
		}
	}
	short, cancelShort := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShort()
	if got, err := ingest.TxnCommitted(short, env.KafkaBrokers, open); err == nil {
		t.Fatalf("an open transaction has no outcome yet, got committed=%v", got)
	}
	if err := a.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	if got, err := ingest.TxnCommitted(ctx, env.KafkaBrokers, open); err != nil || !got {
		t.Fatalf("after its commit: committed=%v err=%v", got, err)
	}
}

// pendingProducer answers the first request like a transaction whose commit got no
// answer: the request gives up, the outcome comes later. Later requests go to Kafka.
type pendingProducer struct {
	ingest.Producer
	outcome chan error
	calls   int
	mu      sync.Mutex
}

func (p *pendingProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	p.mu.Lock()
	p.calls++
	first := p.calls == 1
	p.mu.Unlock()
	if !first {
		return p.Producer.ProduceSync(ctx, rs...)
	}
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r, Err: &ingest.PendingError{Outcome: p.outcome}}
	}
	return out
}

// A batch whose transaction outcome is unknown is neither released nor confirmed: the
// retry waits (409) until the outcome arrives. Committed — the retry gets 202 duplicate
// and nothing is written twice; aborted — the retry writes it (#41).
func TestPendingOutcomeSettlesTheGate(t *testing.T) {
	requireEnv(t)
	for _, c := range []struct {
		name      string
		outcome   error
		duplicate bool
	}{{"committed", nil, true}, {"aborted", fmt.Errorf("ingest: kafka transaction aborted"), false}} {
		t.Run(c.name, func(t *testing.T) {
			p := &pendingProducer{Producer: producer, outcome: make(chan error, 1)}
			h := newHarness(t, options{producer: p})
			batch := sdkBatch(sdkEvent(nil), sdkEvent(nil), sdkEvent(nil))
			if r := h.send(batch); r.status != http.StatusServiceUnavailable {
				t.Fatalf("first attempt: %d %v", r.status, r.body)
			}
			if r := h.send(batch); r.status != http.StatusConflict {
				t.Fatalf("retry while the outcome is unknown: %d %v, want 409", r.status, r.body)
			}
			p.outcome <- c.outcome
			deadline := time.Now().Add(10 * time.Second)
			for {
				r := h.send(batch)
				if r.status == http.StatusAccepted {
					if r.body["duplicate"] != c.duplicate || r.body["accepted"] != 3.0 {
						t.Fatalf("retry after the outcome: %v, want duplicate=%v", r.body, c.duplicate)
					}
					break
				}
				if r.status != http.StatusConflict || time.Now().After(deadline) {
					t.Fatalf("retry after the outcome: %d %v", r.status, r.body)
				}
				time.Sleep(20 * time.Millisecond)
			}
			p.mu.Lock()
			calls := p.calls
			p.mu.Unlock()
			if want := map[bool]int{true: 1, false: 2}[c.duplicate]; calls != want {
				t.Fatalf("produce calls %d, want %d", calls, want)
			}
		})
	}
}

// The case of the nightly kafka_stop run (#41): the coordinator commits, but the answer
// never comes back. Before, the request got an error, the collector released the batch,
// and the SDK retry wrote it twice. The producer reads the marker and reports success.
func TestTxnProducerCommitWithoutAnswer(t *testing.T) {
	requireEnv(t)
	topic := txnTopic(t)
	lost := errors.New("context deadline exceeded")
	cfg := ingest.WithEndTxn(ingest.TxnConfig{
		Brokers: env.KafkaBrokers, IDPrefix: fmt.Sprintf("test-%d", time.Now().UnixNano()),
		Workers: 1, Logger: slog.New(slog.DiscardHandler), ResolveTimeout: 20 * time.Second,
	}, func(ctx context.Context, cl *kgo.Client, try kgo.TransactionEndTry) error {
		if err := cl.EndTransaction(ctx, try); err != nil {
			return err
		}
		return lost // committed, the answer is lost
	})
	p, err := ingest.NewTxnProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	if err := p.ProduceSync(context.Background(), records(topic, 20, 10)...).FirstErr(); err != nil {
		t.Fatalf("a committed transaction is reported as failed: %v", err)
	}
	if n := count(t, topic, kgo.ReadCommitted(), 3*time.Second); n != 20 {
		t.Fatalf("committed records: %d, want 20", n)
	}

	// a request that gives up first gets the outcome later
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := p.ProduceSync(ctx, records(topic, 5, 10)...).FirstErr()
	var pending *ingest.PendingError
	if res != nil && !errors.As(res, &pending) && !errors.Is(res, context.Canceled) {
		t.Fatalf("given up request: %v", res)
	}
	if pending != nil {
		select {
		case err := <-pending.Outcome:
			if err != nil {
				t.Fatalf("outcome: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("no outcome")
		}
	}
}
