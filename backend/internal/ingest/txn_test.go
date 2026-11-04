package ingest_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
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
