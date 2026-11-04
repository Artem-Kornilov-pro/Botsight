package live

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/event"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/testenv"
)

// fake is a topic in memory: the test pushes records, the running consumer delivers them.
type fake struct {
	records chan rec
	runs    atomic.Int32
	active  atomic.Int32
}

type rec struct {
	pid   uint32
	value []byte
}

func newFake() *fake { return &fake{records: make(chan rec, 1024)} }

func (f *fake) Run(ctx context.Context, deliver func(uint32, []byte)) error {
	f.runs.Add(1)
	f.active.Add(1)
	defer f.active.Add(-1)
	for {
		select {
		case <-ctx.Done():
			return nil
		case r := <-f.records:
			deliver(r.pid, r.value)
		}
	}
}

func record(pid uint32, user uint64, name string) rec {
	b, _ := json.Marshal(event.Record{ProjectID: pid, EventID: name + strconv.FormatUint(user, 10), Type: "update",
		Name: name, UserID: user, TS: 1_730_000_000_123})
	return rec{pid, b}
}

func receive(t *testing.T, s *Sub) Event {
	t.Helper()
	select {
	case ev := <-s.C:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no event")
	}
	return Event{}
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met")
}

func newHub(src Source) *Hub {
	h := NewHub(src, slog.New(slog.DiscardHandler))
	h.Linger = 50 * time.Millisecond
	return h
}

func TestFanOutAndIsolation(t *testing.T) {
	src := newFake()
	h := newHub(src)
	defer h.Close()
	if h.Running() {
		t.Fatal("consumer runs without subscribers")
	}
	a1, _ := h.Subscribe(1)
	a2, _ := h.Subscribe(1)
	b, _ := h.Subscribe(2)

	src.records <- record(3, 1, "nobody_watches")
	src.records <- record(1, 9_100_000_000_000_000_001, "message")
	for _, s := range []*Sub{a1, a2} {
		ev := receive(t, s)
		if ev.Name != "message" || ev.UserID != "9100000000000000001" || !ev.TS.Equal(time.UnixMilli(1_730_000_000_123)) {
			t.Fatalf("%+v", ev)
		}
	}
	src.records <- record(2, 5, "callback_query")
	if ev := receive(t, b); ev.Name != "callback_query" {
		t.Fatalf("%+v", ev)
	}
	select {
	case ev := <-a1.C:
		t.Fatalf("project 1 got an event of project 2: %+v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSlowSubscriberDoesNotBlockOthers(t *testing.T) {
	src := newFake()
	h := newHub(src)
	h.Buffer = 4
	defer h.Close()
	slow, _ := h.Subscribe(1)
	fast, _ := h.Subscribe(1)
	for i := range 10 {
		src.records <- record(1, uint64(i+1), "message")
		receive(t, fast)
	}
	if n := slow.TakeDropped(); n != 6 || len(slow.C) != 4 {
		t.Fatalf("dropped %d, buffered %d", n, len(slow.C))
	}
	if slow.TakeDropped() != 0 {
		t.Fatal("dropped count is not reset")
	}
}

func TestConsumerLivesWhileWatched(t *testing.T) {
	src := newFake()
	h := newHub(src)
	defer h.Close()
	s, _ := h.Subscribe(1)
	eventually(t, func() bool { return src.active.Load() == 1 })
	s.Close()
	s.Close() // twice is fine
	if _, ok := <-s.C; ok {
		t.Fatal("channel not closed")
	}
	// a subscriber within Linger keeps the same consumer
	s2, _ := h.Subscribe(1)
	time.Sleep(100 * time.Millisecond)
	if src.runs.Load() != 1 {
		t.Fatalf("consumer restarted: %d runs", src.runs.Load())
	}
	s2.Close()
	eventually(t, func() bool { return src.active.Load() == 0 && !h.Running() })
	s3, _ := h.Subscribe(1)
	eventually(t, func() bool { return src.active.Load() == 1 && src.runs.Load() == 2 })
	s3.Close()
}

func TestLimitsAndClose(t *testing.T) {
	h := newHub(newFake())
	h.MaxPerProject, h.MaxTotal = 2, 3
	a, _ := h.Subscribe(1)
	_, _ = h.Subscribe(1)
	if _, err := h.Subscribe(1); !errors.Is(err, ErrTooMany) {
		t.Fatalf("per project: %v", err)
	}
	_, _ = h.Subscribe(2)
	if _, err := h.Subscribe(3); !errors.Is(err, ErrTooMany) {
		t.Fatalf("total: %v", err)
	}
	h.Close()
	if _, ok := <-a.C; ok {
		t.Fatal("streams must end on Close")
	}
	a.Close() // after the hub: no double close
	if _, err := h.Subscribe(1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

type failing struct{ calls atomic.Int32 }

func (f *failing) Run(ctx context.Context, _ func(uint32, []byte)) error {
	f.calls.Add(1)
	return errors.New("brokers unreachable")
}

func TestConsumerRestartsAfterError(t *testing.T) {
	src := &failing{}
	h := newHub(src)
	defer h.Close()
	s, _ := h.Subscribe(1)
	defer s.Close()
	eventually(t, func() bool { return src.calls.Load() >= 2 })
}

func TestKafkaSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	env, err := testenv.StartWith(ctx, testenv.Options{Kafka: true})
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal(err)
		}
		t.Skipf("Docker: %v", err)
	}
	defer env.Close()
	producer, err := kgo.NewClient(kgo.SeedBrokers(env.KafkaBrokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := kafka.EnsureTopics(ctx, producer, 1, []kafka.Topic{{Name: kafka.TopicEvents, Partitions: 3, Retention: time.Hour}}); err != nil {
		t.Fatal(err)
	}
	produce := func(r rec) {
		res := producer.ProduceSync(ctx, &kgo.Record{Topic: kafka.TopicEvents, Value: r.value,
			Headers: []kgo.RecordHeader{{Key: "project_id", Value: []byte(strconv.Itoa(int(r.pid)))}}})
		if err := res.FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce(record(42, 1, "before_subscribe")) // a stream starts from now

	h := NewHub(KafkaSource{Brokers: env.KafkaBrokers}, slog.New(slog.DiscardHandler))
	defer h.Close()
	s, err := h.Subscribe(42)
	if err != nil {
		t.Fatal(err)
	}
	// the consumer finds the end of the partitions asynchronously: produce until it reads
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case ev := <-s.C:
			if ev.Name == "before_subscribe" {
				t.Fatal("an event from before the subscription")
			}
			if ev.Name == "message" && ev.UserID == "7" {
				return
			}
		case <-tick.C:
			produce(record(43, 1, "other_project"))
			produce(record(42, 7, "message"))
		case <-deadline:
			t.Fatal("no event from Kafka")
		}
	}
}
