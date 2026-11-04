package live

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/kafka"
)

// KafkaSource reads events.raw directly, without a consumer group and without commits:
// a live stream starts from "now", there is nothing to resume.
type KafkaSource struct {
	Brokers []string
	Topic   string // default events.raw
}

func (k KafkaSource) Run(ctx context.Context, deliver func(projectID uint32, value []byte)) error {
	topic := k.Topic
	if topic == "" {
		topic = kafka.TopicEvents
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.Brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond), // the dashboard waits for each event
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return nil
		}
		// fetch errors are retried by the client; the stream just goes on
		fetches.EachRecord(func(r *kgo.Record) {
			if pid, ok := projectOf(r); ok {
				deliver(pid, r.Value)
			}
		})
	}
}

// projectOf reads the project_id header that the collector puts on every record.
func projectOf(r *kgo.Record) (uint32, bool) {
	for _, h := range r.Headers {
		if h.Key == "project_id" {
			n, err := strconv.ParseUint(string(h.Value), 10, 32)
			return uint32(n), err == nil
		}
	}
	return 0, false
}
