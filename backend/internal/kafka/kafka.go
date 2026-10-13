// Package kafka holds the topics of the event bus (Botsight.md, 11.1) and client settings.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	TopicEvents  = "events.raw"
	TopicTexts   = "messages.text"
	TopicBatches = "ingest.batches"
	TopicDLQ     = "events.dlq"
)

// Topic is the desired configuration of a topic.
type Topic struct {
	Name       string
	Partitions int32
	Retention  time.Duration
}

// Topics are the defaults of Botsight.md, 11.1. Partitions bound how many
// ch-writer (and antifraud) instances can share the load.
func Topics() []Topic {
	return []Topic{
		{Name: TopicEvents, Partitions: 24, Retention: 72 * time.Hour},
		{Name: TopicTexts, Partitions: 12, Retention: 24 * time.Hour},
		{Name: TopicBatches, Partitions: 6, Retention: 72 * time.Hour},
		{Name: TopicDLQ, Partitions: 3, Retention: 7 * 24 * time.Hour},
	}
}

// EnsureTopics creates missing topics. Existing topics are left as they are: partition
// counts and retention in production are changed deliberately, not on service start.
func EnsureTopics(ctx context.Context, cl *kgo.Client, replication int16, topics []Topic) error {
	adm := kadm.NewClient(cl)
	for _, t := range topics {
		retention := strconv.FormatInt(t.Retention.Milliseconds(), 10)
		configs := map[string]*string{"retention.ms": &retention}
		if replication > 1 {
			minISR := strconv.Itoa(int(replication) - 1)
			configs["min.insync.replicas"] = &minISR
		}
		resp, err := adm.CreateTopic(ctx, t.Partitions, replication, configs, t.Name)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
	}
	return nil
}

// Brokers splits a comma-separated KAFKA_BROKERS value.
func Brokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// ProducerOptions: acks from all in-sync replicas and an idempotent producer (the
// franz-go default) so retries never duplicate records; zstd and a short linger
// batch records of many concurrent requests together (Botsight.md, 11.1).
func ProducerOptions() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(1 << 20),
		kgo.MaxBufferedBytes(256 << 20),
		kgo.RecordDeliveryTimeout(10 * time.Second),
	}
}
