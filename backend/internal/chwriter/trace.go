package chwriter

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Artem-Kornilov-pro/Botsight/backend/internal/tracing"
)

// maxTracedBatches bounds the spans of one chunk; with sampling there are far fewer.
const maxTracedBatches = 256

// chunkTrace continues the traces of the sampled batches in a chunk (Botsight.md, 17.3).
// A batch gets one "deliver <table>" span per chunk, a child of the collector's span: it starts
// when the batch's records were written to Kafka and ends when its rows are in ClickHouse
// and the offset is committed, so its length is the time from intake to queryable data.
// Events mark when the chunk was taken from the buffer, inserted and committed. One insert
// carries many batches, so the chunk has no span of its own; sampling stays the
// collector's decision, and an unsampled batch costs a header lookup per record.
type chunkTrace struct {
	spans []trace.Span
	done  bool
}

func (pw *partitionWorker) startTrace(chunk []*kgo.Record, token string) *chunkTrace {
	tracer := pw.w.tracer
	if tracer == nil {
		return nil
	}
	type batch struct {
		parent []byte
		first  time.Time
		rows   int
	}
	var batches []batch
	var index map[string]int
	for _, r := range chunk {
		tp := tracing.TraceParent(r.Headers)
		if tp == nil {
			continue
		}
		i, ok := index[string(tp)]
		if !ok {
			if len(batches) == maxTracedBatches {
				continue
			}
			if index == nil {
				index = map[string]int{}
			}
			i = len(batches)
			index[string(tp)] = i
			batches = append(batches, batch{parent: tp, first: r.Timestamp})
		}
		b := &batches[i]
		b.rows++
		if r.Timestamp.Before(b.first) {
			b.first = r.Timestamp
		}
	}
	if len(batches) == 0 {
		return nil
	}
	ct := &chunkTrace{spans: make([]trace.Span, 0, len(batches))}
	common := []attribute.KeyValue{
		attribute.String("messaging.destination.name", pw.topic),
		attribute.String("messaging.destination.partition.id", strconv.Itoa(int(pw.partition))),
		attribute.String("db.collection.name", pw.sink.Table),
		attribute.Int("botsight.chunk_rows", len(chunk)),
		attribute.String("botsight.insert_token", token),
	}
	taken := time.Now()
	for _, b := range batches {
		parent := tracing.Extract(context.Background(), []kgo.RecordHeader{{Key: tracing.HeaderKey, Value: b.parent}})
		_, span := tracer.Start(parent, "deliver "+pw.sink.Table, trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithTimestamp(b.first), trace.WithAttributes(common...),
			trace.WithAttributes(attribute.Int("botsight.batch_rows", b.rows)))
		span.AddEvent("chunk taken", trace.WithTimestamp(taken))
		ct.spans = append(ct.spans, span)
	}
	return ct
}

func (ct *chunkTrace) event(name string) {
	if ct == nil {
		return
	}
	now := time.Now()
	for _, s := range ct.spans {
		s.AddEvent(name, trace.WithTimestamp(now))
	}
}

// committed ends the spans: the batch is in ClickHouse and will not be read again.
func (ct *chunkTrace) committed() {
	if ct == nil {
		return
	}
	ct.event("offset committed")
	for _, s := range ct.spans {
		s.End()
	}
	ct.done = true
}

// abandon ends the spans of a chunk that was not committed (revoked, shutting down or
// crashed): its records are read again, and the next attempt gets new spans.
func (ct *chunkTrace) abandon() {
	if ct == nil || ct.done {
		return
	}
	for _, s := range ct.spans {
		s.SetStatus(codes.Error, "not committed, the records will be read again")
		s.End()
	}
}
