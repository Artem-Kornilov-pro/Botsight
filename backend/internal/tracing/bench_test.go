package tracing

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// BenchmarkBatchSpans is what tracing adds to one /v1/batch in the collector: the batch
// span, three child spans and the traceparent header. Compare with BenchmarkHotPath
// (about 900 µs per batch of 500 events) to see the share.
func BenchmarkBatchSpans(b *testing.B) {
	for _, c := range []struct {
		name string
		tp   trace.TracerProvider
	}{
		{"off", noop.NewTracerProvider()},
		{"unsampled", sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))},
		// sampled spans go to a processor that drops them: the cost of recording, not of export
		{"sampled", sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(dropProcessor{}))},
	} {
		b.Run(c.name, func(b *testing.B) {
			tracer := c.tp.Tracer("bench")
			b.ReportAllocs()
			for b.Loop() {
				ctx, span := tracer.Start(context.Background(), "POST /v1/batch", trace.WithSpanKind(trace.SpanKindServer))
				for _, name := range []string{"redis gate", "build records", "kafka produce"} {
					_, s := tracer.Start(ctx, name)
					s.End()
				}
				_ = Inject(ctx, make([]kgo.RecordHeader, 2, 3))
				span.End()
			}
		})
	}
}

type dropProcessor struct{}

func (dropProcessor) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (dropProcessor) OnEnd(sdktrace.ReadOnlySpan)                     {}
func (dropProcessor) Shutdown(context.Context) error                  { return nil }
func (dropProcessor) ForceFlush(context.Context) error                { return nil }
