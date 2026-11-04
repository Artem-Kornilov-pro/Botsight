// Package tracing sets up OpenTelemetry tracing and carries the trace context of a batch
// through Kafka (Botsight.md, 17.3): the collector starts a trace per batch, writes it to
// the headers of the batch's records, and ch-writer continues it up to the insert into
// ClickHouse, so one trace shows how long a batch took from the SDK to a query.
package tracing

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Setup returns the tracer provider of a service. Tracing is on when the standard
// OTEL_EXPORTER_OTLP_ENDPOINT (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set: spans go
// over OTLP/HTTP in batches. Sampling follows OTEL_TRACES_SAMPLER and
// OTEL_TRACES_SAMPLER_ARG, by default every trace. Without an endpoint the provider is a
// no-op and the hot path pays nothing. shutdown flushes the spans still buffered.
func Setup(ctx context.Context, service string) (tp trace.TracerProvider, shutdown func(context.Context) error, err error) {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" && os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		return noop.NewTracerProvider(), func(context.Context) error { return nil }, nil
	}
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", service), attribute.String("service.namespace", "botsight")),
		resource.WithFromEnv(), resource.WithTelemetrySDK(), resource.WithHost())
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return nil, nil, err
	}
	sdk := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp), sdktrace.WithResource(res))
	return sdk, sdk.Shutdown, nil
}

// Enabled reports whether tp records anything; services skip trace bookkeeping otherwise.
func Enabled(tp trace.TracerProvider) bool {
	_, isNoop := tp.(noop.TracerProvider)
	return tp != nil && !isNoop
}

// propagator is W3C Trace Context: the traceparent header, the same in HTTP and Kafka.
var propagator = propagation.TraceContext{}

// FromHTTP continues a trace the client started (an SDK may send traceparent).
func FromHTTP(ctx context.Context, h propagation.HeaderCarrier) context.Context {
	return propagator.Extract(ctx, h)
}

// HeaderKey is the Kafka record header with the trace context.
const HeaderKey = "traceparent"

// Inject appends the span context of ctx to Kafka record headers. Only sampled spans are
// written: records of an unsampled batch carry nothing, and ch-writer does no work for them.
func Inject(ctx context.Context, headers []kgo.RecordHeader) []kgo.RecordHeader {
	if !trace.SpanContextFromContext(ctx).IsSampled() {
		return headers
	}
	c := headerCarrier{&headers}
	propagator.Inject(ctx, c)
	return headers
}

// Extract returns ctx with the span context from Kafka record headers as the remote parent.
func Extract(ctx context.Context, headers []kgo.RecordHeader) context.Context {
	return propagator.Extract(ctx, headerCarrier{&headers})
}

// TraceParent is the raw traceparent header of a record, nil when there is none.
func TraceParent(headers []kgo.RecordHeader) []byte {
	for _, h := range headers {
		if h.Key == HeaderKey {
			return h.Value
		}
	}
	return nil
}

type headerCarrier struct{ h *[]kgo.RecordHeader }

func (c headerCarrier) Get(key string) string {
	for _, h := range *c.h {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, value string) {
	for i, h := range *c.h {
		if h.Key == key {
			(*c.h)[i].Value = []byte(value)
			return
		}
	}
	*c.h = append(*c.h, kgo.RecordHeader{Key: key, Value: []byte(value)})
}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(*c.h))
	for i, h := range *c.h {
		keys[i] = h.Key
	}
	return keys
}

// LogHandler adds trace_id and span_id to records logged with a context that holds a
// span, so a log line of a failed batch leads to its trace.
func LogHandler(h slog.Handler) slog.Handler { return logHandler{h} }

type logHandler struct{ slog.Handler }

func (l logHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return l.Handler.Handle(ctx, r)
}

func (l logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return logHandler{l.Handler.WithAttrs(attrs)}
}

func (l logHandler) WithGroup(name string) slog.Handler { return logHandler{l.Handler.WithGroup(name)} }
