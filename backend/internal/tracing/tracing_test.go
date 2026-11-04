package tracing

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaHeadersRoundTrip(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("t").Start(context.Background(), "batch")
	defer span.End()

	base := []kgo.RecordHeader{{Key: "project_id", Value: []byte("42")}}
	headers := Inject(ctx, base)
	if len(headers) != 2 || headers[0].Key != "project_id" || TraceParent(headers) == nil {
		t.Fatalf("headers: %+v", headers)
	}
	got := trace.SpanContextFromContext(Extract(context.Background(), headers))
	want := span.SpanContext()
	if got.TraceID() != want.TraceID() || got.SpanID() != want.SpanID() || !got.IsRemote() || !got.IsSampled() {
		t.Fatalf("extracted %v, want %v", got, want)
	}
}

func TestUnsampledBatchCarriesNothing(t *testing.T) {
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
	ctx, span := tp.Tracer("t").Start(context.Background(), "batch")
	defer span.End()
	if h := Inject(ctx, nil); len(h) != 0 {
		t.Fatalf("unsampled span injected: %+v", h)
	}
	if h := Inject(context.Background(), nil); len(h) != 0 {
		t.Fatal("no span, but a header")
	}
	if trace.SpanContextFromContext(Extract(context.Background(), nil)).IsValid() {
		t.Fatal("span context from no headers")
	}
}

func TestSetupWithoutEndpointIsNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	tp, shutdown, err := Setup(context.Background(), "test")
	if err != nil || Enabled(tp) || shutdown(context.Background()) != nil {
		t.Fatalf("tp %T, err %v", tp, err)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	tp, shutdown, err = Setup(context.Background(), "test")
	if err != nil || !Enabled(tp) {
		t.Fatalf("tp %T, err %v", tp, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = shutdown(ctx)
}

func TestLogHandlerAddsTraceID(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(LogHandler(slog.NewJSONHandler(&buf, nil))).With("service", "x")
	ctx, span := sdktrace.NewTracerProvider().Tracer("t").Start(context.Background(), "batch")
	defer span.End()

	log.ErrorContext(ctx, "kafka produce failed")
	if !strings.Contains(buf.String(), `"trace_id":"`+span.SpanContext().TraceID().String()+`"`) || !strings.Contains(buf.String(), `"service":"x"`) {
		t.Fatalf("log line: %s", buf.String())
	}
	buf.Reset()
	log.Error("no context")
	if strings.Contains(buf.String(), "trace_id") {
		t.Fatalf("trace_id without a span: %s", buf.String())
	}
}
