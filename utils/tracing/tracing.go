package tracing

import (
	"context"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

type Option func(*Tracer)

func WithNamePrefix(namePrefix string) Option {
	return func(t *Tracer) {
		if len(namePrefix) > 0 {
			namePrefix += "."
		}
		t.namePrefix = namePrefix
	}
}

type traceConfig struct {
	timestamp time.Time
	tags      stats.Tags
	kind      stats.SpanKind
}

type TraceOption func(*traceConfig)

func WithTraceStart(t time.Time) TraceOption {
	return func(c *traceConfig) { c.timestamp = t }
}

func WithTraceTags(tags stats.Tags) TraceOption {
	return func(c *traceConfig) { c.tags = tags }
}

func WithTraceKind(kind stats.SpanKind) TraceOption {
	return func(c *traceConfig) { c.kind = kind }
}

type RecordSpanOption func(*traceConfig)

func WithRecordSpanTags(tags stats.Tags) RecordSpanOption {
	return func(c *traceConfig) { c.tags = tags }
}

func WithRecordSpanKind(kind stats.SpanKind) RecordSpanOption {
	return func(c *traceConfig) { c.kind = kind }
}

type Tracer struct {
	tracer     stats.Tracer
	namePrefix string
}

func New(tracer stats.Tracer, opts ...Option) *Tracer {
	if tracer == nil {
		tracer = stats.NOP.NewTracer("")
	}
	t := &Tracer{tracer: tracer}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func (t *Tracer) Trace(ctx context.Context, name string, opts ...TraceOption) (context.Context, stats.TraceSpan) {
	kind, options := t.getOptions(opts...)
	return t.tracer.Start(ctx, t.namePrefix+name, kind, options...)
}

func (t *Tracer) TraceFunc(ctx context.Context, name string, f func(ctx context.Context), opts ...TraceOption) context.Context {
	kind, options := t.getOptions(opts...)
	newCtx, span := t.tracer.Start(ctx, t.namePrefix+name, kind, options...)
	f(ctx)
	span.End()
	return newCtx
}

func (t *Tracer) RecordSpan(ctx context.Context, name string, start time.Time, opts ...RecordSpanOption) {
	if start.IsZero() {
		panic(fmt.Errorf("start time must be set when using RecordSpan: %q", name))
	}
	tc := traceConfig{
		kind: stats.SpanKindInternal,
	}
	for _, opt := range opts {
		opt(&tc)
	}
	spanOptions := make([]stats.SpanOption, 0, 2)
	spanOptions = append(spanOptions, stats.SpanWithTimestamp(start))
	if len(tc.tags) > 0 {
		spanOptions = append(spanOptions, stats.SpanWithTags(tc.tags))
	}
	_, span := t.tracer.Start(ctx, t.namePrefix+name, tc.kind, spanOptions...)
	span.End()
}

func (t *Tracer) getOptions(opts ...TraceOption) (stats.SpanKind, []stats.SpanOption) {
	tc := traceConfig{
		kind: stats.SpanKindInternal,
	}
	for _, opt := range opts {
		opt(&tc)
	}
	spanOptions := make([]stats.SpanOption, 0, 2)
	if !tc.timestamp.IsZero() {
		spanOptions = append(spanOptions, stats.SpanWithTimestamp(tc.timestamp))
	}
	if len(tc.tags) > 0 {
		spanOptions = append(spanOptions, stats.SpanWithTags(tc.tags))
	}
	return tc.kind, spanOptions
}
