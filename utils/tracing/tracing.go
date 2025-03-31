package tracing

import (
	"context"
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
}

type TraceOption func(*traceConfig)

func WithTimestamp(t time.Time) TraceOption {
	return func(c *traceConfig) { c.timestamp = t }
}

func WithTags(tags stats.Tags) TraceOption {
	return func(c *traceConfig) { c.tags = tags }
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

func (t *Tracer) Trace(ctx context.Context, name string, f func(ctx context.Context), opts ...TraceOption) (
	context.Context,
	func(),
) {
	var tc traceConfig
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
	newCtx, span := t.tracer.Start(ctx, t.namePrefix+name, stats.SpanKindInternal, spanOptions...)
	if f != nil {
		f(ctx)
	}
	return newCtx, span.End
}

func (t *Tracer) RecordSpan(ctx context.Context, name string, start time.Time, tags stats.Tags) {
	spanOptions := make([]stats.SpanOption, 0, 2)
	spanOptions = append(spanOptions, stats.SpanWithTimestamp(start))
	if len(tags) > 0 {
		spanOptions = append(spanOptions, stats.SpanWithTags(tags))
	}
	_, span := t.tracer.Start(ctx, t.namePrefix+name, stats.SpanKindInternal, spanOptions...)
	span.End()
}
