package traces

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/jobsdb"
)

type options struct {
	tags stats.Tags
}

type Option func(*options)

func WithTags(tags stats.Tags) Option {
	return func(o *options) { o.tags = tags }
}

type SpanRecorder interface {
	RecordSpan(traceParent, name string, kind stats.SpanKind, start time.Time, opts ...Option)
	RecordSpans(contexts []context.Context, name string, kind stats.SpanKind, start time.Time, opts ...Option)
	RecordJobsSpans(ctx context.Context, jobs []*jobsdb.JobT, name string, kind stats.SpanKind, start time.Time, opts ...Option)
}

type spanRecorderImpl struct {
	tracer stats.Tracer
}

// TODO ideally we want this to live in the stats package of the go-kit and we want it to do NOTHING if tracing is not
// enabled in the first place

func NewSpanRecorder(tracer stats.Tracer) SpanRecorder {
	return &spanRecorderImpl{tracer: tracer}
}

func (s *spanRecorderImpl) RecordSpan(traceParent, name string, kind stats.SpanKind, start time.Time, opts ...Option) {
	ctx := stats.InjectTraceParentIntoContext(context.Background(), traceParent)
	var options options
	for _, opt := range opts {
		opt(&options)
	}
	so := []stats.SpanOption{stats.SpanWithTimestamp(start)}
	if len(options.tags) > 0 {
		so = append(so, stats.SpanWithTags(options.tags))
	}
	_, span := s.tracer.Start(ctx, name, kind, so...)
	span.End()
}

func (s *spanRecorderImpl) RecordSpans(
	contexts []context.Context, name string, kind stats.SpanKind, start time.Time, opts ...Option,
) {
	var options options
	for _, opt := range opts {
		opt(&options)
	}
	for _, ctx := range contexts {
		so := []stats.SpanOption{stats.SpanWithTimestamp(start)}
		if len(options.tags) > 0 {
			so = append(so, stats.SpanWithTags(options.tags))
		}
		_, span := s.tracer.Start(ctx, name, kind, so...)
		span.End()
	}
}

func (s *spanRecorderImpl) RecordJobsSpans(
	ctx context.Context,
	jobs []*jobsdb.JobT, name string, kind stats.SpanKind, start time.Time, opts ...Option,
) {
	seen := make(map[string]context.Context)
	for _, job := range jobs {
		traceParent := job.EventParameters.TraceParent
		if traceParent == "" {
			continue
		}
		spanCtx, ok := seen[traceParent]
		if !ok {
			spanCtx = stats.InjectTraceParentIntoContext(ctx, traceParent)
			seen[traceParent] = spanCtx
		}
	}

	if len(seen) == 0 {
		return
	}

	var options options
	for _, opt := range opts {
		opt(&options)
	}
	so := []stats.SpanOption{stats.SpanWithTimestamp(start)}
	if len(options.tags) > 0 {
		so = append(so, stats.SpanWithTags(options.tags))
	}

	for _, ctx := range seen {
		_, span := s.tracer.Start(ctx, name, kind, so...)
		span.End()
	}
}

type NOP struct{}

func (n *NOP) RecordSpan(_, _ string, _ stats.SpanKind, _ time.Time, _ ...Option) {
}

func (n *NOP) RecordSpans(_ []context.Context, _ string, _ stats.SpanKind, _ time.Time, _ ...Option) {
}

func (n *NOP) RecordJobsSpans(_ context.Context, _ []*jobsdb.JobT, _ string, _ stats.SpanKind, _ time.Time, _ ...Option) {
}
