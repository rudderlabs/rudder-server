package flusher

import (
	"context"
	"time"
)

type Flusher interface {
	Start(ctx context.Context)

	Stop()

	initializeStats(tags map[string]string)

	initCommonTags()

	emitLagMetricLoop(ctx context.Context) error

	mainLoop(ctx context.Context) error

	mainLoopOnce(ctx context.Context) error

	getTimeRangeToFlush(ctx context.Context) (minReportedAt, maxReportedAt time.Time)

	calculateMaxReportedAt(minReportedAt time.Time) time.Time

	getReportsInBatchesAndAggregateInApp(ctx context.Context, minReportedAt, maxReportedAt time.Time) map[string]interface{}

	addReportToAggregate(aggregatedReports map[string]interface{}, report map[string]interface{}) error

	getLabelKeyForAggregation(report map[string]interface{}) string

	flushAggregatedReports(ctx context.Context, aggregatedReports map[string]interface{}) error

	deleteReports(ctx context.Context, minReportedAt, maxReportedAt time.Time) error
}
