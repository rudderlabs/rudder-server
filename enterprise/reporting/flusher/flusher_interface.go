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

	// Step 1. Get the time range to flush
	getTimeRangeToFlush(ctx context.Context) (minReportedAt, maxReportedAt time.Time)

	calculateMaxReportedAt(minReportedAt time.Time) time.Time

	// Step 2. Get the reports in batches and aggregate them in the app
	getReportsInBatchesAndAggregateInApp(ctx context.Context, minReportedAt, maxReportedAt time.Time) map[string]interface{}

	// This function will be overidden by actual Flusher. TrackedUserFlusher will do the merging HLLs here. MetricsFlusher will do add the counts here.
	addReportToAggregate(aggregatedReports map[string]interface{}, report map[string]interface{}) error

	getLabelKeyForAggregation(report map[string]interface{}) string

	// Step 3. Flush the aggregated reports to the reporting service
	flushAggregatedReports(ctx context.Context, aggregatedReports map[string]interface{}) error

	// Step 4. Delete the flushed reports from the database
	deleteReports(ctx context.Context, minReportedAt, maxReportedAt time.Time) error
}
