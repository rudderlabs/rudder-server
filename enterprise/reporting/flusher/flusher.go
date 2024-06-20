package flusher

import (
	"context"
	"time"
)

type Flusher struct {
	inAppAggregator InAppAggregator
}

func (f *Flusher) Start(ctx context.Context) {

}

func (f *Flusher) Stop() {

}

func (f *Flusher) initializeStats(tags map[string]string) {
}

func (f *Flusher) initCommonTags() {

}

func (f *Flusher) emitLagMetricLoop(ctx context.Context) error {

	return nil
}

func (f *Flusher) mainLoop(ctx context.Context) error {

	return nil
}

func (f *Flusher) mainLoopOnce(ctx context.Context) error {

	return nil
}

// Step 1. Get the time range to flush
func (f *Flusher) getTimeRangeToFlush(ctx context.Context) (minReportedAt, maxReportedAt time.Time) {

	return time.Time{}, time.Time{}
}

func (f *Flusher) calculateMaxReportedAt(minReportedAt time.Time) time.Time {

	return time.Time{}
}

// Step 2. Get the reports in batches and aggregate them in the app
func (f *Flusher) getReportsInBatchesAndAggregateInApp(ctx context.Context, minReportedAt, maxReportedAt time.Time) map[string]interface{} {
	// Use inAppAggregator.AddReportToAggregate()
	return nil
}

func (f *Flusher) getLabelKeyForAggregation(report map[string]interface{}) string {

	return ""
}

// Step 3. Flush the aggregated reports to the reporting service
func (f *Flusher) flushAggregatedReports(ctx context.Context, aggregatedReports map[string]interface{}) error {

	return nil
}

// Step 4. Delete the flushed reports from the database
func (f *Flusher) deleteReports(ctx context.Context, minReportedAt, maxReportedAt time.Time) error {

	return nil
}
