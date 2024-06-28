package flusher

const (
	StatFlusherFlushTime = "flusher_flush_duration_seconds"

	StatFlusherGetMinReportedAtQueryTime = "flusher_get_min_reported_at_query_duration_seconds"

	StatFlusherGetReportsBatchQueryTime = "flusher_get_reports_batch_query_duration_seconds"
	StatFlusherGetReportsCount          = "flusher_get_reports_count"
	StatFluherGetAggregatedReportsCount = "flusher_get_aggregated_reports_count"
	StatFlusherGetAggregatedReportsTime = "flusher_get_aggregated_reports_duration_seconds"

	StatFlusherSendReportsTime    = "flusher_send_reports_duration_seconds"
	StatFlusherConcurrentRequests = "flusher_concurrent_requests_in_progress"

	StatFlusherDeleteReportsTime = "flusher_delete_reports_duration_seconds"

	StatFlusherLagInSeconds = "flusher_lag_seconds"
)
