package flusher

const (
	StatReportingMainLoopTime = "reporting_client_main_loop_time"

	StatReportingGetMinReportedAtQueryTime = "reporting_client_get_min_reported_at_query_time"
	StatReportingGetReportsBatchQueryTime  = "reporting_client_get_reports_batch_query_time"

	StatReportingGetReportsTime  = "reporting_client_get_reports_time"
	StatReportingGetReportsCount = "reporting_client_get_reports_count"

	StatReportingGetAggregatedReportsTime  = "reporting_client_get_aggregated_reports_time"
	StatReportingGetAggregatedReportsCount = "reporting_client_get_aggregated_reports_count"

	StatReportingSendReportsTime    = "reporting_send_reports_time"
	StatReportingHttpReqLatency     = "reporting_client_http_request_latency"
	StatReportingHttpReqCount       = "reporting_client_http_request_count"
	StatReportingConcurrentRequests = "reporting_client_concurrent_requests"

	StatReportingDeleteReportsTime = "reporting_delete_reports_time"

	StatReportingMetricsLagInSeconds = "reporting_metrics_lag_seconds"
)
