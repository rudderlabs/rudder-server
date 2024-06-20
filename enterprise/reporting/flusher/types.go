package flusher

type InAppAggregator interface {
	AddReportToAggregate(aggregatedReports map[string]interface{}, report map[string]interface{}) error
}
