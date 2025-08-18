package reporting

import (
	"strings"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/types"
)

// mergeMetricGroupsByErrorSimilarity merges metrics by LCS similarity on error message
// Returns a map where key is "connectionKey::errorKey" and value is the grouped metrics
func (edr *ErrorDetailReporter) mergeMetricGroupsByErrorMessage(metricGroups map[string][]*types.PUReportedMetric) map[string][]*types.PUReportedMetric {
	for groupKey, metrics := range metricGroups {
		metricsGroupedByError := lo.GroupBy(metrics, func(metric *types.PUReportedMetric) string {
			return metric.StatusDetail.ErrorDetails.Message
		})

		mergedMetricsGroups := make(map[string]*types.PUReportedMetric)
		for errorMsg, metrics := range metricsGroupedByError {
			mergedMetricsGroups[errorMsg] = metrics[0]
			mergedMetricsGroups[errorMsg].StatusDetail.Count = lo.SumBy(metrics, func(metric *types.PUReportedMetric) int64 {
				return metric.StatusDetail.Count
			})
		}
		metricGroups[groupKey] = lo.Values(mergedMetricsGroups)
	}
	return metricGroups
}

// groupMetricsByConnection groups metrics by source, destination, PU, and event type
func (edr *ErrorDetailReporter) groupByConnection(metrics []*types.PUReportedMetric) map[string][]*types.PUReportedMetric {
	groups := make(map[string][]*types.PUReportedMetric)

	for _, metric := range metrics {
		key := edr.generateMetricGroupKey(metric)
		groups[key] = append(groups[key], metric)
	}

	return groups
}

// generateMetricGroupKey creates a unique key for connection details
func (edr *ErrorDetailReporter) generateMetricGroupKey(metric *types.PUReportedMetric) string {
	keys := []string{
		metric.SourceID,
		metric.DestinationID,
		metric.PU,
		metric.StatusDetail.EventType,
	}
	return strings.Join(keys, "::")
}
