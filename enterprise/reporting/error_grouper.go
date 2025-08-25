package reporting

import (
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-server/utils/types"
)

func (edr *ErrorDetailReporter) mergeMetricGroupsByErrorMessage(metricGroups map[types.ErrorDetailGroupKey][]*types.EDReportsDB) map[types.ErrorDetailGroupKey][]*types.EDReportsDB {
	for groupKey, metrics := range metricGroups {
		metricsGroupedByError := lo.GroupBy(metrics, func(metric *types.EDReportsDB) string {
			return metric.ErrorMessage
		})

		mergedMetricsGroups := make(map[string]*types.EDReportsDB)
		for errorMsg, metrics := range metricsGroupedByError {
			mergedMetricsGroups[errorMsg] = metrics[0]
			mergedMetricsGroups[errorMsg].Count = lo.SumBy(metrics, func(metric *types.EDReportsDB) int64 {
				return metric.Count
			})
		}
		metricGroups[groupKey] = lo.Values(mergedMetricsGroups)
	}
	return metricGroups
}

// groupMetricsByConnection groups metrics by source, destination, PU, and event type
func (edr *ErrorDetailReporter) groupByConnection(metrics []*types.EDReportsDB) map[types.ErrorDetailGroupKey][]*types.EDReportsDB {
	groups := make(map[types.ErrorDetailGroupKey][]*types.EDReportsDB)

	for _, metric := range metrics {
		key := edr.generateMetricGroupKey(metric)
		groups[key] = append(groups[key], metric)
	}

	return groups
}

// generateMetricGroupKey creates a unique key for connection details
func (edr *ErrorDetailReporter) generateMetricGroupKey(metric *types.EDReportsDB) types.ErrorDetailGroupKey {
	return types.ErrorDetailGroupKey{
		SourceID:      metric.SourceID,
		DestinationID: metric.DestinationID,
		PU:            metric.PU,
		EventType:     metric.EventType,
	}
}
