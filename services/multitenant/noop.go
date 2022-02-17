package multitenant

import (
	"time"
)

var NOOP = &noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool) {
}

func (*noop) AddToInMemoryCount(workspaceID string, destinationType string, count int, tableType string) {
}

func (*noop) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}

func (*noop) RemoveFromInMemoryCount(workspaceID string, destinationType string, count int, tableType string) {
}

func (*noop) ReportProcLoopAddStats(stats map[string]map[string]int, tableType string) {
}

func (*noop) AddWorkspaceToLatencyMap(destType string, workspaceID string) {

}

func (*noop) UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64) {

}
