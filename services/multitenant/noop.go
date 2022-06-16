package multitenant

import (
	"time"
)

var NOOP = &noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(workspace, destType string, isSuccess, isDrained bool) {
}

func (*noop) AddToInMemoryCount(workspaceID, destinationType string, count int, tableType string) {
}

func (*noop) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}

func (*noop) RemoveFromInMemoryCount(workspaceID, destinationType string, count int, tableType string) {
}

func (*noop) ReportProcLoopAddStats(stats map[string]map[string]int, tableType string) {
}

func (*noop) AddWorkspaceToLatencyMap(destType, workspaceID string) {
}

func (*noop) UpdateWorkspaceLatencyMap(destType, workspaceID string, val float64) {
}

func (*noop) Status() map[string]map[string]map[string]int {
	return map[string]map[string]map[string]int{}
}

func (*noop) Start() {
}

func (*noop) Stop() {
}
