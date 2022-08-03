package multitenant

import "time"

var NOOP = &noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(_, _ string, _, _ bool)         {}
func (*noop) AddToInMemoryCount(_, _ string, _ int, _ string)              {}
func (*noop) RemoveFromInMemoryCount(_, _ string, _ int, _ string)         {}
func (*noop) ReportProcLoopAddStats(_ map[string]map[string]int, _ string) {}
func (*noop) AddWorkspaceToLatencyMap(_, _ string)                         {}
func (*noop) UpdateWorkspaceLatencyMap(_, _ string, _ float64)             {}
func (*noop) Start() error                                                 { return nil }
func (*noop) Stop()                                                        {}
func (*noop) Status() map[string]map[string]map[string]int {
	return map[string]map[string]map[string]int{}
}

func (*noop) GetRouterPickupJobs(_ string, _ int, _ time.Duration, jobQueryBatchSize int, _ float64) (
	map[string]int, map[string]float64,
) {
	return map[string]int{"0": jobQueryBatchSize}, map[string]float64{}
}
