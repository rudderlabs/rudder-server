package multitenant

import (
	"time"

	"github.com/rudderlabs/rudder-server/utils/misc"
)

var NOOP = &noop{}

type noop struct{}

func (*noop) CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool) {
}

func (*noop) AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
}

func (*noop) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	return map[string]int{
		"0": jobQueryBatchSize,
	}, map[string]float64{}
}

func (*noop) RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
}

func (*noop) ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
}
