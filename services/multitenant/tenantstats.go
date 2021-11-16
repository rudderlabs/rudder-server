package multitenant

import (
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jobQueryBatchSize int
var RouterInMemoryJobCounts map[string]map[string]map[string]int
var routerJobCountMutex sync.RWMutex
var ProcessorJobsMovingAverages map[string]map[string]map[string]misc.MovingAverage
var pkgLogger logger.LoggerI

func Init() {
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
	RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	ProcessorJobsMovingAverages = make(map[string]map[string]map[string]misc.MovingAverage)
	ProcessorJobsMovingAverages["router"] = make(map[string]map[string]misc.MovingAverage)
	ProcessorJobsMovingAverages["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	config.RegisterIntConfigVariable(10000, &jobQueryBatchSize, true, 1, "Router.jobQueryBatchSize")
}

func AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	routerJobCountMutex.RLock()
	_, ok := RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		routerJobCountMutex.RUnlock()
		routerJobCountMutex.Lock()
		RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		routerJobCountMutex.Unlock()
		routerJobCountMutex.RLock()
	}
	routerJobCountMutex.RUnlock()
	RouterInMemoryJobCounts[tableType][customerID][destinationType] += count
}

func RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	routerJobCountMutex.RLock()
	_, ok := RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		routerJobCountMutex.RUnlock()
		routerJobCountMutex.Lock()
		RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		routerJobCountMutex.Unlock()
		routerJobCountMutex.RLock()
	}
	routerJobCountMutex.RUnlock()
	RouterInMemoryJobCounts[tableType][customerID][destinationType] += -1 * count
}

func ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	for key := range stats {
		routerJobCountMutex.RLock()
		_, ok := ProcessorJobsMovingAverages[tableType][key]
		if !ok {
			routerJobCountMutex.RUnlock()
			routerJobCountMutex.Lock()
			ProcessorJobsMovingAverages[tableType][key] = make(map[string]misc.MovingAverage)
			routerJobCountMutex.Unlock()
			routerJobCountMutex.RLock()
		}
		routerJobCountMutex.RUnlock()
		for destType := range stats[key] {
			routerJobCountMutex.RLock()
			_, ok := ProcessorJobsMovingAverages[tableType][key][destType]
			if !ok {
				routerJobCountMutex.RUnlock()
				routerJobCountMutex.Lock()
				ProcessorJobsMovingAverages[tableType][key][destType] = misc.NewMovingAverage()
				routerJobCountMutex.Unlock()
				routerJobCountMutex.RLock()
			}
			routerJobCountMutex.RUnlock()
			ProcessorJobsMovingAverages[tableType][key][destType].Add(float64(stats[key][destType]) * float64(time.Second) / float64(timeTaken))
			AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for customerKey := range ProcessorJobsMovingAverages[tableType] {
		_, ok := stats[customerKey]
		if !ok {
			for destType := range stats[customerKey] {
				routerJobCountMutex.Lock()
				ProcessorJobsMovingAverages[tableType][customerKey][destType].Add(0)
				routerJobCountMutex.Unlock()
			}
		}

		for destType := range ProcessorJobsMovingAverages[tableType][customerKey] {
			_, ok := stats[customerKey][destType]
			if !ok {
				routerJobCountMutex.Lock()
				ProcessorJobsMovingAverages[tableType][customerKey][destType].Add(0)
				routerJobCountMutex.Unlock()
			}
		}
	}
}

func GetRouterPickupJobs(destType string, earliestJobMap map[string]time.Time) map[string]int {
	customerLiveCount := make(map[string]float64)
	customerPickUpCount := make(map[string]int)
	totalCount := 0.0
	runningCounter := jobQueryBatchSize
	routerJobCountMutex.RLock()
	defer routerJobCountMutex.RUnlock()
	for customerKey := range ProcessorJobsMovingAverages["router"] {
		customerLiveCount[customerKey] = ProcessorJobsMovingAverages["router"][customerKey][destType].Value()
		totalCount += customerLiveCount[customerKey]
	}

	for customerKey := range ProcessorJobsMovingAverages["router"] {
		customerPickUpCount[customerKey] = int(float64(jobQueryBatchSize)*(customerLiveCount[customerKey]/totalCount)) + 1
		/// Need to add a check if the current workspaceID is part of Active Configuration
		if customerPickUpCount[customerKey] > RouterInMemoryJobCounts["router"][customerKey][destType] {
			customerPickUpCount[customerKey] = RouterInMemoryJobCounts["router"][customerKey][destType]
		}
		runningCounter = runningCounter - customerPickUpCount[customerKey]
	}
	if runningCounter <= 0 {
		return customerPickUpCount
	}
	totalCount = 0.0
	for customerKey := range RouterInMemoryJobCounts["router"] {
		totalCount += float64(int(time.Second)*RouterInMemoryJobCounts["router"][customerKey][destType]) / float64(time.Since(earliestJobMap[customerKey]))
	}
	for customerKey := range RouterInMemoryJobCounts["router"] {
		customerPickUpCount[customerKey] += int(float64(runningCounter)*(customerLiveCount[customerKey]/totalCount)) + 1
		/// Need to add a check if the current workspaceID is part of Active Configuration
	}
	return customerPickUpCount
}
