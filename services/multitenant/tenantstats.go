package multitenant

import (
	"math"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var pkgLogger logger.LoggerI
var multitenantStat MultitenantStatsT

type MultitenantStatsT struct {
	RouterInMemoryJobCounts map[string]map[string]map[string]int
	routerJobCountMutex     sync.RWMutex
	RouterInputRates        map[string]map[string]map[string]misc.MovingAverage
}

func Init() {
	multitenantStat = MultitenantStatsT{}
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
	multitenantStat.RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	multitenantStat.RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	go SendRouterInMovingAverageStat()
	go SendPileUpStats()
}

func SendPileUpStats() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		for customer, value := range multitenantStat.RouterInMemoryJobCounts["router"] {
			for destType, count := range value {
				countStat := stats.NewTaggedStat("pile_up_count", stats.GaugeType, stats.Tags{
					"customer": customer,
					"destType": destType,
				})
				countStat.Gauge(count)
				pkgLogger.Infof("pile_up_count is %v for customer %v", count, customer)
			}
		}
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func SendRouterInMovingAverageStat() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		for customer, value := range multitenantStat.RouterInputRates["router"] {
			for destType, count := range value {
				movingAverageStat := stats.NewTaggedStat("router_input_rate", stats.GaugeType, stats.Tags{
					"customer": customer,
					"destType": destType,
				})
				movingAverageStat.Gauge(count.Value())
				pkgLogger.Infof("router_input_rate is %.8f for customer %v", count.Value(), customer)
			}
		}
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += count
}

func RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.RouterInMemoryJobCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.RouterInMemoryJobCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += -1 * count
}

func ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	for key := range stats {
		multitenantStat.routerJobCountMutex.RLock()
		_, ok := multitenantStat.RouterInputRates[tableType][key]
		if !ok {
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.routerJobCountMutex.Lock()
			multitenantStat.RouterInputRates[tableType][key] = make(map[string]misc.MovingAverage)
			multitenantStat.routerJobCountMutex.Unlock()
			multitenantStat.routerJobCountMutex.RLock()
		}
		multitenantStat.routerJobCountMutex.RUnlock()
		for destType := range stats[key] {
			multitenantStat.routerJobCountMutex.RLock()
			_, ok := multitenantStat.RouterInputRates[tableType][key][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.RUnlock()
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][key][destType] = misc.NewMovingAverage()
				multitenantStat.routerJobCountMutex.Unlock()
				multitenantStat.routerJobCountMutex.RLock()
			}
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.RouterInputRates[tableType][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
			AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for customerKey := range multitenantStat.RouterInputRates[tableType] {
		_, ok := stats[customerKey]
		if !ok {
			for destType := range stats[customerKey] {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}

		for destType := range multitenantStat.RouterInputRates[tableType][customerKey] {
			_, ok := stats[customerKey][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.RouterInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}
	}
}

func GetRouterPickupJobs(destType string, earliestJobMap map[string]time.Time, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int) map[string]int {
	customerPickUpCount := make(map[string]int)
	runningTimeCounter := float64(noOfWorkers) * float64(routerTimeOut) / float64(time.Second)
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()
	runningJobCount := jobQueryBatchSize
	for _, customerKey := range sortedLatencyList {
		customerCountKey, ok := multitenantStat.RouterInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {
				timeRequired := 0.0
				//runningJobCount should be a number large enough to ensure fairness but small enough to not cause OOM Issues
				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						customerPickUpCount[customerKey] += 1
					}
					continue
				}
				if latencyMap[customerKey].Value() != 0 {
					timeRequired = float64(latencyMap[customerKey].Value() * destTypeCount.Value() * float64(routerTimeOut) / float64(time.Second))
					customerPickUpCount[customerKey] = int(math.Min(timeRequired, runningTimeCounter) / latencyMap[customerKey].Value())
					// if customerPickUpCount[customerKey] == 0 && multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
					// 	customerPickUpCount[customerKey] = customerPickUpCount[customerKey] + 1
					// }
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType], 0)
						timeRequired = latencyMap[customerKey].Value() * float64(customerPickUpCount[customerKey])
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount)
			}
		}
	}

	for _, customerKey := range sortedLatencyList {
		customerCountKey, ok := multitenantStat.RouterInMemoryJobCounts["router"][customerKey]
		if !ok {
			continue
		}
		if customerCountKey[destType] == 0 {
			continue
		}
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 && customerPickUpCount[customerKey] == 0 {
				customerPickUpCount[customerKey] += 1
			}
			continue
		}

		timeRequired := latencyMap[customerKey].Value() * float64(customerCountKey[destType])
		//TODO : Include earliestJobMap into the algorithm if required or get away with earliestJobMap
		if timeRequired < runningTimeCounter {
			if latencyMap[customerKey].Value() != 0 {
				pickUpCount := misc.MinInt(customerCountKey[destType]-customerPickUpCount[destType], runningJobCount)
				customerPickUpCount[customerKey] += pickUpCount
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - pickUpCount
			}
			// Migrated jobs fix in else condition
		} else {
			pickUpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			pileupPickUp := misc.MinInt(misc.MinInt(pickUpCount, runningJobCount), customerCountKey[destType]-customerPickUpCount[destType])
			customerPickUpCount[customerKey] += pileupPickUp
			runningJobCount = runningJobCount - pileupPickUp
			runningTimeCounter = runningTimeCounter - float64(pileupPickUp)*latencyMap[customerKey].Value()
		}
		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , PileUpLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount)
	}

	return customerPickUpCount
}
