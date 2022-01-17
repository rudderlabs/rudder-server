package multitenant

import (
	"math"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	pkgLogger       logger.LoggerI
	multitenantStat MultitenantStatsT
	minBackOff      time.Duration
	maxBackOff      time.Duration
	backOffFactor   float64
)

type MultitenantStatsT struct {
	RouterInMemoryJobCounts     map[string]map[string]map[string]int
	routerJobCountMutex         sync.RWMutex
	RouterInputRates            map[string]map[string]map[string]misc.MovingAverage
	RouterSuccessRatioLoopCount map[string]map[string]map[string]int
	lastDrainedTimestamps       map[string]map[string]time.Time
	RouterCircuitBreakerMap     map[string]map[string]BackOffT
	routerSuccessRateMutex      sync.RWMutex
}

type BackOffT struct {
	backOff     *backoff.Backoff
	timeToRetry time.Time
}

func Init() {
	multitenantStat = MultitenantStatsT{}
	config.RegisterDurationConfigVariable(time.Duration(10), &minBackOff, false, time.Second, "tenantStats.minBackOff")
	config.RegisterDurationConfigVariable(time.Duration(300), &maxBackOff, false, time.Second, "tenantStats.maxBackOff")
	config.RegisterFloat64ConfigVariable(1.5, &backOffFactor, false, "tenantStats.backOffFactor")
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
	multitenantStat.RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	multitenantStat.RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterSuccessRatioLoopCount = make(map[string]map[string]map[string]int)
	multitenantStat.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	multitenantStat.RouterCircuitBreakerMap = make(map[string]map[string]BackOffT)
	go SendRouterInMovingAverageStat()
	go SendPileUpStats()
}

func SendPileUpStats() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		totalPileUp := 0
		for _, value := range multitenantStat.RouterInMemoryJobCounts["router"] {
			for _, count := range value {
				totalPileUp = totalPileUp + count
			}
		}
		countStat := stats.NewTaggedStat("pile_up_count", stats.GaugeType, stats.Tags{})
		countStat.Gauge(totalPileUp)
		pkgLogger.Debugf("pile_up_count is %v ", totalPileUp)
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func SendRouterInMovingAverageStat() {
	for {
		time.Sleep(10 * time.Second)
		multitenantStat.routerJobCountMutex.RLock()
		for customer, value := range multitenantStat.RouterInputRates["router"] {
			for destType, count := range value {
				pkgLogger.Debugf("router_input_rate is %.8f for customer %v destType %v", count.Value(), customer, destType)
			}
		}
		multitenantStat.routerJobCountMutex.RUnlock()
	}
}

func CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool) {
	multitenantStat.routerSuccessRateMutex.Lock()
	defer multitenantStat.routerSuccessRateMutex.Unlock()
	_, ok := multitenantStat.RouterSuccessRatioLoopCount[customer]
	if !ok {
		multitenantStat.RouterSuccessRatioLoopCount[customer] = make(map[string]map[string]int)
	}
	_, ok = multitenantStat.RouterSuccessRatioLoopCount[customer][destType]
	if !ok {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType] = make(map[string]int)
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["success"] = 0
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["failure"] = 0
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["drained"] = 0
	}
	if isSuccess {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["success"] += 1
	} else if isDrained {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["drained"] += 1

		_, ok := multitenantStat.lastDrainedTimestamps[customer]
		if !ok {
			multitenantStat.lastDrainedTimestamps[customer] = make(map[string]time.Time)
		}
		multitenantStat.lastDrainedTimestamps[customer][destType] = time.Now()
	} else {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["failure"] += 1
	}
}

func checkIfBackedOff(customer string, destType string) (backedOff bool, timeExpired bool) {
	_, ok := multitenantStat.RouterCircuitBreakerMap[customer]
	if !ok {
		return false, false
	}
	_, ok = multitenantStat.RouterCircuitBreakerMap[customer][destType]
	if !ok {
		return false, false
	}
	if time.Now().After(multitenantStat.RouterCircuitBreakerMap[customer][destType].timeToRetry) {
		return true, true
	}
	return true, false
}

func GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64) {
	multitenantStat.routerSuccessRateMutex.RLock()
	customerSuccessRate := make(map[string]float64)
	customerDrainedMap := make(map[string]float64)
	for customer, destTypeMap := range multitenantStat.RouterSuccessRatioLoopCount {
		_, ok := destTypeMap[destType]
		if ok {
			successCount := destTypeMap[destType]["success"]
			failureCount := destTypeMap[destType]["failure"]
			drainedCount := destTypeMap[destType]["drained"]

			// TODO : Maintain this logic cleanly
			if failureCount == 0 && successCount == 0 && drainedCount == 0 {
				customerSuccessRate[customer] = 1
				customerDrainedMap[customer] = 0
			} else {
				customerSuccessRate[customer] = float64(successCount) / float64(successCount+failureCount+drainedCount)
				customerDrainedMap[customer] = float64(drainedCount) / float64(successCount+failureCount+drainedCount)
			}
		}
	}

	multitenantStat.routerSuccessRateMutex.RUnlock()
	multitenantStat.routerSuccessRateMutex.Lock()
	multitenantStat.RouterSuccessRatioLoopCount = make(map[string]map[string]map[string]int)
	multitenantStat.routerSuccessRateMutex.Unlock()
	return customerSuccessRate, customerDrainedMap
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
	multitenantStat.routerJobCountMutex.Lock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += count
	multitenantStat.routerJobCountMutex.Unlock()
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
	multitenantStat.routerJobCountMutex.Lock()
	multitenantStat.RouterInMemoryJobCounts[tableType][customerID][destinationType] += -1 * count
	multitenantStat.routerJobCountMutex.Unlock()
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

func getLastDrainedTimestamp(customerKey string, destType string) time.Time {
	multitenantStat.routerSuccessRateMutex.RLock()
	defer multitenantStat.routerSuccessRateMutex.RUnlock()
	destWiseMap, ok := multitenantStat.lastDrainedTimestamps[customerKey]
	if !ok {
		return time.Time{}
	}
	lastDrainedTS, ok := destWiseMap[destType]
	if !ok {
		return time.Time{}
	}
	return lastDrainedTS
}

func GetRouterPickupJobs(destType string, recentCustomerList []string, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, successRateMap map[string]float64, drainedMap map[string]float64) map[string]int {
	//Add 30% to the time interval as exact difference leads to a catchup scenario, but this may cause to give some priority to pileup in the inrate pass
	boostedRouterTimeOut := time.Duration(1.3 * float64(routerTimeOut))
	//TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	customerPickUpCount := make(map[string]int)
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)

	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()

	runningJobCount := jobQueryBatchSize
	//pkgLogger.Debugf("Sorted Latency Map is : %v ", strings.Join(sortedLatencyList, ", "))

	//Note: Essentially, we need to have a distinction between piling up customers & realtime customers. Draining && Least Recent Job in the pileup are good indicators of it.
	//deprioritise draining in the inrate and least recent job guys in the pileup pass
	finalList := make([]string, len(sortedLatencyList))
	depriorityIndicator := make([]bool, len(sortedLatencyList))
	deprioristiedList := make([]string, len(sortedLatencyList))
	finalListCounter := 0
	deprioristiedListCounter := 0
	for _, customerKey := range sortedLatencyList {
		lastDrainedTS := getLastDrainedTimestamp(customerKey, destType)
		pkgLogger.Debugf("[DRAIN DEBUG] %v checking for draining priority ", destType, customerKey, lastDrainedTS, time.Since(lastDrainedTS))
		if time.Since(lastDrainedTS) < 10*routerTimeOut {
			pkgLogger.Debugf("[DRAIN DEBUG] %v customer %v deprioritising in inrate and pileup ", destType, customerKey)
			deprioristiedList[deprioristiedListCounter] = customerKey
			deprioristiedListCounter++
		} else {
			finalList[finalListCounter] = customerKey
			depriorityIndicator[finalListCounter] = false
			finalListCounter++
		}
	}

	for i := 0; i < deprioristiedListCounter; i++ {
		finalList[finalListCounter+i] = deprioristiedList[i]
		depriorityIndicator[finalListCounter+i] = true

	}

	//TODO : Optimise the loop only for customers having jobs
	for i, customerKey := range finalList {
		customerCountKey, ok := multitenantStat.RouterInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						customerPickUpCount[customerKey] = 1
					}
					continue
				}

				timeGiven := boostedRouterTimeOut
				if depriorityIndicator[i] {
					timeGiven = routerTimeOut
				}

				timeRequired := 0.0

				unReliableLatencyORInRate := false
				if latencyMap[customerKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(timeGiven)/float64(time.Second), runningTimeCounter/(latencyMap[customerKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate customer %v latency value %v in rate %v", destType, customerKey, latencyMap[customerKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					customerPickUpCount[customerKey] = tmpPickCount
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType], 0)
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(timeGiven)/float64(time.Second)), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}

				timeRequired = float64(customerPickUpCount[customerKey]) * latencyMap[customerKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				if customerPickUpCount[customerKey] == 0 {
					delete(customerPickUpCount, customerKey)
				}
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRare : %v ,InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), destTypeCount.Value())
			}
		}
	}

	for _, customerKey := range recentCustomerList {
		customerCountKey, ok := multitenantStat.RouterInMemoryJobCounts["router"][customerKey]
		if !ok {
			continue
		}
		if customerCountKey[destType] == 0 {
			continue
		}

		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}
		timeRequired := latencyMap[customerKey].Value() * float64(customerCountKey[destType])
		if timeRequired < runningTimeCounter {
			pickUpCount := misc.MinInt(customerCountKey[destType]-customerPickUpCount[destType], runningJobCount)
			customerPickUpCount[customerKey] += pickUpCount
			runningTimeCounter = runningTimeCounter - float64(pickUpCount)*latencyMap[customerKey].Value()
			runningJobCount = runningJobCount - pickUpCount
			// Migrated jobs fix in else condition
		} else {
			pickUpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			pileupPickUp := misc.MinInt(misc.MinInt(pickUpCount, runningJobCount), customerCountKey[destType]-customerPickUpCount[destType])
			customerPickUpCount[customerKey] += pileupPickUp
			runningJobCount = runningJobCount - pileupPickUp
			runningTimeCounter = runningTimeCounter - float64(pileupPickUp)*latencyMap[customerKey].Value()
		}
		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,PileUpLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), customerCountKey[destType])
	}

	return customerPickUpCount

}
