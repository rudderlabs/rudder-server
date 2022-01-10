package multitenant

import (
	"fmt"
	"math"
	"strings"
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

func GenerateSuccessRateMap(destType string) (map[string]float64, map[string]bool) {
	multitenantStat.routerSuccessRateMutex.RLock()
	customerSuccessRate := make(map[string]float64)
	customerDrainedMap := make(map[string]bool)
	for customer, destTypeMap := range multitenantStat.RouterSuccessRatioLoopCount {
		_, ok := destTypeMap[destType]
		if ok {
			successCount := destTypeMap[destType]["success"]
			failureCount := destTypeMap[destType]["failure"]

			// TODO : Maintain this logic cleanly
			if failureCount == 0 && successCount == 0 {
				customerSuccessRate[customer] = 1
			} else {
				customerSuccessRate[customer] = float64(successCount) / float64(successCount+failureCount)
			}
		}
	}

	// for customer, destTypeMap := range multitenantStat.RouterSuccessRatioLoopCount {
	// 	_, ok := destTypeMap[destType]
	// 	drainedCount := destTypeMap[destType]["drained"]
	// 	if ok {
	// 		if drainedCount == 0 {
	// 			customerDrainedMap[customer] = false
	// 		} else {
	// 			customerDrainedMap[customer] = true
	// 		}
	// 	}
	// }

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

func getCorrectedJobsPickupCount(customerKey string, destType string, jobsPicked int, timeRequired float64, successRate float64) (float64, int, bool) {

	if successRate > 1 {
		panic(fmt.Errorf("success rate is more than 1.Panicking for %v customer , %v destType with successRate %v", customerKey, destType, successRate))
	} else if successRate > 0 {
		_, ok := multitenantStat.RouterCircuitBreakerMap[customerKey]
		if ok {
			delete(multitenantStat.RouterCircuitBreakerMap[customerKey], destType)
		}
	}

	if successRate == 1 {
		return timeRequired, jobsPicked, false
	} else if successRate > 0 {
		return successRate * timeRequired, int(float64(jobsPicked) * successRate), false
	}
	_, ok := multitenantStat.RouterCircuitBreakerMap[customerKey]
	if !ok {
		multitenantStat.RouterCircuitBreakerMap[customerKey] = make(map[string]BackOffT)
	}
	_, ok = multitenantStat.RouterCircuitBreakerMap[customerKey][destType]
	if !ok {
		backOff := &backoff.Backoff{
			Min:    minBackOff,
			Max:    maxBackOff,
			Factor: backOffFactor,
			Jitter: false,
		}
		multitenantStat.RouterCircuitBreakerMap[customerKey][destType] = BackOffT{backOff: backOff, timeToRetry: time.Now().Add(backOff.Duration())}
		pkgLogger.Debugf("Backing off for %v customer for the first time. Next Time to Retry would be %v", customerKey, multitenantStat.RouterCircuitBreakerMap[customerKey][destType].timeToRetry)
		return 0, 0, true
	} else if time.Now().After(multitenantStat.RouterCircuitBreakerMap[customerKey][destType].timeToRetry) {
		timeToRetry := time.Now().Add(multitenantStat.RouterCircuitBreakerMap[customerKey][destType].backOff.Duration())
		multitenantStat.RouterCircuitBreakerMap[customerKey][destType] = BackOffT{backOff: multitenantStat.RouterCircuitBreakerMap[customerKey][destType].backOff, timeToRetry: timeToRetry}
		pkgLogger.Debugf("Backing off for %v customer. Next Time to Retry would be %v", customerKey, timeToRetry)
		return 0, misc.MinInt(jobsPicked, 10), true
	} else {
		return 0, 0, true
	}
}
func GetRouterPickupJobs(destType string, earliestJobMap map[string]time.Time, sortedLatencyList []string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, successRateMap map[string]float64, drainedMap map[string]bool) map[string]int {
	customerPickUpCount := make(map[string]int)
	runningTimeCounter := float64(noOfWorkers) * float64(routerTimeOut) / float64(time.Second)
	customerBlockedMap := make(map[string]bool)
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()
	runningJobCount := jobQueryBatchSize
	pkgLogger.Debugf("Sorted Latency Map is : %v ", strings.Join(sortedLatencyList, ", "))
	//TODO : Optimise the loop only for customers having jobs
	for _, customerKey := range sortedLatencyList {
		customerCountKey, ok := multitenantStat.RouterInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {
				timeRequired := 0.0

				successRate := 1.0

				_, ok = successRateMap[customerKey]
				if ok {
					successRate = successRateMap[customerKey]
				} else {
					isBackedOff, isTimeExpired := checkIfBackedOff(customerKey, destType)
					if isBackedOff && !isTimeExpired {
						customerBlockedMap[customerKey] = true
						continue
					} else if isBackedOff && isTimeExpired {
						successRate = 0.0
					}
				}

				// If backed off : Ignore
				// If backedoff and time expired : Pick up Beta : Success rate 0

				//runningJobCount should be a number large enough to ensure fairness but small enough to not cause OOM Issues
				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						customerPickUpCount[customerKey] = 1
					}
					continue
				}

				if latencyMap[customerKey].Value() != 0 {
					customerPickUpCount[customerKey] = misc.MaxInt(int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(latencyMap[customerKey].Value()))), 1)
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType], 0)
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}
				timeRequired = float64(customerPickUpCount[customerKey]) * latencyMap[customerKey].Value()
				updatedTimeRequired, updatedPickUpCount, isCustomerLimited := getCorrectedJobsPickupCount(customerKey, destType, customerPickUpCount[customerKey], timeRequired, successRate)
				customerBlockedMap[customerKey] = isCustomerLimited
				runningTimeCounter = runningTimeCounter - updatedTimeRequired
				customerPickUpCount[customerKey] = updatedPickUpCount
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				if customerPickUpCount[customerKey] == 0 {
					delete(customerPickUpCount, customerKey)
				}
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRare : %v ,InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), destTypeCount.Value())
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
		if customerBlockedMap[customerKey] {
			continue
		}
		//customerBlockedMap contains backoff information
		/*isBackedOff, _ := checkIfBackedOff(customerKey, destType)
		if isBackedOff {
			continue
		}*/

		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}
		timeRequired := latencyMap[customerKey].Value() * float64(customerCountKey[destType])
		//TODO : Include earliestJobMap into the algorithm if required or get away with earliestJobMap
		if timeRequired < runningTimeCounter {
			pickUpCount := misc.MinInt(customerCountKey[destType]-customerPickUpCount[destType], runningJobCount)
			customerPickUpCount[customerKey] += pickUpCount
			runningTimeCounter = runningTimeCounter - timeRequired
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
	//TODO : BackedOff Pass , Optimise it later
	for _, customerKey := range sortedLatencyList {
		if runningJobCount <= 0 || int(runningTimeCounter) <= 0 {
			break
		}
		if customerBlockedMap[customerKey] {
			jobsPickedUp := misc.MinInt(runningJobCount, multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
			if latencyMap[customerKey].Value() != 0 {
				jobsPickedUp = misc.MinInt(jobsPickedUp, int(runningTimeCounter/latencyMap[customerKey].Value()))
			}
			customerPickUpCount[customerKey] += jobsPickedUp
			runningJobCount -= jobsPickedUp
			runningTimeCounter = runningTimeCounter - float64(jobsPickedUp)*latencyMap[customerKey].Value()
		}
		if customerBlockedMap[customerKey] {
			pkgLogger.Infof("[BackedOff Loop] Customer : %v , PickUpCount : %v , Drained Value : %v ,moving_average_latency : %v ", customerKey, customerPickUpCount[customerKey], customerPickUpCount[customerKey], latencyMap[customerKey].Value())
		}
	}

	return customerPickUpCount

}
