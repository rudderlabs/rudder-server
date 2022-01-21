//go:generate mockgen -destination=../../mocks/services/multitenant/mock_tenantstats.go -package mock_tenantstats github.com/rudderlabs/rudder-server/services/multitenant MultiTenantI

package multitenant

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/jpillora/backoff"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	pkgLogger     logger.LoggerI
	minBackOff    time.Duration
	maxBackOff    time.Duration
	backOffFactor float64
)

type MultitenantStatsT struct {
	RouterInMemoryJobCounts     map[string]map[string]map[string]int
	routerJobCountMutex         sync.RWMutex
	RouterInputRates            map[string]map[string]map[string]misc.MovingAverage
	RouterSuccessRatioLoopCount map[string]map[string]map[string]int
	lastDrainedTimestamps       map[string]map[string]time.Time
	failureRate                 map[string]map[string]misc.MovingAverage
	RouterCircuitBreakerMap     map[string]map[string]BackOffT
	routerSuccessRateMutex      sync.RWMutex
}

type BackOffT struct {
	backOff     *backoff.Backoff
	timeToRetry time.Time
}

type MultiTenantI interface {
	CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool)
	GetRouterPickupJobs(destType string, recentJobInResultSet map[string]time.Time, routerLatencyStat map[string]misc.MovingAverage, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)	GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64)
	AddToInMemoryCount(customerID string, destinationType string, count int, tableType string)
	RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string)
	ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string)
}

func Init() {
	config.RegisterDurationConfigVariable(time.Duration(10), &minBackOff, false, time.Second, "tenantStats.minBackOff")
	config.RegisterDurationConfigVariable(time.Duration(300), &maxBackOff, false, time.Second, "tenantStats.maxBackOff")
	config.RegisterFloat64ConfigVariable(1.5, &backOffFactor, false, "tenantStats.backOffFactor")
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
}

func NewStats(routerDB jobsdb.MultiTenantJobsDB) *MultitenantStatsT {
	multitenantStat := MultitenantStatsT{}
	multitenantStat.RouterInMemoryJobCounts = make(map[string]map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["router"] = make(map[string]map[string]int)
	multitenantStat.RouterInMemoryJobCounts["batch_router"] = make(map[string]map[string]int)
	multitenantStat.RouterInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterSuccessRatioLoopCount = make(map[string]map[string]map[string]int)
	multitenantStat.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	multitenantStat.failureRate = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.RouterCircuitBreakerMap = make(map[string]map[string]BackOffT)
	pileUpStatMap := make(map[string]map[string]int)
	routerDB.GetPileUpCounts(pileUpStatMap)
	for customer := range pileUpStatMap {
		for destType := range pileUpStatMap[customer] {
			multitenantStat.AddToInMemoryCount(customer, destType, pileUpStatMap[customer][destType], "router")
		}
	}
	return &multitenantStat
}

func (multitenantStat *MultitenantStatsT) CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool) {
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

	_, ok = multitenantStat.failureRate[customer]
	if !ok {
		multitenantStat.failureRate[customer] = make(map[string]misc.MovingAverage)
	}
	_, ok = multitenantStat.failureRate[customer][destType]
	if !ok {
		multitenantStat.failureRate[customer][destType] = misc.NewMovingAverage(misc.AVG_METRIC_AGE)
	}

	if isSuccess {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["success"] += 1
		multitenantStat.failureRate[customer][destType].Add(0)
	} else if isDrained {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["drained"] += 1

		_, ok := multitenantStat.lastDrainedTimestamps[customer]
		if !ok {
			multitenantStat.lastDrainedTimestamps[customer] = make(map[string]time.Time)
		}
		multitenantStat.lastDrainedTimestamps[customer][destType] = time.Now()
		multitenantStat.failureRate[customer][destType].Add(0)
	} else {
		multitenantStat.RouterSuccessRatioLoopCount[customer][destType]["failure"] += 1
		multitenantStat.failureRate[customer][destType].Add(1)
	}
}

func (multitenantStat *MultitenantStatsT) checkIfBackedOff(customer string, destType string) (backedOff bool, timeExpired bool) {
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

func (multitenantStat *MultitenantStatsT) getFailureRate(customerKey string, destType string) float64 {
	_, ok := multitenantStat.failureRate[customerKey]
	if ok {
		_, ok = multitenantStat.failureRate[customerKey][destType]
		if ok {
			return multitenantStat.failureRate[customerKey][destType].Value()
		}
	}
	return 0.0
}

func (multitenantStat *MultitenantStatsT) GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64) {
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

func (multitenantStat *MultitenantStatsT) AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
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

func (multitenantStat *MultitenantStatsT) RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
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

func (multitenantStat *MultitenantStatsT) ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
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
			multitenantStat.AddToInMemoryCount(key, destType, stats[key][destType], tableType)
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

//if a workspace has a pileup, returns True
func (multitenantStat *MultitenantStatsT) isWorkspaceLagging(customerKey string, recentJobInResultSet map[string]time.Time, realMaxRecency time.Time) bool {
	if ts, ok := recentJobInResultSet[customerKey]; ok {
		if realMaxRecency.After(ts) && realMaxRecency.Sub(ts) > 900*time.Second { //15 minutes arbitrary
			return true
		}
	}
	return false
}

func (multitenantStat *MultitenantStatsT) getLastDrainedTimestamp(customerKey string, destType string) time.Time {
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

type workspaceScore struct {
	score           float64
	secondary_score float64
	workspaceId     string
}

func (multitenantStat *MultitenantStatsT) GetRouterPickupJobs(destType string, recentJobInResultSet map[string]time.Time, routerLatencyStat map[string]misc.MovingAverage, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()

	customersWithJobs := make([]string, 0)
	for custKey := range routerLatencyStat {
		destWiseMap, ok := multitenantStat.RouterInMemoryJobCounts["router"][custKey]
		if ok {
			val, ok := destWiseMap[destType]
			if ok && val > 0 {
				customersWithJobs = append(customersWithJobs, custKey)
			}
		}
	}
	//Add 30% to the time interval as exact difference leads to a catchup scenario, but this may cause to give some priority to pileup in the inrate pass
	//boostedRouterTimeOut := 3 * time.Second //time.Duration(1.3 * float64(routerTimeOut))
	//if boostedRouterTimeOut < time.Duration(1.3*float64(routerTimeOut)) {
	boostedRouterTimeOut := time.Duration(1.3*float64(routerTimeOut)) + time.Duration(timeGained*float64(time.Second)/float64(noOfWorkers))
	//}
	//TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	runningJobCount := jobQueryBatchSize
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)
	customerPickUpCount := make(map[string]int)
	usedLatencies := make(map[string]float64)

	minLatency := math.MaxFloat64
	maxLatency := -math.MaxFloat64

	//Below two loops, normalize the values and compute the score of each workspace
	for _, customerKey := range customersWithJobs {
		if minLatency > latencyMap[customerKey].Value() {
			minLatency = latencyMap[customerKey].Value()
		}
		if maxLatency < latencyMap[customerKey].Value() {
			maxLatency = latencyMap[customerKey].Value()
		}
	}

	scores := make([]workspaceScore, len(customersWithJobs))
	for i, customerKey := range customersWithJobs {
		scores[i] = workspaceScore{}
		latencyScore := 0.0
		if maxLatency-minLatency != 0 {
			latencyScore = (latencyMap[customerKey].Value() - minLatency) / (maxLatency - minLatency)
		}

		invertedRecencyScore := 0.0
		/*if isWorkspaceLagging(customerKey, recentJobInResultSet, realMaxRecency) {
			if float64(maxRecency.UnixNano()-minRecency.UnixNano()) != 0 {
				recencyScore := (float64(recentJobInResultSet[customerKey].UnixNano() - minRecency.UnixNano())) / float64(maxRecency.UnixNano()-minRecency.UnixNano())
				invertedRecencyScore = 1 - recencyScore
				invertedRecencyScore++ //Moving Range from 1 to 2
			}
		}*/

		isDraining := 0.0
		if time.Since(multitenantStat.getLastDrainedTimestamp(customerKey, destType)) < 100*time.Second {
			isDraining = 1.0
		}

		scores[i].score = latencyScore + 10*(invertedRecencyScore) + 100*isDraining
		scores[i].workspaceId = customerKey
	}

	sort.Slice(scores[:], func(i, j int) bool {
		return scores[i].score < scores[j].score
	})

	//TODO : Optimise the loop only for customers having jobs
	//Latency sorted input rate pass
	for _, scoredWorkspace := range scores {
		customerKey := scoredWorkspace.workspaceId
		customerCountKey, ok := multitenantStat.RouterInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					//Adding BETA
					if multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] > 0 {
						usedLatencies[customerKey] = latencyMap[customerKey].Value()
						customerPickUpCount[customerKey] = 1
					}
					continue
				}

				unReliableLatencyORInRate := false
				if latencyMap[customerKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(latencyMap[customerKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 //Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate customer %v latency value %v in rate %v", destType, customerKey, latencyMap[customerKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					customerPickUpCount[customerKey] = tmpPickCount
					if customerPickUpCount[customerKey] > multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType], 0)
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.RouterInMemoryJobCounts["router"][customerKey][destType])
				}

				timeRequired := float64(customerPickUpCount[customerKey]) * latencyMap[customerKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				if customerPickUpCount[customerKey] == 0 {
					delete(customerPickUpCount, customerKey)
				}
				usedLatencies[customerKey] = latencyMap[customerKey].Value()
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRare : %v ,InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), destTypeCount.Value())
			}
		}
	}

	//Sort by customers who can get to realtime quickly
	scores = make([]workspaceScore, len(customersWithJobs))
	for i, customerKey := range customersWithJobs {
		scores[i] = workspaceScore{}
		scores[i].workspaceId = customerKey

		customerCountKey, ok := multitenantStat.RouterInMemoryJobCounts["router"][customerKey]
		if !ok || customerCountKey[destType]-customerPickUpCount[customerKey] <= 0 {
			scores[i].score = math.MaxFloat64
			scores[i].secondary_score = 0
			continue
		}
		if 1 == multitenantStat.getFailureRate(customerKey, destType) {
			scores[i].score = math.MaxFloat64
		} else {
			scores[i].score = float64(customerCountKey[destType]-customerPickUpCount[customerKey]) * latencyMap[customerKey].Value() / (1 - multitenantStat.getFailureRate(customerKey, destType))
		}
		scores[i].secondary_score = float64(customerCountKey[destType] - customerPickUpCount[customerKey])
	}

	sort.Slice(scores[:], func(i, j int) bool {
		if scores[i].score == math.MaxFloat64 && scores[j].score == math.MaxFloat64 {
			return scores[i].secondary_score < scores[j].secondary_score
		}
		return scores[i].score < scores[j].score
	})

	for _, scoredWorkspace := range scores {
		customerKey := scoredWorkspace.workspaceId
		customerCountKey, ok := multitenantStat.RouterInMemoryJobCounts["router"][customerKey]
		if !ok || customerCountKey[destType] <= 0 {
			continue
		}
		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if latencyMap[customerKey].Value() == 0 {
			pickUpCount = misc.MinInt(customerCountKey[destType]-customerPickUpCount[destType], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), customerCountKey[destType]-customerPickUpCount[destType])
		}
		usedLatencies[customerKey] = latencyMap[customerKey].Value()
		customerPickUpCount[customerKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*latencyMap[customerKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,PileUpLoop ", float64(pickUpCount)*latencyMap[customerKey].Value(), runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), customerCountKey[destType])
	}

	return customerPickUpCount, usedLatencies

}
