//go:generate mockgen -destination=../../mocks/services/multitenant/mock_tenantstats.go -package mock_tenantstats github.com/rudderlabs/rudder-server/services/multitenant MultiTenantI

package multitenant

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	pkgLogger logger.LoggerI
)

type MultitenantStatsT struct {
	routerNonTerminalCounts     map[string]map[string]map[string]int
	routerJobCountMutex         sync.RWMutex
	routerInputRates            map[string]map[string]map[string]misc.MovingAverage
	routerSuccessRatioLoopCount map[string]map[string]map[string]int
	lastDrainedTimestamps       map[string]map[string]time.Time
	failureRate                 map[string]map[string]misc.MovingAverage
	routerSuccessRateMutex      sync.RWMutex
}

type MultiTenantI interface {
	CalculateSuccessFailureCounts(customer string, destType string, isSuccess bool, isDrained bool)
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)
	GenerateSuccessRateMap(destType string) (map[string]float64, map[string]float64)
	AddToInMemoryCount(customerID string, destinationType string, count int, tableType string)
	RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string)
	ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string)
}

func Init() {
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
}

func NewStats(routerDB jobsdb.MultiTenantJobsDB) *MultitenantStatsT {
	multitenantStat := MultitenantStatsT{}
	multitenantStat.routerNonTerminalCounts = make(map[string]map[string]map[string]int)
	multitenantStat.routerNonTerminalCounts["router"] = make(map[string]map[string]int)
	multitenantStat.routerNonTerminalCounts["batch_router"] = make(map[string]map[string]int)
	multitenantStat.routerInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.routerInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.routerInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.routerSuccessRatioLoopCount = make(map[string]map[string]map[string]int)
	multitenantStat.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	multitenantStat.failureRate = make(map[string]map[string]misc.MovingAverage)
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
	_, ok := multitenantStat.routerSuccessRatioLoopCount[customer]
	if !ok {
		multitenantStat.routerSuccessRatioLoopCount[customer] = make(map[string]map[string]int)
	}
	_, ok = multitenantStat.routerSuccessRatioLoopCount[customer][destType]
	if !ok {
		multitenantStat.routerSuccessRatioLoopCount[customer][destType] = make(map[string]int)
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["success"] = 0
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["failure"] = 0
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["drained"] = 0
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
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["success"] += 1
		multitenantStat.failureRate[customer][destType].Add(0)
	} else if isDrained {
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["drained"] += 1

		_, ok := multitenantStat.lastDrainedTimestamps[customer]
		if !ok {
			multitenantStat.lastDrainedTimestamps[customer] = make(map[string]time.Time)
		}
		multitenantStat.lastDrainedTimestamps[customer][destType] = time.Now()
		multitenantStat.failureRate[customer][destType].Add(0)
	} else {
		multitenantStat.routerSuccessRatioLoopCount[customer][destType]["failure"] += 1
		multitenantStat.failureRate[customer][destType].Add(1)
	}
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
	for customer, destTypeMap := range multitenantStat.routerSuccessRatioLoopCount {
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
	multitenantStat.routerSuccessRatioLoopCount = make(map[string]map[string]map[string]int)
	multitenantStat.routerSuccessRateMutex.Unlock()
	return customerSuccessRate, customerDrainedMap
}

func (multitenantStat *MultitenantStatsT) AddToInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.routerNonTerminalCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.routerNonTerminalCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.routerJobCountMutex.Lock()
	multitenantStat.routerNonTerminalCounts[tableType][customerID][destinationType] += count
	multitenantStat.routerJobCountMutex.Unlock()
}

func (multitenantStat *MultitenantStatsT) RemoveFromInMemoryCount(customerID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.RLock()
	_, ok := multitenantStat.routerNonTerminalCounts[tableType][customerID]
	if !ok {
		multitenantStat.routerJobCountMutex.RUnlock()
		multitenantStat.routerJobCountMutex.Lock()
		multitenantStat.routerNonTerminalCounts[tableType][customerID] = make(map[string]int)
		multitenantStat.routerJobCountMutex.Unlock()
		multitenantStat.routerJobCountMutex.RLock()
	}
	multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.routerJobCountMutex.Lock()
	multitenantStat.routerNonTerminalCounts[tableType][customerID][destinationType] += -1 * count
	multitenantStat.routerJobCountMutex.Unlock()
}

func (multitenantStat *MultitenantStatsT) ReportProcLoopAddStats(stats map[string]map[string]int, timeTaken time.Duration, tableType string) {
	for key := range stats {
		multitenantStat.routerJobCountMutex.RLock()
		_, ok := multitenantStat.routerInputRates[tableType][key]
		if !ok {
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.routerJobCountMutex.Lock()
			multitenantStat.routerInputRates[tableType][key] = make(map[string]misc.MovingAverage)
			multitenantStat.routerJobCountMutex.Unlock()
			multitenantStat.routerJobCountMutex.RLock()
		}
		multitenantStat.routerJobCountMutex.RUnlock()
		for destType := range stats[key] {
			multitenantStat.routerJobCountMutex.RLock()
			_, ok := multitenantStat.routerInputRates[tableType][key][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.RUnlock()
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.routerInputRates[tableType][key][destType] = misc.NewMovingAverage()
				multitenantStat.routerJobCountMutex.Unlock()
				multitenantStat.routerJobCountMutex.RLock()
			}
			multitenantStat.routerJobCountMutex.RUnlock()
			multitenantStat.routerInputRates[tableType][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
			multitenantStat.AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for customerKey := range multitenantStat.routerInputRates[tableType] {
		_, ok := stats[customerKey]
		if !ok {
			for destType := range stats[customerKey] {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.routerInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}

		for destType := range multitenantStat.routerInputRates[tableType][customerKey] {
			_, ok := stats[customerKey][destType]
			if !ok {
				multitenantStat.routerJobCountMutex.Lock()
				multitenantStat.routerInputRates[tableType][customerKey][destType].Add(0)
				multitenantStat.routerJobCountMutex.Unlock()
			}
		}
	}
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

func (multitenantStat *MultitenantStatsT) getCustomersWithPendingJobs(destType string, latencyMap map[string]misc.MovingAverage) []string {
	customersWithJobs := make([]string, 0)
	for customerKey := range latencyMap {
		destWiseMap, ok := multitenantStat.routerNonTerminalCounts["router"][customerKey]
		if ok {
			val, ok := destWiseMap[destType]
			if ok && val > 0 {
				customersWithJobs = append(customersWithJobs, customerKey)
			}
		}
	}
	return customersWithJobs
}

func getBoostedRouterTimeOut(routerTimeOut time.Duration, timeGained float64, noOfWorkers int) time.Duration {
	//Add 30% to the time interval as exact difference leads to a catchup scenario, but this may cause to give some priority to pileup in the inrate pass
	//boostedRouterTimeOut := 3 * time.Second //time.Duration(1.3 * float64(routerTimeOut))
	//if boostedRouterTimeOut < time.Duration(1.3*float64(routerTimeOut)) {
	return time.Duration(1.3*float64(routerTimeOut)) + time.Duration(timeGained*float64(time.Second)/float64(noOfWorkers))
}

func getMinMaxCustomerLatency(customersWithJobs []string, latencyMap map[string]misc.MovingAverage) (float64, float64) {
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
	return minLatency, maxLatency
}

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceScoreList(customersWithJobs []string, maxLatency, minLatency float64, latencyMap map[string]misc.MovingAverage, destType string) []workspaceScore {
	scores := make([]workspaceScore, len(customersWithJobs))
	for i, customerKey := range customersWithJobs {
		scores[i] = workspaceScore{}
		latencyScore := 0.0
		if maxLatency-minLatency != 0 {
			latencyScore = (latencyMap[customerKey].Value() - minLatency) / (maxLatency - minLatency)
		}

		invertedRecencyScore := 0.0

		isDraining := 0.0
		if time.Since(multitenantStat.getLastDrainedTimestamp(customerKey, destType)) < 100*time.Second {
			isDraining = 1.0
		}

		scores[i].score = latencyScore + 10*(invertedRecencyScore) + 100*isDraining
		scores[i].workspaceId = customerKey
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score < scores[j].score
	})
	return scores
}

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceSecondaryScoreList(customersWithJobs []string, customerPickUpCount map[string]int, destType string, latencyMap map[string]misc.MovingAverage) []workspaceScore {
	//Sort by customers who can get to realtime quickly
	scores := make([]workspaceScore, len(customersWithJobs))
	for i, customerKey := range customersWithJobs {
		scores[i] = workspaceScore{}
		scores[i].workspaceId = customerKey

		customerCountKey, ok := multitenantStat.routerNonTerminalCounts["router"][customerKey]
		if !ok || customerCountKey[destType]-customerPickUpCount[customerKey] <= 0 {
			scores[i].score = math.MaxFloat64
			scores[i].secondary_score = 0
			continue
		}
		if multitenantStat.getFailureRate(customerKey, destType) == 1 {
			scores[i].score = math.MaxFloat64
		} else {
			scores[i].score = float64(customerCountKey[destType]-customerPickUpCount[customerKey]) * latencyMap[customerKey].Value() / (1 - multitenantStat.getFailureRate(customerKey, destType))
		}
		scores[i].secondary_score = float64(customerCountKey[destType] - customerPickUpCount[customerKey])
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score == math.MaxFloat64 && scores[j].score == math.MaxFloat64 {
			return scores[i].secondary_score < scores[j].secondary_score
		}
		return scores[i].score < scores[j].score
	})
	return scores
}

func (multitenantStat *MultitenantStatsT) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, latencyMap map[string]misc.MovingAverage, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()

	customersWithJobs := multitenantStat.getCustomersWithPendingJobs(destType, latencyMap)
	boostedRouterTimeOut := getBoostedRouterTimeOut(routerTimeOut, timeGained, noOfWorkers)
	//TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	runningJobCount := jobQueryBatchSize
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)
	customerPickUpCount := make(map[string]int)
	usedLatencies := make(map[string]float64)

	minLatency, maxLatency := getMinMaxCustomerLatency(customersWithJobs, latencyMap)

	scores := multitenantStat.getSortedWorkspaceScoreList(customersWithJobs, maxLatency, minLatency, latencyMap, destType)

	//TODO : Optimise the loop only for customers having jobs
	//Latency sorted input rate pass
	for _, scoredWorkspace := range scores {
		customerKey := scoredWorkspace.workspaceId
		customerCountKey, ok := multitenantStat.routerInputRates["router"][customerKey]
		if ok {
			destTypeCount, ok := customerCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					//Adding BETA
					if multitenantStat.routerNonTerminalCounts["router"][customerKey][destType] > 0 {
						usedLatencies[customerKey] = latencyMap[customerKey].Value()
						customerPickUpCount[customerKey] = 1
					}
					continue
				}
				//TODO : Get rid of unReliableLatencyORInRate hack
				unReliableLatencyORInRate := false
				if latencyMap[customerKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(latencyMap[customerKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 //Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate customer %v latency value %v in rate %v", destType, customerKey, latencyMap[customerKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					customerPickUpCount[customerKey] = tmpPickCount
					if customerPickUpCount[customerKey] > multitenantStat.routerNonTerminalCounts["router"][customerKey][destType] {
						customerPickUpCount[customerKey] = misc.MaxInt(multitenantStat.routerNonTerminalCounts["router"][customerKey][destType], 0)
					}
				} else {
					customerPickUpCount[customerKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.routerNonTerminalCounts["router"][customerKey][destType])
				}

				timeRequired := float64(customerPickUpCount[customerKey]) * latencyMap[customerKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - customerPickUpCount[customerKey]
				usedLatencies[customerKey] = latencyMap[customerKey].Value()
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRare : %v ,InRateLoop ", timeRequired, runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), destTypeCount.Value())
			}
		}
	}

	//Sort by customers who can get to realtime quickly
	secondaryScores := multitenantStat.getSortedWorkspaceSecondaryScoreList(customersWithJobs, customerPickUpCount, destType, latencyMap)
	for _, scoredWorkspace := range secondaryScores {
		customerKey := scoredWorkspace.workspaceId
		customerCountKey, ok := multitenantStat.routerNonTerminalCounts["router"][customerKey]
		if !ok || customerCountKey[destType] <= 0 {
			continue
		}
		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if latencyMap[customerKey].Value() == 0 {
			pickUpCount = misc.MinInt(customerCountKey[destType]-customerPickUpCount[customerKey], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / latencyMap[customerKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), customerCountKey[destType]-customerPickUpCount[customerKey])
		}
		usedLatencies[customerKey] = latencyMap[customerKey].Value()
		customerPickUpCount[customerKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*latencyMap[customerKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Customer : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,PileUpLoop ", float64(pickUpCount)*latencyMap[customerKey].Value(), runningTimeCounter, customerKey, runningJobCount, latencyMap[customerKey].Value(), customerCountKey[destType])
	}

	return customerPickUpCount, usedLatencies

}
