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
	routerNonTerminalCounts map[string]map[string]map[string]int
	routerJobCountMutex     *sync.RWMutex
	routerInputRates        map[string]map[string]map[string]misc.MovingAverage
	lastDrainedTimestamps   map[string]map[string]time.Time
	failureRate             map[string]map[string]misc.MovingAverage
	routerSuccessRateMutex  *sync.RWMutex
	routerTenantLatencyStat map[string]map[string]misc.MovingAverage
	routerLatencyMutex      *sync.RWMutex
	processorStageTime      time.Time
}

type MultiTenantI interface {
	CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool)
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)
	AddToInMemoryCount(workspaceID string, destinationType string, count int, tableType string)
	RemoveFromInMemoryCount(workspaceID string, destinationType string, count int, tableType string)
	ReportProcLoopAddStats(stats map[string]map[string]int, tableType string)
	UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64)
}

type workspaceScore struct {
	score           float64
	secondary_score float64
	workspaceId     string
}

func Init() {
	pkgLogger = logger.NewLogger().Child("services").Child("multitenant")
}

func NewStats(routerDB jobsdb.MultiTenantJobsDB) *MultitenantStatsT {
	multitenantStat := MultitenantStatsT{}
	multitenantStat.routerJobCountMutex = &sync.RWMutex{}
	multitenantStat.routerSuccessRateMutex = &sync.RWMutex{}
	multitenantStat.routerLatencyMutex = &sync.RWMutex{}

	multitenantStat.routerJobCountMutex.Lock()
	multitenantStat.routerNonTerminalCounts = make(map[string]map[string]map[string]int)
	multitenantStat.routerNonTerminalCounts["router"] = make(map[string]map[string]int)
	multitenantStat.routerNonTerminalCounts["batch_router"] = make(map[string]map[string]int)
	multitenantStat.routerInputRates = make(map[string]map[string]map[string]misc.MovingAverage)
	multitenantStat.routerInputRates["router"] = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.routerInputRates["batch_router"] = make(map[string]map[string]misc.MovingAverage)

	multitenantStat.routerSuccessRateMutex.Lock()
	multitenantStat.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	multitenantStat.failureRate = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.routerSuccessRateMutex.Unlock()
	pileUpStatMap := make(map[string]map[string]int)
	routerDB.GetPileUpCounts(pileUpStatMap)
	for workspace := range pileUpStatMap {
		for destType := range pileUpStatMap[workspace] {
			multitenantStat.AddToInMemoryCount(workspace, destType, pileUpStatMap[workspace][destType], "router")
		}
	}

	multitenantStat.routerJobCountMutex.Unlock()

	multitenantStat.routerLatencyMutex.Lock()
	multitenantStat.routerTenantLatencyStat = make(map[string]map[string]misc.MovingAverage)
	multitenantStat.routerLatencyMutex.Unlock()

	multitenantStat.processorStageTime = time.Now()

	return &multitenantStat
}

func (multitenantStat *MultitenantStatsT) UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64) {
	multitenantStat.routerLatencyMutex.Lock()
	defer multitenantStat.routerLatencyMutex.Unlock()
	_, ok := multitenantStat.routerTenantLatencyStat[destType]
	if !ok {
		multitenantStat.routerTenantLatencyStat[destType] = make(map[string]misc.MovingAverage)
	}
	_, ok = multitenantStat.routerTenantLatencyStat[destType][workspaceID]
	if !ok {
		multitenantStat.routerTenantLatencyStat[destType][workspaceID] = misc.NewMovingAverage(misc.AVG_METRIC_AGE)
	}
	multitenantStat.routerTenantLatencyStat[destType][workspaceID].Add(val)
}

func (multitenantStat *MultitenantStatsT) CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool) {
	multitenantStat.routerSuccessRateMutex.Lock()
	defer multitenantStat.routerSuccessRateMutex.Unlock()

	_, ok := multitenantStat.failureRate[workspace]
	if !ok {
		multitenantStat.failureRate[workspace] = make(map[string]misc.MovingAverage)
	}
	_, ok = multitenantStat.failureRate[workspace][destType]
	if !ok {
		multitenantStat.failureRate[workspace][destType] = misc.NewMovingAverage(misc.AVG_METRIC_AGE)
	}

	if isSuccess {
		multitenantStat.failureRate[workspace][destType].Add(0)
	} else if isDrained {

		_, ok := multitenantStat.lastDrainedTimestamps[workspace]
		if !ok {
			multitenantStat.lastDrainedTimestamps[workspace] = make(map[string]time.Time)
		}
		multitenantStat.lastDrainedTimestamps[workspace][destType] = time.Now()
		multitenantStat.failureRate[workspace][destType].Add(0)
	} else {
		multitenantStat.failureRate[workspace][destType].Add(1)
	}
}

func (multitenantStat *MultitenantStatsT) AddToInMemoryCount(workspaceID string, destinationType string, count int, tableType string) {
	_, ok := multitenantStat.routerNonTerminalCounts[tableType][workspaceID]
	if !ok {
		multitenantStat.routerNonTerminalCounts[tableType][workspaceID] = make(map[string]int)
	}
	multitenantStat.routerNonTerminalCounts[tableType][workspaceID][destinationType] += count
}

func (multitenantStat *MultitenantStatsT) RemoveFromInMemoryCount(workspaceID string, destinationType string, count int, tableType string) {
	multitenantStat.routerJobCountMutex.Lock()
	defer multitenantStat.routerJobCountMutex.Unlock()
	multitenantStat.routerNonTerminalCounts[tableType][workspaceID][destinationType] -= count
}

func (multitenantStat *MultitenantStatsT) ReportProcLoopAddStats(stats map[string]map[string]int, tableType string) {
	multitenantStat.routerJobCountMutex.Lock()
	defer multitenantStat.routerJobCountMutex.Unlock()

	timeTaken := time.Since(multitenantStat.processorStageTime)
	for key := range stats {
		_, ok := multitenantStat.routerInputRates[tableType][key]
		if !ok {
			multitenantStat.routerInputRates[tableType][key] = make(map[string]misc.MovingAverage)
		}
		for destType := range stats[key] {
			_, ok := multitenantStat.routerInputRates[tableType][key][destType]
			if !ok {
				multitenantStat.routerInputRates[tableType][key][destType] = misc.NewMovingAverage()
			}
			multitenantStat.routerInputRates[tableType][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
			multitenantStat.AddToInMemoryCount(key, destType, stats[key][destType], tableType)
		}
	}
	for workspaceKey := range multitenantStat.routerInputRates[tableType] {
		_, ok := stats[workspaceKey]
		if !ok {
			for destType := range stats[workspaceKey] {
				multitenantStat.routerInputRates[tableType][workspaceKey][destType].Add(0)
			}
		}

		for destType := range multitenantStat.routerInputRates[tableType][workspaceKey] {
			_, ok := stats[workspaceKey][destType]
			if !ok {
				multitenantStat.routerInputRates[tableType][workspaceKey][destType].Add(0)
			}
		}
	}
	multitenantStat.processorStageTime = time.Now()
}

func (multitenantStat *MultitenantStatsT) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	multitenantStat.routerJobCountMutex.RLock()
	defer multitenantStat.routerJobCountMutex.RUnlock()
	multitenantStat.routerLatencyMutex.RLock()
	defer multitenantStat.routerLatencyMutex.RUnlock()

	workspacesWithJobs := multitenantStat.getWorkspacesWithPendingJobs(destType, multitenantStat.routerTenantLatencyStat[destType])
	boostedRouterTimeOut := getBoostedRouterTimeOut(routerTimeOut, timeGained, noOfWorkers)
	//TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	runningJobCount := jobQueryBatchSize
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)
	workspacePickUpCount := make(map[string]int)
	usedLatencies := make(map[string]float64)

	minLatency, maxLatency := getMinMaxWorkspaceLatency(workspacesWithJobs, multitenantStat.routerTenantLatencyStat[destType])

	scores := multitenantStat.getSortedWorkspaceScoreList(workspacesWithJobs, maxLatency, minLatency, multitenantStat.routerTenantLatencyStat[destType], destType)
	//TODO : Optimise the loop only for workspaces having jobs
	//Latency sorted input rate pass
	for _, scoredWorkspace := range scores {
		workspaceKey := scoredWorkspace.workspaceId
		workspaceCountKey, ok := multitenantStat.routerInputRates["router"][workspaceKey]
		if ok {
			destTypeCount, ok := workspaceCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					//Adding BETA
					if multitenantStat.routerNonTerminalCounts["router"][workspaceKey][destType] > 0 {
						usedLatencies[workspaceKey] = multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
						workspacePickUpCount[workspaceKey] = 1
					}
					continue
				}
				//TODO : Get rid of unReliableLatencyORInRate hack
				unReliableLatencyORInRate := false
				if multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 //Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate workspace %v latency value %v in rate %v", destType, workspaceKey, multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					workspacePickUpCount[workspaceKey] = tmpPickCount
					if workspacePickUpCount[workspaceKey] > multitenantStat.routerNonTerminalCounts["router"][workspaceKey][destType] {
						workspacePickUpCount[workspaceKey] = misc.MaxInt(multitenantStat.routerNonTerminalCounts["router"][workspaceKey][destType], 0)
					}
				} else {
					workspacePickUpCount[workspaceKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), multitenantStat.routerNonTerminalCounts["router"][workspaceKey][destType])
				}

				timeRequired := float64(workspacePickUpCount[workspaceKey]) * multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - workspacePickUpCount[workspaceKey]
				usedLatencies[workspaceKey] = multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRare : %v ,InRateLoop ", timeRequired, runningTimeCounter, workspaceKey, runningJobCount, multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value())
			}
		}
	}

	//Sort by workspaces who can get to realtime quickly
	secondaryScores := multitenantStat.getSortedWorkspaceSecondaryScoreList(workspacesWithJobs, workspacePickUpCount, destType, multitenantStat.routerTenantLatencyStat[destType])
	for _, scoredWorkspace := range secondaryScores {
		workspaceKey := scoredWorkspace.workspaceId
		workspaceCountKey, ok := multitenantStat.routerNonTerminalCounts["router"][workspaceKey]
		if !ok || workspaceCountKey[destType] <= 0 {
			continue
		}
		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value() == 0 {
			pickUpCount = misc.MinInt(workspaceCountKey[destType]-workspacePickUpCount[workspaceKey], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), workspaceCountKey[destType]-workspacePickUpCount[workspaceKey])
		}
		usedLatencies[workspaceKey] = multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
		workspacePickUpCount[workspaceKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,PileUpLoop ", float64(pickUpCount)*multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), runningTimeCounter, workspaceKey, runningJobCount, multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), workspaceCountKey[destType])
	}

	return workspacePickUpCount, usedLatencies

}

func (multitenantStat *MultitenantStatsT) getFailureRate(workspaceKey string, destType string) float64 {
	multitenantStat.routerSuccessRateMutex.RLock()
	defer multitenantStat.routerSuccessRateMutex.RUnlock()
	_, ok := multitenantStat.failureRate[workspaceKey]
	if ok {
		_, ok = multitenantStat.failureRate[workspaceKey][destType]
		if ok {
			return multitenantStat.failureRate[workspaceKey][destType].Value()
		}
	}
	return 0.0
}

func (multitenantStat *MultitenantStatsT) getLastDrainedTimestamp(workspaceKey string, destType string) time.Time {
	multitenantStat.routerSuccessRateMutex.RLock()
	defer multitenantStat.routerSuccessRateMutex.RUnlock()
	destWiseMap, ok := multitenantStat.lastDrainedTimestamps[workspaceKey]
	if !ok {
		return time.Time{}
	}
	lastDrainedTS, ok := destWiseMap[destType]
	if !ok {
		return time.Time{}
	}
	return lastDrainedTS
}

func (multitenantStat *MultitenantStatsT) getWorkspacesWithPendingJobs(destType string, latencyMap map[string]misc.MovingAverage) []string {
	workspacesWithJobs := make([]string, 0)
	for workspaceKey := range latencyMap {
		destWiseMap, ok := multitenantStat.routerNonTerminalCounts["router"][workspaceKey]
		if ok {
			val, ok := destWiseMap[destType]
			if ok && val > 0 {
				workspacesWithJobs = append(workspacesWithJobs, workspaceKey)
			}
		}
	}
	return workspacesWithJobs
}

func getBoostedRouterTimeOut(routerTimeOut time.Duration, timeGained float64, noOfWorkers int) time.Duration {
	//Add 30% to the time interval as exact difference leads to a catchup scenario, but this may cause to give some priority to pileup in the inrate pass
	//boostedRouterTimeOut := 3 * time.Second //time.Duration(1.3 * float64(routerTimeOut))
	//if boostedRouterTimeOut < time.Duration(1.3*float64(routerTimeOut)) {
	return time.Duration(1.3*float64(routerTimeOut)) + time.Duration(timeGained*float64(time.Second)/float64(noOfWorkers))
}

func getMinMaxWorkspaceLatency(workspacesWithJobs []string, latencyMap map[string]misc.MovingAverage) (float64, float64) {
	minLatency := math.MaxFloat64
	maxLatency := -math.MaxFloat64

	//Below two loops, normalize the values and compute the score of each workspace
	for _, workspaceKey := range workspacesWithJobs {
		if minLatency > latencyMap[workspaceKey].Value() {
			minLatency = latencyMap[workspaceKey].Value()
		}
		if maxLatency < latencyMap[workspaceKey].Value() {
			maxLatency = latencyMap[workspaceKey].Value()
		}
	}
	return minLatency, maxLatency
}

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceScoreList(workspacesWithJobs []string, maxLatency, minLatency float64, latencyMap map[string]misc.MovingAverage, destType string) []workspaceScore {
	scores := make([]workspaceScore, len(workspacesWithJobs))
	for i, workspaceKey := range workspacesWithJobs {
		scores[i] = workspaceScore{}
		latencyScore := 0.0
		if maxLatency-minLatency != 0 {
			latencyScore = (latencyMap[workspaceKey].Value() - minLatency) / (maxLatency - minLatency)
		}

		isDraining := 0.0
		if time.Since(multitenantStat.getLastDrainedTimestamp(workspaceKey, destType)) < 100*time.Second {
			isDraining = 1.0
		}

		scores[i].score = latencyScore + 100*isDraining
		scores[i].workspaceId = workspaceKey
	}

	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score < scores[j].score
	})
	return scores
}

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceSecondaryScoreList(workspacesWithJobs []string, workspacePickUpCount map[string]int, destType string, latencyMap map[string]misc.MovingAverage) []workspaceScore {
	//Sort by workspaces who can get to realtime quickly
	scores := make([]workspaceScore, len(workspacesWithJobs))
	for i, workspaceKey := range workspacesWithJobs {
		scores[i] = workspaceScore{}
		scores[i].workspaceId = workspaceKey

		workspaceCountKey, ok := multitenantStat.routerNonTerminalCounts["router"][workspaceKey]
		if !ok || workspaceCountKey[destType]-workspacePickUpCount[workspaceKey] <= 0 {
			scores[i].score = math.MaxFloat64
			scores[i].secondary_score = 0
			continue
		}
		if multitenantStat.getFailureRate(workspaceKey, destType) == 1 {
			scores[i].score = math.MaxFloat64
		} else {
			scores[i].score = float64(workspaceCountKey[destType]-workspacePickUpCount[workspaceKey]) * latencyMap[workspaceKey].Value() / (1 - multitenantStat.getFailureRate(workspaceKey, destType))
		}
		scores[i].secondary_score = float64(workspaceCountKey[destType] - workspacePickUpCount[workspaceKey])
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score == math.MaxFloat64 && scores[j].score == math.MaxFloat64 {
			return scores[i].secondary_score < scores[j].secondary_score
		}
		return scores[i].score < scores[j].score
	})
	return scores
}
