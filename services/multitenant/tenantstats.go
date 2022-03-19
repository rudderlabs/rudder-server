//go:generate mockgen -destination=../../mocks/services/multitenant/mock_tenantstats.go -package mock_tenantstats github.com/rudderlabs/rudder-server/services/multitenant MultiTenantI

package multitenant

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/services/metric"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	pkgLogger logger.LoggerI
)

type MultitenantStatsT struct {
	routerJobCountMutex sync.RWMutex
	// routerInputRates: dbPrefix, workspace, desType, measurement
	routerInputRates map[string]map[string]map[string]metric.MovingAverage
	// lastDrainedTimestamps: workspace, destType
	lastDrainedTimestamps map[string]map[string]time.Time
	// failureRate: workspace, destType
	failureRate            map[string]map[string]metric.MovingAverage
	routerSuccessRateMutex sync.RWMutex
	// routerTenantLatencyStat: destType, workspace, measurement
	routerTenantLatencyStat map[string]map[string]metric.MovingAverage
	routerLatencyMutex      sync.RWMutex
	processorStageTime      time.Time
}

type MultiTenantI interface {
	CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool)
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)
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

func NewStats(routerDBs map[string]jobsdb.MultiTenantJobsDB) *MultitenantStatsT {
	multitenantStat := MultitenantStatsT{}
	multitenantStat.routerInputRates = make(map[string]map[string]map[string]metric.MovingAverage)
	multitenantStat.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	multitenantStat.failureRate = make(map[string]map[string]metric.MovingAverage)
	for dbPrefix := range routerDBs {
		multitenantStat.routerInputRates[dbPrefix] = make(map[string]map[string]metric.MovingAverage)
		pileUpStatMap := make(map[string]map[string]int)
		routerDBs[dbPrefix].GetPileUpCounts(pileUpStatMap)
		for workspace := range pileUpStatMap {
			for destType := range pileUpStatMap[workspace] {
				metric.GetPendingEventsMeasurement(dbPrefix, workspace, destType).Add(float64(pileUpStatMap[workspace][destType]))
			}
		}
	}

	multitenantStat.routerTenantLatencyStat = make(map[string]map[string]metric.MovingAverage)

	multitenantStat.processorStageTime = time.Now()

	return &multitenantStat
}

func (multitenantStat *MultitenantStatsT) UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64) {
	multitenantStat.routerLatencyMutex.Lock()
	defer multitenantStat.routerLatencyMutex.Unlock()
	_, ok := multitenantStat.routerTenantLatencyStat[destType]
	if !ok {
		multitenantStat.routerTenantLatencyStat[destType] = make(map[string]metric.MovingAverage)
	}
	_, ok = multitenantStat.routerTenantLatencyStat[destType][workspaceID]
	if !ok {
		multitenantStat.routerTenantLatencyStat[destType][workspaceID] = metric.NewMovingAverage(metric.AVG_METRIC_AGE)
	}
	multitenantStat.routerTenantLatencyStat[destType][workspaceID].Add(val)
}

func (multitenantStat *MultitenantStatsT) CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool) {
	multitenantStat.routerSuccessRateMutex.Lock()
	defer multitenantStat.routerSuccessRateMutex.Unlock()

	_, ok := multitenantStat.failureRate[workspace]
	if !ok {
		multitenantStat.failureRate[workspace] = make(map[string]metric.MovingAverage)
	}
	_, ok = multitenantStat.failureRate[workspace][destType]
	if !ok {
		multitenantStat.failureRate[workspace][destType] = metric.NewMovingAverage(metric.AVG_METRIC_AGE)
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

func (multitenantStat *MultitenantStatsT) ReportProcLoopAddStats(stats map[string]map[string]int, dbPrefix string) {
	multitenantStat.routerJobCountMutex.Lock()
	defer multitenantStat.routerJobCountMutex.Unlock()

	timeTaken := time.Since(multitenantStat.processorStageTime)
	for key := range stats {
		_, ok := multitenantStat.routerInputRates[dbPrefix][key]
		if !ok {
			multitenantStat.routerInputRates[dbPrefix][key] = make(map[string]metric.MovingAverage)
		}
		for destType := range stats[key] {
			_, ok := multitenantStat.routerInputRates[dbPrefix][key][destType]
			if !ok {
				multitenantStat.routerInputRates[dbPrefix][key][destType] = metric.NewMovingAverage()
			}
			multitenantStat.routerInputRates[dbPrefix][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
			metric.GetPendingEventsMeasurement(dbPrefix, key, destType).Add(float64(stats[key][destType]))
		}
	}
	for workspaceKey := range multitenantStat.routerInputRates[dbPrefix] {
		_, ok := stats[workspaceKey]
		if !ok {
			for destType := range multitenantStat.routerInputRates[workspaceKey] {
				multitenantStat.routerInputRates[dbPrefix][workspaceKey][destType].Add(0)
			}
		} else {
			for destType := range multitenantStat.routerInputRates[dbPrefix][workspaceKey] {
				_, ok := stats[workspaceKey][destType]
				if !ok {
					multitenantStat.routerInputRates[dbPrefix][workspaceKey][destType].Add(0)
				}
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
		workspaceCountKey, ok := multitenantStat.routerInputRates["rt"][workspaceKey]
		if ok {
			destTypeCount, ok := workspaceCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					//Adding BETA
					if metric.GetPendingEventsMeasurement("rt", workspaceKey, destType).Value() > 0 {
						usedLatencies[workspaceKey] = multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
						workspacePickUpCount[workspaceKey] = 1
					}
					continue
				}
				//TODO : Get rid of unReliableLatencyORInRate hack
				unReliableLatencyORInRate := false
				pendingEvents := metric.GetPendingEventsMeasurement("rt", workspaceKey, destType).IntValue()
				if multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 //Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate workspace %v latency value %v in rate %v", destType, workspaceKey, multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					workspacePickUpCount[workspaceKey] = tmpPickCount
					if workspacePickUpCount[workspaceKey] > pendingEvents {
						workspacePickUpCount[workspaceKey] = misc.MaxInt(pendingEvents, 0)
					}
				} else {
					workspacePickUpCount[workspaceKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), pendingEvents)
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
		pendingEvents := metric.GetPendingEventsMeasurement("rt", workspaceKey, destType).IntValue()
		if pendingEvents <= 0 {
			continue
		}
		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value() == 0 {
			pickUpCount = misc.MinInt(pendingEvents-workspacePickUpCount[workspaceKey], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), pendingEvents-workspacePickUpCount[workspaceKey])
		}
		usedLatencies[workspaceKey] = multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()
		workspacePickUpCount[workspaceKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,PileUpLoop ", float64(pickUpCount)*multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), runningTimeCounter, workspaceKey, runningJobCount, multitenantStat.routerTenantLatencyStat[destType][workspaceKey].Value(), pendingEvents)
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

func (multitenantStat *MultitenantStatsT) getWorkspacesWithPendingJobs(destType string, latencyMap map[string]metric.MovingAverage) []string {
	workspacesWithJobs := make([]string, 0)
	for workspaceKey := range latencyMap {
		val := metric.GetPendingEventsMeasurement("rt", workspaceKey, destType).IntValue()
		if val > 0 {
			workspacesWithJobs = append(workspacesWithJobs, workspaceKey)
		} else if val < 0 {
			pkgLogger.Errorf("ws: %s, val: %d", workspaceKey, val)
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

func getMinMaxWorkspaceLatency(workspacesWithJobs []string, latencyMap map[string]metric.MovingAverage) (float64, float64) {
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

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceScoreList(workspacesWithJobs []string, maxLatency, minLatency float64, latencyMap map[string]metric.MovingAverage, destType string) []workspaceScore {
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

func (multitenantStat *MultitenantStatsT) getSortedWorkspaceSecondaryScoreList(workspacesWithJobs []string, workspacePickUpCount map[string]int, destType string, latencyMap map[string]metric.MovingAverage) []workspaceScore {
	//Sort by workspaces who can get to realtime quickly
	scores := make([]workspaceScore, len(workspacesWithJobs))
	for i, workspaceKey := range workspacesWithJobs {
		scores[i] = workspaceScore{}
		scores[i].workspaceId = workspaceKey
		pendingEvents := metric.GetPendingEventsMeasurement("rt", workspaceKey, destType).IntValue()
		if pendingEvents-workspacePickUpCount[workspaceKey] <= 0 {
			scores[i].score = math.MaxFloat64
			scores[i].secondary_score = 0
			continue
		}
		if multitenantStat.getFailureRate(workspaceKey, destType) == 1 {
			scores[i].score = math.MaxFloat64
		} else {
			scores[i].score = float64(pendingEvents-workspacePickUpCount[workspaceKey]) * latencyMap[workspaceKey].Value() / (1 - multitenantStat.getFailureRate(workspaceKey, destType))
		}
		scores[i].secondary_score = float64(pendingEvents - workspacePickUpCount[workspaceKey])
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score == math.MaxFloat64 && scores[j].score == math.MaxFloat64 {
			return scores[i].secondary_score < scores[j].secondary_score
		}
		return scores[i].score < scores[j].score
	})
	return scores
}
