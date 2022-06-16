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

type Stats struct {
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
	// have DBs also
	RouterDBs map[string]jobsdb.MultiTenantJobsDB
}

type MultiTenantI interface {
	CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool)
	GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64)
	ReportProcLoopAddStats(stats map[string]map[string]int, tableType string)
	UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64)
	lifecycle
}

type lifecycle interface {
	Start()
	Stop()
}

func (*Stats) Stop() {
	// reset the store sync map
	metric.GetManager().Reset()
}

func (t *Stats) Start() {
	t.routerInputRates = make(map[string]map[string]map[string]metric.MovingAverage)
	t.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	t.failureRate = make(map[string]map[string]metric.MovingAverage)
	for dbPrefix := range t.RouterDBs {
		t.routerInputRates[dbPrefix] = make(map[string]map[string]metric.MovingAverage)
		pileUpStatMap := make(map[string]map[string]int)
		t.RouterDBs[dbPrefix].GetPileUpCounts(pileUpStatMap)
		for workspace := range pileUpStatMap {
			for destType := range pileUpStatMap[workspace] {
				metric.IncreasePendingEvents(dbPrefix, workspace, destType, float64(pileUpStatMap[workspace][destType]))
			}
		}
	}

	t.routerTenantLatencyStat = make(map[string]map[string]metric.MovingAverage)
	t.processorStageTime = time.Now()
}

type workspaceScore struct {
	score          float64
	secondaryScore float64
	workspaceId    string
}

func Init() {
	pkgLogger = logger.NewLogger().Child("services").Child("multiTenant")
}

func NewStats(routerDBs map[string]jobsdb.MultiTenantJobsDB) *Stats {
	t := Stats{}
	t.routerInputRates = make(map[string]map[string]map[string]metric.MovingAverage)
	t.lastDrainedTimestamps = make(map[string]map[string]time.Time)
	t.failureRate = make(map[string]map[string]metric.MovingAverage)
	t.RouterDBs = routerDBs
	for dbPrefix := range routerDBs {
		t.routerInputRates[dbPrefix] = make(map[string]map[string]metric.MovingAverage)
		pileUpStatMap := make(map[string]map[string]int)
		routerDBs[dbPrefix].GetPileUpCounts(pileUpStatMap)
		for workspace := range pileUpStatMap {
			for destType := range pileUpStatMap[workspace] {
				metric.IncreasePendingEvents(dbPrefix, workspace, destType, float64(pileUpStatMap[workspace][destType]))
			}
		}
	}

	t.routerTenantLatencyStat = make(map[string]map[string]metric.MovingAverage)

	t.processorStageTime = time.Now()

	return &t
}

func (t *Stats) UpdateWorkspaceLatencyMap(destType string, workspaceID string, val float64) {
	t.routerLatencyMutex.Lock()
	defer t.routerLatencyMutex.Unlock()
	_, ok := t.routerTenantLatencyStat[destType]
	if !ok {
		t.routerTenantLatencyStat[destType] = make(map[string]metric.MovingAverage)
	}
	_, ok = t.routerTenantLatencyStat[destType][workspaceID]
	if !ok {
		t.routerTenantLatencyStat[destType][workspaceID] = metric.NewMovingAverage(metric.AVG_METRIC_AGE)
	}
	t.routerTenantLatencyStat[destType][workspaceID].Add(val)
}

func (t *Stats) CalculateSuccessFailureCounts(workspace string, destType string, isSuccess bool, isDrained bool) {
	t.routerSuccessRateMutex.Lock()
	defer t.routerSuccessRateMutex.Unlock()

	_, ok := t.failureRate[workspace]
	if !ok {
		t.failureRate[workspace] = make(map[string]metric.MovingAverage)
	}
	_, ok = t.failureRate[workspace][destType]
	if !ok {
		t.failureRate[workspace][destType] = metric.NewMovingAverage(metric.AVG_METRIC_AGE)
	}

	if isSuccess {
		t.failureRate[workspace][destType].Add(0)
	} else if isDrained {

		_, ok := t.lastDrainedTimestamps[workspace]
		if !ok {
			t.lastDrainedTimestamps[workspace] = make(map[string]time.Time)
		}
		t.lastDrainedTimestamps[workspace][destType] = time.Now()
		t.failureRate[workspace][destType].Add(0)
	} else {
		t.failureRate[workspace][destType].Add(1)
	}
}

func (t *Stats) ReportProcLoopAddStats(stats map[string]map[string]int, dbPrefix string) {
	t.routerJobCountMutex.Lock()
	defer t.routerJobCountMutex.Unlock()

	timeTaken := time.Since(t.processorStageTime)
	for key := range stats {
		_, ok := t.routerInputRates[dbPrefix][key]
		if !ok {
			t.routerInputRates[dbPrefix][key] = make(map[string]metric.MovingAverage)
		}
		for destType := range stats[key] {
			_, ok := t.routerInputRates[dbPrefix][key][destType]
			if !ok {
				t.routerInputRates[dbPrefix][key][destType] = metric.NewMovingAverage()
			}
			t.routerInputRates[dbPrefix][key][destType].Add((float64(stats[key][destType]) * float64(time.Second)) / float64(timeTaken))
			metric.IncreasePendingEvents(dbPrefix, key, destType, float64(stats[key][destType]))
		}
	}
	for workspaceKey := range t.routerInputRates[dbPrefix] {
		_, ok := stats[workspaceKey]
		if !ok {
			for destType := range t.routerInputRates[workspaceKey] {
				t.routerInputRates[dbPrefix][workspaceKey][destType].Add(0)
			}
		} else {
			for destType := range t.routerInputRates[dbPrefix][workspaceKey] {
				_, ok := stats[workspaceKey][destType]
				if !ok {
					t.routerInputRates[dbPrefix][workspaceKey][destType].Add(0)
				}
			}
		}
	}
	t.processorStageTime = time.Now()
}

func (t *Stats) GetRouterPickupJobs(destType string, noOfWorkers int, routerTimeOut time.Duration, jobQueryBatchSize int, timeGained float64) (map[string]int, map[string]float64) {
	t.routerJobCountMutex.RLock()
	defer t.routerJobCountMutex.RUnlock()
	t.routerLatencyMutex.RLock()
	defer t.routerLatencyMutex.RUnlock()

	workspacesWithJobs := getWorkspacesWithPendingJobs(destType, t.routerTenantLatencyStat[destType])
	boostedRouterTimeOut := getBoostedRouterTimeOut(routerTimeOut, timeGained, noOfWorkers)
	//TODO: Also while allocating jobs to router workers, we need to assign so that sum of assigned jobs latency equals the timeout

	runningJobCount := jobQueryBatchSize
	runningTimeCounter := float64(noOfWorkers) * float64(boostedRouterTimeOut) / float64(time.Second)
	workspacePickUpCount := make(map[string]int)
	usedLatencies := make(map[string]float64)

	minLatency, maxLatency := getMinMaxWorkspaceLatency(workspacesWithJobs, t.routerTenantLatencyStat[destType])

	scores := t.getSortedWorkspaceScoreList(workspacesWithJobs, maxLatency, minLatency, t.routerTenantLatencyStat[destType], destType)
	//Latency sorted input rate pass
	for _, scoredWorkspace := range scores {
		workspaceKey := scoredWorkspace.workspaceId
		workspaceCountKey, ok := t.routerInputRates["rt"][workspaceKey]
		if ok {
			destTypeCount, ok := workspaceCountKey[destType]
			if ok {

				if runningJobCount <= 0 || runningTimeCounter <= 0 {
					//Adding BETA
					if metric.PendingEvents("rt", workspaceKey, destType).Value() > 0 {
						usedLatencies[workspaceKey] = t.routerTenantLatencyStat[destType][workspaceKey].Value()
						workspacePickUpCount[workspaceKey] = 1
					}
					continue
				}
				//TODO : Get rid of unReliableLatencyORInRate hack
				unReliableLatencyORInRate := false
				pendingEvents := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
				if t.routerTenantLatencyStat[destType][workspaceKey].Value() != 0 {
					tmpPickCount := int(math.Min(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second), runningTimeCounter/(t.routerTenantLatencyStat[destType][workspaceKey].Value())))
					if tmpPickCount < 1 {
						tmpPickCount = 1 //Adding BETA
						pkgLogger.Debugf("[DRAIN DEBUG] %v  checking for high latency/low in rate workspace %v latency value %v in rate %v", destType, workspaceKey, t.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value())
						unReliableLatencyORInRate = true
					}
					workspacePickUpCount[workspaceKey] = tmpPickCount
					if workspacePickUpCount[workspaceKey] > pendingEvents {
						workspacePickUpCount[workspaceKey] = misc.MaxInt(pendingEvents, 0)
					}
				} else {
					workspacePickUpCount[workspaceKey] = misc.MinInt(int(destTypeCount.Value()*float64(routerTimeOut)/float64(time.Second)), pendingEvents)
				}

				timeRequired := float64(workspacePickUpCount[workspaceKey]) * t.routerTenantLatencyStat[destType][workspaceKey].Value()
				if unReliableLatencyORInRate {
					timeRequired = 0
				}
				runningTimeCounter = runningTimeCounter - timeRequired
				runningJobCount = runningJobCount - workspacePickUpCount[workspaceKey]
				usedLatencies[workspaceKey] = t.routerTenantLatencyStat[destType][workspaceKey].Value()
				pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, routerInRate : %v ,DestType : %v,InRateLoop ", timeRequired, runningTimeCounter, workspaceKey, runningJobCount, t.routerTenantLatencyStat[destType][workspaceKey].Value(), destTypeCount.Value(), destType)
			}
		}
	}

	//Sort by workspaces who can get to realtime quickly
	secondaryScores := t.getSortedWorkspaceSecondaryScoreList(workspacesWithJobs, workspacePickUpCount, destType, t.routerTenantLatencyStat[destType])
	for _, scoredWorkspace := range secondaryScores {
		workspaceKey := scoredWorkspace.workspaceId
		pendingEvents := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
		if pendingEvents <= 0 {
			continue
		}
		//BETA already added in the above loop
		if runningJobCount <= 0 || runningTimeCounter <= 0 {
			break
		}

		pickUpCount := 0
		if t.routerTenantLatencyStat[destType][workspaceKey].Value() == 0 {
			pickUpCount = misc.MinInt(pendingEvents-workspacePickUpCount[workspaceKey], runningJobCount)
		} else {
			tmpCount := int(runningTimeCounter / t.routerTenantLatencyStat[destType][workspaceKey].Value())
			pickUpCount = misc.MinInt(misc.MinInt(tmpCount, runningJobCount), pendingEvents-workspacePickUpCount[workspaceKey])
		}
		usedLatencies[workspaceKey] = t.routerTenantLatencyStat[destType][workspaceKey].Value()
		workspacePickUpCount[workspaceKey] += pickUpCount
		runningJobCount = runningJobCount - pickUpCount
		runningTimeCounter = runningTimeCounter - float64(pickUpCount)*t.routerTenantLatencyStat[destType][workspaceKey].Value()

		pkgLogger.Debugf("Time Calculated : %v , Remaining Time : %v , Workspace : %v ,runningJobCount : %v , moving_average_latency : %v, pileUpCount : %v ,DestType : %v ,PileUpLoop ", float64(pickUpCount)*t.routerTenantLatencyStat[destType][workspaceKey].Value(), runningTimeCounter, workspaceKey, runningJobCount, t.routerTenantLatencyStat[destType][workspaceKey].Value(), pendingEvents, destType)
	}

	return workspacePickUpCount, usedLatencies

}

func (t *Stats) getFailureRate(workspaceKey string, destType string) float64 {
	t.routerSuccessRateMutex.RLock()
	defer t.routerSuccessRateMutex.RUnlock()
	_, ok := t.failureRate[workspaceKey]
	if ok {
		_, ok = t.failureRate[workspaceKey][destType]
		if ok {
			return t.failureRate[workspaceKey][destType].Value()
		}
	}
	return 0.0
}

func (t *Stats) getLastDrainedTimestamp(workspaceKey string, destType string) time.Time {
	t.routerSuccessRateMutex.RLock()
	defer t.routerSuccessRateMutex.RUnlock()
	destWiseMap, ok := t.lastDrainedTimestamps[workspaceKey]
	if !ok {
		return time.Time{}
	}
	lastDrainedTS, ok := destWiseMap[destType]
	if !ok {
		return time.Time{}
	}
	return lastDrainedTS
}

func getWorkspacesWithPendingJobs(destType string, latencyMap map[string]metric.MovingAverage) []string {
	workspacesWithJobs := make([]string, 0)
	for workspaceKey := range latencyMap {
		val := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
		if val > 0 {
			workspacesWithJobs = append(workspacesWithJobs, workspaceKey)
		} else if val < 0 {
			pkgLogger.Errorf("ws: %s, val: %d", workspaceKey, val)
		}
	}
	return workspacesWithJobs
}

func getBoostedRouterTimeOut(routerTimeOut time.Duration, timeGained float64, noOfWorkers int) time.Duration {
	//Add 30% to the time interval as exact difference leads to a catchup scenario, but this may cause to give some priority to pileup in the inRate pass
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

func (t *Stats) getSortedWorkspaceScoreList(workspacesWithJobs []string, maxLatency, minLatency float64, latencyMap map[string]metric.MovingAverage, destType string) []workspaceScore {
	scores := make([]workspaceScore, len(workspacesWithJobs))
	for i, workspaceKey := range workspacesWithJobs {
		scores[i] = workspaceScore{}
		latencyScore := 0.0
		if maxLatency-minLatency != 0 {
			latencyScore = (latencyMap[workspaceKey].Value() - minLatency) / (maxLatency - minLatency)
		}

		isDraining := 0.0
		if time.Since(t.getLastDrainedTimestamp(workspaceKey, destType)) < 100*time.Second {
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

func (t *Stats) getSortedWorkspaceSecondaryScoreList(workspacesWithJobs []string, workspacePickUpCount map[string]int, destType string, latencyMap map[string]metric.MovingAverage) []workspaceScore {
	//Sort by workspaces who can get to realtime quickly
	scores := make([]workspaceScore, len(workspacesWithJobs))
	for i, workspaceKey := range workspacesWithJobs {
		scores[i] = workspaceScore{}
		scores[i].workspaceId = workspaceKey
		pendingEvents := metric.PendingEvents("rt", workspaceKey, destType).IntValue()
		if pendingEvents-workspacePickUpCount[workspaceKey] <= 0 {
			scores[i].score = math.MaxFloat64
			scores[i].secondaryScore = 0
			continue
		}
		if t.getFailureRate(workspaceKey, destType) == 1 {
			scores[i].score = math.MaxFloat64
		} else {
			scores[i].score = float64(pendingEvents-workspacePickUpCount[workspaceKey]) * latencyMap[workspaceKey].Value() / (1 - t.getFailureRate(workspaceKey, destType))
		}
		scores[i].secondaryScore = float64(pendingEvents - workspacePickUpCount[workspaceKey])
	}

	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score == math.MaxFloat64 && scores[j].score == math.MaxFloat64 {
			return scores[i].secondaryScore < scores[j].secondaryScore
		}
		return scores[i].score < scores[j].score
	})
	return scores
}
