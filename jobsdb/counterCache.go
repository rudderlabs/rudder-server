package jobsdb

import (
	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/samber/lo"
)

// cache subject structure
// workspaceID.sourceID.destinationID.jobState

type cacheSubject struct {
	tablePrefix   string
	dsIndex       string
	workspaceID   string
	customVal     string
	sourceID      string
	destinationID string
	jobState      string
}

func (cs cacheSubject) GetName() string {
	return cs.tablePrefix +
		"." +
		wildCard(cs.dsIndex) +
		"." +
		wildCard(cs.workspaceID) +
		"." +
		wildCard(cs.customVal) +
		"." +
		wildCard(cs.sourceID) +
		"." +
		wildCard(cs.destinationID) +
		"." +
		wildCard(cs.jobState)
}

func (cs cacheSubject) GetTags() map[string]string {
	return map[string]string{
		"tablePrefix":   cs.tablePrefix,
		"dsIndex":       cs.dsIndex,
		"workspaceID":   cs.workspaceID,
		"customVal":     cs.customVal,
		"sourceID":      cs.sourceID,
		"destinationID": cs.destinationID,
		"jobState":      cs.jobState,
	}
}

func getCacheSubjectPowerSet(cs cacheSubject) []cacheSubject {
	var (
		dsIndices    []string
		workspaceIDS []string
		customVals   []string
		sourceIDS    []string
		destIDS      []string
		jobStates    []string
	)
	if cs.dsIndex == "" {
		dsIndices = []string{"*"}
	} else {
		dsIndices = []string{"*", cs.dsIndex}
	}
	if cs.workspaceID == "" {
		workspaceIDS = []string{"*"}
	} else {
		workspaceIDS = []string{"*", cs.workspaceID}
	}
	if cs.customVal == "" {
		customVals = []string{"*"}
	} else {
		customVals = []string{"*", cs.customVal}
	}
	if cs.sourceID == "" {
		sourceIDS = []string{"*"}
	} else {
		sourceIDS = []string{"*", cs.sourceID}
	}
	if cs.destinationID == "" {
		destIDS = []string{"*"}
	} else {
		destIDS = []string{"*", cs.destinationID}
	}
	if cs.jobState == "" {
		jobStates = []string{"*"}
	} else {
		jobStates = []string{"*", cs.jobState}
	}
	var cacheSubjects []cacheSubject
	for _, dsIndex := range dsIndices {
		for _, workspaceID := range workspaceIDS {
			for _, customVal := range customVals {
				for _, sourceID := range sourceIDS {
					for _, destID := range destIDS {
						for _, jobState := range jobStates {
							cacheSubjects = append(cacheSubjects, cacheSubject{
								tablePrefix:   cs.tablePrefix,
								dsIndex:       dsIndex,
								workspaceID:   workspaceID,
								customVal:     customVal,
								sourceID:      sourceID,
								destinationID: destID,
								jobState:      jobState,
							})
						}
					}
				}
			}
		}
	}
	return cacheSubjects
}

func wildCard(atom string) string {
	if atom == "" {
		return "*"
	}
	return atom
}

var instance = metric.Instance.GetRegistry(metric.PublishedMetrics)

func increaseCount(cs cacheSubject, count int) {
	lo.ForEach(
		getCacheSubjectPowerSet(cs),
		func(subject cacheSubject, _ int) {
			instance.MustGetGauge(subject).Add(float64(count))
		},
	)
}

func decreaseCount(cs cacheSubject, count int) {
	lo.ForEach(
		getCacheSubjectPowerSet(cs),
		func(subject cacheSubject, _ int) {
			instance.MustGetGauge(subject).Sub(float64(count))
		},
	)
}

func jobsExist(cs cacheSubject) bool {
	return getJobCount(cs) > 0
}

func getJobCount(cs cacheSubject) int {
	return instance.MustGetGauge(setWildCardInCacheSubject(cs)).IntValue()
}

func setWildCardInCacheSubject(cs cacheSubject) cacheSubject {
	if cs.dsIndex == "" {
		cs.dsIndex = "*"
	}
	if cs.workspaceID == "" {
		cs.workspaceID = "*"
	}
	if cs.customVal == "" {
		cs.customVal = "*"
	}
	if cs.sourceID == "" {
		cs.sourceID = "*"
	}
	if cs.destinationID == "" {
		cs.destinationID = "*"
	}
	if cs.jobState == "" {
		cs.jobState = "*"
	}
	return cs
}

// check counters and return true if any of the following is non-zero
//
// otherwise return false
func checkIfJobsExist(
	tablePrefix,
	dsIndex,
	workspaceID string,
	customVals []string,
	sources []string,
	destinations []string,
	states []string,
) bool {
	if workspaceID == "" {
		workspaceID = "*"
	}
	subjects := getLookupSubjects(
		tablePrefix,
		dsIndex,
		workspaceID,
		customVals,
		sources,
		destinations,
		states,
	)
	for _, subject := range subjects {
		if jobsExist(subject) {
			return true
		}
	}

	// ideally checking for any one of the most granular level should be enough?
	return false
}

func checkAndSetWildcard(compositeList []string) []string {
	if len(compositeList) == 0 {
		return []string{"*"}
	}
	return compositeList
}

func getLookupSubjects(
	tablePrefix,
	dsIndex,
	workspaceID string,
	customVals []string,
	sources []string,
	destinations []string,
	states []string,
) []cacheSubject {
	customVals = checkAndSetWildcard(customVals)
	sources = checkAndSetWildcard(sources)
	destinations = checkAndSetWildcard(destinations)
	states = checkAndSetWildcard(states)
	subjects := make([]cacheSubject, 0)
	for _, customVal := range customVals {
		for _, source := range sources {
			for _, destination := range destinations {
				for _, state := range states {
					subjects = append(subjects, cacheSubject{
						tablePrefix:   tablePrefix,
						dsIndex:       dsIndex,
						workspaceID:   workspaceID,
						customVal:     customVal,
						sourceID:      source,
						destinationID: destination,
						jobState:      state,
					},
					)
				}
			}
		}
	}
	return subjects
}

// for when direct queries are made against the database tables
// migration/fail executing/ delete executing

func failExecuting(dsIndex, tablePrefix string) {
	// this sets all DS-executing counters to 0 and returns previous counts
	failedCountersMap := removeState(dsIndex, tablePrefix, Executing.State)
	for sub, count := range failedCountersMap {
		// add to DS failed counter
		sub.jobState = Failed.State
		instance.MustGetGauge(sub).Add(float64(count))
		// add to wildcard DS failed counter
		sub.dsIndex = "*"
		instance.MustGetGauge(sub).Add(float64(count))
		// subtract from wildcard DS executing counter
		sub.jobState = Executing.State
		instance.MustGetGauge(sub).Sub(float64(count))
	}
}

func deleteExecuting(dsIndex, tablePrefix string) {
	// this sets all DS-executing counters to 0 and returns previous counts
	executingCountsMap := removeState(dsIndex, tablePrefix, Executing.State)
	for sub, count := range executingCountsMap {
		// add to DS NotPickedYet counter
		sub.jobState = NotProcessed.State
		instance.MustGetGauge(sub).Add(float64(count))
		// add to wildcard DS NotPickedYet counter
		sub.dsIndex = "*"
		instance.MustGetGauge(sub).Add(float64(count))
		// subtract from wildcard DS executing counter
		sub.jobState = Executing.State
		instance.MustGetGauge(sub).Sub(float64(count))
	}
}

// for terminal jobs - clear those jobs entirely from everywhere
//
// update non-terminal counters from old to migrated(new) DS
func postMigrationCounterUpdate(oldDSIndex, newDSIndex, tablePrefix string) {
	for _, state := range validTerminalStates {
		clearedTerminalCounts := removeState(oldDSIndex, tablePrefix, state)
		for sub, count := range clearedTerminalCounts {
			// clear wildcard DS state
			sub.dsIndex = "*"
			instance.MustGetGauge(sub).Sub(float64(count))
			// clear old DS wildcard states
			sub.dsIndex = oldDSIndex
			sub.jobState = "*"
			instance.MustGetGauge(sub).Sub(float64(count))
			// clear wildcard DS wildcard states
			sub.dsIndex = "*"
			instance.MustGetGauge(sub).Sub(float64(count))
		}
	}
	for _, state := range validNonTerminalStates {
		clearedNonTerminalCountsMap := removeState(oldDSIndex, tablePrefix, state)
		for sub, count := range clearedNonTerminalCountsMap {
			// clear Old DS wildcard state
			instance.MustGetGauge(sub).Sub(float64(count))
			// add to new DS state
			sub.dsIndex = newDSIndex
			instance.MustGetGauge(sub).Add(float64(count))
			// add to new DS wildcard states
			sub.jobState = "*"
			instance.MustGetGauge(sub).Add(float64(count))
		}
	}
}

// sets count to 0 for all counters with the given dsIndex, tablePrefix and jobState
//
// and returns a map of the counters that were cleared
func removeState(dsIndex, tablePrefix, jobState string) map[cacheSubject]int {
	clearedCountsMap := make(map[cacheSubject]int)
	instance.Range(
		func(key, value interface{}) bool {
			if m, ok := key.(cacheSubject); ok {
				if gauge, ok := value.(metric.Gauge); ok {
					if m.tablePrefix == tablePrefix &&
						m.dsIndex == dsIndex &&
						m.jobState == jobState {
						preStatusDeleteCount := gauge.IntValue()
						clearedCountsMap[m] = preStatusDeleteCount
						gauge.Set(0)
					}
				}
			}
			return true
		},
	)
	return clearedCountsMap
}

// TODO
//
// ignore MinDSRetentionPeriod, MaxDSRetentionPeriod?
func checkIfMigrateDS(dsIndex, tablePrefix string) bool {
	allJobsCount := getJobCount(cacheSubject{
		tablePrefix: tablePrefix,
		dsIndex:     dsIndex,
	})
	if allJobsCount == 0 {
		return false
	}
	var terminalJobsCount int
	for _, state := range validTerminalStates {
		terminalJobsCount += getJobCount(cacheSubject{
			tablePrefix: tablePrefix,
			dsIndex:     dsIndex,
			jobState:    state,
		})
	}
	// TODO
	return false
}
