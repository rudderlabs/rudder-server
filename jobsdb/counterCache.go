package jobsdb

import (
	"strings"

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

// tablePrefix.dsIndex.workspaceID.customVal.sourceID.destinationID.jobState
func getSubject(cs cacheSubject) subject {
	return subject(
		cs.tablePrefix +
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
			wildCard(cs.jobState),
	)
}

func getComposites(cs cacheSubject) []string {
	return []string{
		wildCard(cs.tablePrefix),
		wildCard(cs.dsIndex),
		wildCard(cs.workspaceID),
		wildCard(cs.customVal),
		wildCard(cs.sourceID),
		wildCard(cs.destinationID),
		wildCard(cs.jobState),
	}
}

func getSubjectPowerSet(cs cacheSubject) []subject {
	composites := getComposites(cs)
	// need not take tablePrefix into consideration here because while all the
	// other composites may/may not be wildcards,
	// tablePrefix is always guaranteed to be non wildcard
	tablePrefix := composites[0]
	comps := composites[1:]
	nonWildCardCompIndices := lo.Filter(
		lo.Range(len(comps)),
		func(i, _ int) bool {
			return comps[i] != "*"
		},
	)
	if len(nonWildCardCompIndices) == 0 {
		return []subject{getSubject(cs)}
	}
	// get powerset (composites could be either wildcard or non-wildcard)
	indexPowerSet := powerSet(nonWildCardCompIndices)
	subjects := make([]subject, len(indexPowerSet))
	for i := range indexPowerSet {
		subSet := make([]string, len(composites))
		subSet[0] = tablePrefix
		for j := 0; j < len(comps); j++ {
			if lo.Contains(indexPowerSet[i], j) {
				subSet[j+1] = "*"
			} else {
				subSet[j+1] = comps[j]
			}
		}
		subjects[i] = subject(strings.Join(subSet, "."))
	}
	return subjects
}

func wildCard(atom string) string {
	if atom == "" {
		return "*"
	}
	return atom
}

type subject string

func (s subject) GetName() string {
	return string(s)
}

func (s subject) GetTags() map[string]string {
	return map[string]string{}
}

func powerSet[T any](s []T) [][]T {
	var ps [][]T
	ps = append(ps, []T{})
	for _, e := range s {
		setsWithE := make([][]T, len(ps))
		for i, subset := range ps {
			newSet := make([]T, 0)
			newSet = append(newSet, subset...)
			newSet = append(newSet, e)
			setsWithE[i] = newSet
		}
		ps = append(ps, setsWithE...)
	}
	return ps
}

var instance = metric.Instance.GetRegistry(metric.PublishedMetrics)

func increaseCount(cs cacheSubject, count int) {
	lo.ForEach(
		getSubjectPowerSet(cs),
		func(subject subject, _ int) {
			instance.MustGetGauge(subject).Add(float64(count))
		},
	)
}

func decreaseCount(cs cacheSubject, count int) {
	lo.ForEach(
		getSubjectPowerSet(cs),
		func(subject subject, _ int) {
			instance.MustGetGauge(subject).Sub(float64(count))
		},
	)
}

func jobsExist(cs cacheSubject) bool {
	return instance.MustGetGauge(getSubject(cs)).IntValue() > 0
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
	failedCountersMap := removeState(dsIndex, tablePrefix, Executing.State)
	for sub, count := range failedCountersMap {
		instance.MustGetGauge(
			subject(
				strings.Replace(sub, Executing.State, Failed.State, 1),
			),
		).Add(float64(count))
	}
}

func deleteExecuting(dsIndex, tablePrefix string) {
	executingCountsMap := removeState(dsIndex, tablePrefix, Executing.State)
	for sub, count := range executingCountsMap {
		instance.MustGetGauge(
			subject(
				strings.Replace(sub, Executing.State, NotProcessed.State, 1),
			),
		).Add(float64(count))
	}
}

// clear termnial counters for old DS
//
// update non-terminal counters from old to migrated(new) DS
func postMigrationCounterUpdate(oldDS, newDS, tablePrefix string) {
	for _, state := range validTerminalStates {
		_ = removeState(oldDS, tablePrefix, state)
	}
	for _, state := range validNonTerminalStates {
		clearedCountsMap := removeState(oldDS, tablePrefix, state)
		for sub, count := range clearedCountsMap {
			instance.MustGetGauge(
				subject(
					strings.Replace(sub, oldDS, newDS, 1),
				),
			).Add(float64(count))
		}
	}
}

// sets count to 0 for all counters with the given dsIndex, tablePrefix and jobState
//
// and returns a map of the counters that were cleared
func removeState(dsIndex, tablePrefix, jobState string) map[string]int {
	subjectPrefix := tablePrefix + "." + dsIndex
	clearedCountsMap := make(map[string]int)
	instance.Range(
		func(key, value interface{}) bool {
			if m, ok := key.(subject); ok {
				if gauge, ok := value.(metric.Gauge); ok {
					subjectString := string(m)
					if strings.HasPrefix(subjectString, subjectPrefix) &&
						strings.HasSuffix(subjectString, jobState) {
						preStatusDeleteCount := gauge.IntValue()
						clearedCountsMap[subjectString] = preStatusDeleteCount
						gauge.Set(0)
					}
				}
			}
			return true
		},
	)
	return clearedCountsMap
}
