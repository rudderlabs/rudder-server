package jobsdb

import (
	"strings"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
	"github.com/samber/lo"
)

// The struct fields need to be exposed to JSON package
type dataSetT struct {
	JobTable       string `json:"job"`
	JobStatusTable string `json:"status"`
	Index          string `json:"index"`
}

type dataSetRangeT struct {
	minJobID  int64
	maxJobID  int64
	startTime int64
	endTime   int64
	ds        dataSetT
}

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
	comps := composites[1:]
	nonWildCardCompIndices := lo.Filter(
		lo.Range(len(comps)),
		func(i int, _ int) bool {
			return comps[i] != "*"
		},
	)
	if len(nonWildCardCompIndices) == 0 {
		return []subject{getSubject(cs)}
	}
	// get powerset (composites could be either wildcard or non-wildcard)
	indexPowerSet := powerSet(nonWildCardCompIndices)
	subjects := make([]subject, len(indexPowerSet))
	subjects[0] = getSubject(cs)
	for i := range indexPowerSet[1:] { // first one is empty
		subSet := make([]string, len(composites))
		subSet[0] = composites[0]
		for j := range comps {
			if lo.Contains(indexPowerSet[i], j) {
				subSet[j] = "*"
			} else {
				subSet[j] = comps[j]
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

func increaseCount(cs cacheSubject, count int) {
	subjects := getSubjectPowerSet(cs)
	for _, subject := range subjects {
		metric.Instance.GetRegistry(metric.PublishedMetrics).
			MustGetGauge(subject).Add(float64(count))
	}
}

func decreaseCount(cs cacheSubject, count int) {
	subjects := getSubjectPowerSet(cs)
	for _, subject := range subjects {
		metric.Instance.GetRegistry(metric.PublishedMetrics).
			MustGetGauge(subject).Sub(float64(count))
	}
}

func getCount(cs cacheSubject) int {
	return metric.Instance.GetRegistry(metric.PublishedMetrics).
		MustGetGauge(getSubject(cs)).IntValue()
}
