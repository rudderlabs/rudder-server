package metric

import (
	"fmt"
)

// Measurement acts as a key in a Registry.
type Measurement interface {
	// GetName gets the name of the measurement
	GetName() string
	// GetTags gets the tags of the measurement
	GetTags() map[string]string
}

const (
	JOBSDB_PENDING_EVENTS_COUNT = "jobsdb_%s_pending_events_count"
	ALL                         = "ALL"
)

// IncreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func IncreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Add(value)
	PendingEvents(tablePrefix, ALL, destType).Add(value)
	PendingEvents(tablePrefix, ALL, ALL).Add(value)
}

// DecreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func DecreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Sub(value)
	PendingEvents(tablePrefix, ALL, destType).Sub(value)
	PendingEvents(tablePrefix, ALL, ALL).Sub(value)
}

// PendingEvents gets the measurement for pending events metric
func PendingEvents(tablePrefix, workspace, destType string) Gauge {
	return Instance.GetRegistry(PUBLISHED_METRICS).MustGetGauge(PendingEventsMeasurement(tablePrefix, workspace, destType))
}

func PendingEventsMeasurement(tablePrefix, workspace, destType string) Measurement {
	return pendingEventsMeasurement{tablePrefix, workspace, destType}
}

type pendingEventsMeasurement struct {
	tablePrefix string
	workspace   string
	destType    string
}

func (r pendingEventsMeasurement) GetName() string {
	return fmt.Sprintf(JOBSDB_PENDING_EVENTS_COUNT, r.tablePrefix)
}

func (r pendingEventsMeasurement) GetTags() map[string]string {
	res := map[string]string{
		"workspace":   r.workspace,
		"workspaceId": r.workspace,
		"destType":    r.destType,
	}
	return res
}
