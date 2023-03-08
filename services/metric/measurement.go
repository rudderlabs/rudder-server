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
	JobsdbPendingEventsCount = "jobsdb_%s_pending_events_count"
	All                      = "ALL"
)

// IncreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func IncreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Add(value)
	PendingEvents(tablePrefix, All, destType).Add(value)
	PendingEvents(tablePrefix, All, All).Add(value)
	Instance.GetRegistry(PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Add(value)
	Instance.GetRegistry(PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Add(value)
}

// DecreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func DecreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Sub(value)
	PendingEvents(tablePrefix, All, destType).Sub(value)
	PendingEvents(tablePrefix, All, All).Sub(value)
	Instance.GetRegistry(PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Sub(value)
	Instance.GetRegistry(PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Sub(value)
}

// PendingEvents gets the measurement for pending events metric
func PendingEvents(tablePrefix, workspace, destType string) Gauge {
	return Instance.GetRegistry(PublishedMetrics).MustGetGauge(PendingEventsMeasurement(tablePrefix, workspace, destType))
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
	return fmt.Sprintf(JobsdbPendingEventsCount, r.tablePrefix)
}

func (r pendingEventsMeasurement) GetTags() map[string]string {
	return map[string]string{
		"workspaceId": r.workspace,
		"destType":    r.destType,
	}
}

type pendingEventsMeasurementAll struct {
	tablePrefix string
	destType    string
}

func (r pendingEventsMeasurementAll) GetName() string {
	return fmt.Sprintf(JobsdbPendingEventsCount, r.tablePrefix) + "_all"
}

func (r pendingEventsMeasurementAll) GetTags() map[string]string {
	return map[string]string{
		"destType": r.destType,
	}
}
