package rmetrics

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

const (
	JobsdbPendingEventsCount = "jobsdb_%s_pending_events_count"
	All                      = "ALL"
)

// IncreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func IncreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Add(value)
	PendingEvents(tablePrefix, All, destType).Add(value)
	PendingEvents(tablePrefix, All, All).Add(value)
	metric.Instance.GetRegistry(metric.PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Add(value)
	metric.Instance.GetRegistry(metric.PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Add(value)
}

// DecreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func DecreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	PendingEvents(tablePrefix, workspace, destType).Sub(value)
	PendingEvents(tablePrefix, All, destType).Sub(value)
	PendingEvents(tablePrefix, All, All).Sub(value)
	metric.Instance.GetRegistry(metric.PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Sub(value)
	metric.Instance.GetRegistry(metric.PublishedMetrics).MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Sub(value)
}

// PendingEvents gets the measurement for pending events metric
func PendingEvents(tablePrefix, workspace, destType string) metric.Gauge {
	return metric.Instance.GetRegistry(metric.PublishedMetrics).MustGetGauge(PendingEventsMeasurement(tablePrefix, workspace, destType))
}

func PendingEventsMeasurement(tablePrefix, workspace, destType string) metric.Measurement {
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
