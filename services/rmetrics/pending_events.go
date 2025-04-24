package rmetrics

import (
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/stats/metric"
)

const (
	JobsdbPendingEventsCount = "jobsdb_%s_pending_events_count"
	All                      = "ALL"
)

type (
	DecreasePendingEventsFunc func(tablePrefix, workspace, destType string, value float64)
	IncreasePendingEventsFunc func(tablePrefix, workspace, destType string, value float64)
)

// PendingEventsRegistry is a registry for pending events metrics
type PendingEventsRegistry interface {
	// IncreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
	IncreasePendingEvents(tablePrefix, workspace, destType string, value float64)
	// DecreasePendingEvents decrements three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
	DecreasePendingEvents(tablePrefix, workspace, destType string, value float64)
	// PendingEvents gets the measurement for pending events metric
	PendingEvents(tablePrefix, workspace, destType string) metric.Gauge
	// Publish publishes the metrics to the global published metrics registry
	Publish()
	// Reset resets the registry to a new, non published one and clears the global published metrics registry
	Reset()
}

type Option func(*pendingEventsRegistry)

// WithPublished creates a registry that writes metrics to the global published metrics registry, without having to call Publish first.
func WithPublished() Option {
	return func(per *pendingEventsRegistry) {
		per.published = true
		per.registry = metric.Instance.GetRegistry(metric.PublishedMetrics)
	}
}

// NewPendingEventsRegistry creates a new PendingEventsRegistry. By default, metrics are not published to the global published metrics registry, until [Publish] is called.
func NewPendingEventsRegistry(opts ...Option) PendingEventsRegistry {
	per := &pendingEventsRegistry{
		registry: metric.NewRegistry(),
	}
	for _, opt := range opts {
		opt(per)
	}
	return per
}

type pendingEventsRegistry struct {
	registryMu sync.RWMutex
	published  bool
	registry   metric.Registry
}

// IncreasePendingEvents increments three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func (pem *pendingEventsRegistry) IncreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	pem.registryMu.RLock()
	defer pem.registryMu.RUnlock()

	pem.PendingEvents(tablePrefix, workspace, destType).Add(value)
	pem.PendingEvents(tablePrefix, All, destType).Add(value)
	pem.PendingEvents(tablePrefix, All, All).Add(value)
	pem.registry.MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Add(value)
	pem.registry.MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Add(value)
}

// DecreasePendingEvents decrements three gauges, the dest & workspace-specific gauge, plus two aggregate (global) gauges
func (pem *pendingEventsRegistry) DecreasePendingEvents(tablePrefix, workspace, destType string, value float64) {
	pem.registryMu.RLock()
	defer pem.registryMu.RUnlock()
	pem.PendingEvents(tablePrefix, workspace, destType).Sub(value)
	pem.PendingEvents(tablePrefix, All, destType).Sub(value)
	pem.PendingEvents(tablePrefix, All, All).Sub(value)
	pem.registry.MustGetGauge(pendingEventsMeasurementAll{tablePrefix, destType}).Sub(value)
	pem.registry.MustGetGauge(pendingEventsMeasurementAll{tablePrefix, All}).Sub(value)
}

// PendingEvents gets the measurement for pending events metric
func (pem *pendingEventsRegistry) PendingEvents(tablePrefix, workspace, destType string) metric.Gauge {
	return pem.registry.MustGetGauge(newPendingEventsMeasurement(tablePrefix, workspace, destType))
}

// Publish publishes the metrics to the global published metrics registry
func (pem *pendingEventsRegistry) Publish() {
	pem.registryMu.Lock()
	defer pem.registryMu.Unlock()
	if pem.published {
		return
	}
	pem.published = true

	publishedRegistry := metric.Instance.GetRegistry(metric.PublishedMetrics)
	pem.registry.Range(func(key, value any) bool { // copy all gauge metrics to the published registry
		m := key.(metric.Measurement)
		switch value := value.(type) {
		case metric.Gauge:
			publishedRegistry.MustGetGauge(m).Set(value.Value())
		}
		return true
	})
	pem.registry = publishedRegistry
}

// Reset resets the registry to a new, non published one and clears the global published metrics registry
func (pem *pendingEventsRegistry) Reset() {
	pem.registryMu.Lock()
	defer pem.registryMu.Unlock()
	pem.registry = metric.NewRegistry()
	pem.published = false
	metric.Instance.Reset()
}

func newPendingEventsMeasurement(tablePrefix, workspace, destType string) metric.Measurement {
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
