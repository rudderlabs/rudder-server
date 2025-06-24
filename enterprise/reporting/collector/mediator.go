package collector

import (
	"context"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-go-kit/config"
	. "github.com/rudderlabs/rudder-server/utils/tx"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// MetricsCollectorMediator forwards OutMetricEvent to all enabled MetricsCollectors
type MetricsCollectorMediator struct {
	collectors []MetricsCollector
	mu         sync.RWMutex
}

// NewMetricsCollectorMediator creates a new MetricsCollector mediator with enabled collectors based on configuration
// and initializes it with the given stage details and input events
func NewMetricsCollectorMediator(stageDetails reportingtypes.StageDetails, inputEvents []*reportingtypes.InMetricEvent, conf *config.Config) *MetricsCollectorMediator {
	var collectors []MetricsCollector

	// Check if default metrics collection is enabled
	if conf.GetBoolVar(true, "Reporting.defaultMetrics.enabled") {
		collectors = append(collectors, NewDefaultMetricsCollector(stageDetails, inputEvents, conf))
	}

	// Check if error details collection is enabled
	if conf.GetBoolVar(false, "Reporting.errorDetails.enabled") {
		collectors = append(collectors, NewErrorDetailsMetricsCollector(stageDetails, inputEvents, conf))
	}

	// Check if MTU collection is enabled
	if conf.GetBoolVar(false, "Reporting.mtu.enabled") {
		collectors = append(collectors, NewMTUMetricsCollector(stageDetails, inputEvents, conf))
	}

	// Check if DataMapper collection is enabled
	if conf.GetBoolVar(false, "Reporting.dataMapper.enabled") {
		collectors = append(collectors, NewDataMapperCollector(stageDetails, inputEvents, conf))
	}

	mediator := &MetricsCollectorMediator{
		collectors: collectors,
	}

	return mediator
}

// AddCollector adds a new collector to the mediator
func (m *MetricsCollectorMediator) AddCollector(collector MetricsCollector) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.collectors = append(m.collectors, collector)
}

// CurrentStage returns the current stage (all collectors should be in the same stage)
func (m *MetricsCollectorMediator) CurrentStage() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.collectors) == 0 {
		return ""
	}
	return m.collectors[0].CurrentStage()
}

// Collect forwards the events to all collectors
func (m *MetricsCollectorMediator) Collect(events ...*reportingtypes.OutMetricEvent) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, collector := range m.collectors {
		if err := collector.Collect(events...); err != nil {
			return err
		}
	}
	return nil
}

// NextStage advances all collectors to the next stage
func (m *MetricsCollectorMediator) NextStage(stageDetails reportingtypes.StageDetails) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, collector := range m.collectors {
		if err := collector.NextStage(stageDetails); err != nil {
			return err
		}
	}
	return nil
}

// End marks all collectors as complete
func (m *MetricsCollectorMediator) End() error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, collector := range m.collectors {
		if err := collector.End(); err != nil {
			return err
		}
	}
	return nil
}

// Merge merges this mediator with another one
func (m *MetricsCollectorMediator) Merge(other MetricsCollector) (MetricsCollector, error) {
	otherMediator, ok := other.(*MetricsCollectorMediator)
	if !ok {
		return nil, fmt.Errorf("cannot merge with non-mediator collector")
	}

	m.mu.RLock()
	otherMediator.mu.RLock()
	defer m.mu.RUnlock()
	defer otherMediator.mu.RUnlock()

	mergedCollectors := make([]MetricsCollector, 0, len(m.collectors)+len(otherMediator.collectors))
	mergedCollectors = append(mergedCollectors, m.collectors...)
	mergedCollectors = append(mergedCollectors, otherMediator.collectors...)

	return &MetricsCollectorMediator{
		collectors: mergedCollectors,
	}, nil
}

// Flush flushes all collectors
func (m *MetricsCollectorMediator) Flush(ctx context.Context, tx *Tx) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, collector := range m.collectors {
		if err := collector.Flush(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}
