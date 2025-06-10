package collector

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

type DefaultMetricsCollector struct {
	store *MetricsStore
}

func NewDefaultMetricsCollector() *DefaultMetricsCollector {
	return &DefaultMetricsCollector{
		store: NewMetricsStore(),
	}
}

func (c *DefaultMetricsCollector) CollectMultiple(events []*reportingtypes.MetricEvent) error {
	c.store.Lock()
	defer c.store.Unlock()

	for _, event := range events {
		if err := c.collect(event); err != nil {
			return err
		}
	}
	return nil
}

func (c *DefaultMetricsCollector) Collect(event *reportingtypes.MetricEvent) error {
	c.store.Lock()
	defer c.store.Unlock()
	return c.collect(event)
}

func (c *DefaultMetricsCollector) GetMetrics() []*reportingtypes.PUReportedMetric {
	c.store.RLock()
	defer c.store.RUnlock()

	metrics := make([]*reportingtypes.PUReportedMetric, 0)
	for key, connectionDetails := range c.store.connectionDetailsMap {
		for _, statusDetail := range c.store.statusDetailsMap[key] {
			metric := &reportingtypes.PUReportedMetric{
				ConnectionDetails: *connectionDetails,
				PUDetails:         *c.store.puDetailsMap[key],
				StatusDetail:      statusDetail,
			}
			metrics = append(metrics, metric)
		}
	}
	return metrics
}

func (c *DefaultMetricsCollector) Report() error {
	// We can write to outbox table from here
	return nil
}

func (c *DefaultMetricsCollector) collect(event *reportingtypes.MetricEvent) error {
	// Generate key for this combination
	key := generateMetricKey(event)

	// Update connection details
	if _, exists := c.store.connectionDetailsMap[key]; !exists {
		c.store.connectionDetailsMap[key] = &reportingtypes.ConnectionDetails{
			SourceID:                event.ConnectionLabels.SourceLabels.SourceID,
			SourceTaskRunID:         event.ConnectionLabels.SourceLabels.SourceTaskRunID,
			SourceJobID:             event.ConnectionLabels.SourceLabels.SourceJobID,
			SourceJobRunID:          event.ConnectionLabels.SourceLabels.SourceJobRunID,
			SourceDefinitionID:      event.ConnectionLabels.SourceLabels.SourceDefinitionID,
			SourceCategory:          event.ConnectionLabels.SourceLabels.SourceCategory,
			DestinationID:           event.ConnectionLabels.DestinationLabels.DestinationID,
			DestinationDefinitionID: event.ConnectionLabels.DestinationLabels.DestinationDefinitionID,
			TransformationID:        event.ConnectionLabels.TransformationLabels.TransformationID,
			TransformationVersionID: event.ConnectionLabels.TransformationLabels.TransformationVersionID,
			TrackingPlanID:          event.ConnectionLabels.TrackingPlanLabels.TrackingPlanID,
			TrackingPlanVersion:     event.ConnectionLabels.TrackingPlanLabels.TrackingPlanVersion,
		}
	}

	// Update status details
	if _, exists := c.store.statusDetailsMap[key]; !exists {
		c.store.statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
	}

	// Create status details for the whole event
	statusKey := fmt.Sprintf("%s:%d:%s:%s:",
		event.StatusLabels.Status,
		event.StatusLabels.StatusCode,
		event.EventLabels.EventName,
		event.EventLabels.EventType)

	if _, exists := c.store.statusDetailsMap[key][statusKey]; !exists {
		// Only capture sample event and response for the first occurrence
		payloadBytes, err := jsonrs.Marshal(event.Event)
		if err != nil {
			return err
		}

		c.store.statusDetailsMap[key][statusKey] = &reportingtypes.StatusDetail{
			Status:         event.StatusLabels.Status,
			StatusCode:     event.StatusLabels.StatusCode,
			SampleResponse: event.StatusLabels.ErrorType,
			SampleEvent:    payloadBytes,
			EventName:      event.EventLabels.EventName,
			EventType:      event.EventLabels.EventType,
			Count:          0,
		}
	}
	c.store.statusDetailsMap[key][statusKey].Count++

	c.store.puDetailsMap[key] = &reportingtypes.PUDetails{
		PU: event.Stage,
	}

	return nil
}

func generateMetricKey(event *reportingtypes.MetricEvent) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%d:%s:%d:%s:%s",
		event.ConnectionLabels.SourceLabels.SourceID,
		event.ConnectionLabels.DestinationLabels.DestinationID,
		event.ConnectionLabels.SourceLabels.SourceJobRunID,
		event.ConnectionLabels.TransformationLabels.TransformationID,
		event.ConnectionLabels.TransformationLabels.TransformationVersionID,
		event.ConnectionLabels.TrackingPlanLabels.TrackingPlanID,
		event.ConnectionLabels.TrackingPlanLabels.TrackingPlanVersion,
		event.Stage,
		event.StatusLabels.StatusCode,
		event.EventLabels.EventName,
		event.EventLabels.EventType)
}
