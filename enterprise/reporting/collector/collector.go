package collector

import (
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	proctypes "github.com/rudderlabs/rudder-server/processor/types"
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

func (c *DefaultMetricsCollector) CollectMultiple(responses []proctypes.TransformerResponse, stage string) error {
	c.store.Lock()
	defer c.store.Unlock()

	for _, response := range responses {
		if err := c.collectMetricsForResponse(response, stage); err != nil {
			return err
		}
	}
	return nil
}

func (c *DefaultMetricsCollector) Collect(response proctypes.TransformerResponse, stage string) error {
	c.store.Lock()
	defer c.store.Unlock()
	return c.collectMetricsForResponse(response, stage)
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

func (c *DefaultMetricsCollector) Reset() {
	c.store.Lock()
	defer c.store.Unlock()
	c.store = NewMetricsStore()
}

func (c *DefaultMetricsCollector) collectMetricsForResponse(response proctypes.TransformerResponse, stage string) error {
	// Generate key for this combination
	key := generateMetricKey(response, stage)

	// Update connection details
	if _, exists := c.store.connectionDetailsMap[key]; !exists {
		c.store.connectionDetailsMap[key] = &reportingtypes.ConnectionDetails{
			SourceID:                response.Metadata.SourceID,
			SourceTaskRunID:         response.Metadata.SourceTaskRunID,
			SourceJobID:             response.Metadata.SourceJobID,
			SourceJobRunID:          response.Metadata.SourceJobRunID,
			SourceDefinitionID:      response.Metadata.SourceDefinitionID,
			SourceCategory:          response.Metadata.SourceCategory,
			DestinationID:           response.Metadata.DestinationID,
			DestinationDefinitionID: response.Metadata.DestinationDefinitionID,
			TransformationID:        response.Metadata.TransformationID,
			TransformationVersionID: response.Metadata.TransformationVersionID,
			TrackingPlanID:          response.Metadata.TrackingPlanID,
			TrackingPlanVersion:     response.Metadata.TrackingPlanVersion,
		}
	}

	// Update status details
	if _, exists := c.store.statusDetailsMap[key]; !exists {
		c.store.statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
	}

	// Determine status based on statusCode
	status := "succeeded"
	if response.StatusCode >= 400 && response.StatusCode < 500 {
		status = "aborted"
	} else if response.StatusCode >= 500 {
		status = "failed"
	}

	// Create status details for the whole event
	statusKey := fmt.Sprintf("%s:%d:%s:%s:",
		status,
		response.StatusCode,
		response.Metadata.EventName,
		response.Metadata.EventType)

	if _, exists := c.store.statusDetailsMap[key][statusKey]; !exists {
		// Only capture sample event and response for the first occurrence

		payloadBytes, err := jsonrs.Marshal(response.Output)
		if err != nil {
			return err
		}

		c.store.statusDetailsMap[key][statusKey] = &reportingtypes.StatusDetail{
			Status:         status,
			StatusCode:     response.StatusCode,
			SampleResponse: response.Error,
			SampleEvent:    payloadBytes,
			EventName:      response.Metadata.EventName,
			EventType:      response.Metadata.EventType,
			StatTags:       response.StatTags,
			Count:          0,
			ViolationCount: int64(len(response.ValidationErrors)),
		}
	}
	c.store.statusDetailsMap[key][statusKey].Count++

	// TODO: We need not store PU details for each key, We can have nested map
	c.store.puDetailsMap[key] = &reportingtypes.PUDetails{
		PU: stage,
	}

	return nil
}

func generateMetricKey(response proctypes.TransformerResponse, stage string) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%d:%s:%d:%s:%s",
		response.Metadata.SourceID,
		response.Metadata.DestinationID,
		response.Metadata.SourceJobRunID,
		response.Metadata.TransformationID,
		response.Metadata.TransformationVersionID,
		response.Metadata.TrackingPlanID,
		response.Metadata.TrackingPlanVersion,
		stage,
		response.StatusCode,
		response.Metadata.EventName,
		response.Metadata.EventType)
}
