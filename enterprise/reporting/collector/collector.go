package collector

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-server/enterprise/reporting"
	. "github.com/rudderlabs/rudder-server/utils/tx"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// StageNode represents a stage in the metrics collection pipeline
type StageNode struct {
	Stage    string
	Initial  bool
	Terminal bool
	Next     *StageNode
}

// DefaultMetricsCollector collects metrics for billing and UI features
type DefaultMetricsCollector struct {
	currentStage         *StageNode
	completed            bool
	statusDetailsMap     map[string]map[string]*reportingtypes.StatusDetail
	connectionDetailsMap map[string]*reportingtypes.ConnectionDetails
	puDetailsMap         map[string]*reportingtypes.PUDetails
	reporter             *reporting.Mediator
	conf                 *config.Config
}

// NewDefaultMetricsCollector creates a new default metrics collector
func NewDefaultMetricsCollector(stageDetails reportingtypes.StageDetails, inputEvents []*reportingtypes.InMetricEvent, conf *config.Config) *DefaultMetricsCollector {
	// Create the initial stage node
	initialStage := &StageNode{
		Stage:    stageDetails.Stage,
		Initial:  stageDetails.Initial,
		Terminal: stageDetails.Terminal,
		Next:     nil,
	}

	collector := &DefaultMetricsCollector{
		currentStage:         initialStage,
		statusDetailsMap:     make(map[string]map[string]*reportingtypes.StatusDetail),
		connectionDetailsMap: make(map[string]*reportingtypes.ConnectionDetails),
		puDetailsMap:         make(map[string]*reportingtypes.PUDetails),
		conf:                 conf,
	}
	// Optionally process/store inputEvents here if needed
	return collector
}

// CurrentStage returns the current stage
func (c *DefaultMetricsCollector) CurrentStage() string {
	if c.currentStage == nil {
		return ""
	}
	return c.currentStage.Stage
}

// Collect processes output metric events for the current stage
func (c *DefaultMetricsCollector) Collect(events ...*reportingtypes.OutMetricEvent) error {
	if c.completed {
		return fmt.Errorf("collector is completed and cannot accept new events")
	}

	for _, event := range events {
		if err := c.collect(event); err != nil {
			return err
		}
	}
	return nil
}

// NextStage advances to the next stage by creating a new stage node and linking it
func (c *DefaultMetricsCollector) NextStage(stageDetails reportingtypes.StageDetails) error {
	if c.completed {
		return fmt.Errorf("collector is completed and cannot advance to next stage")
	}

	// Create new stage node
	newStage := &StageNode{
		Stage:    stageDetails.Stage,
		Initial:  stageDetails.Initial,
		Terminal: stageDetails.Terminal,
		Next:     nil,
	}

	// Link the current stage to the new stage
	if c.currentStage != nil {
		c.currentStage.Next = newStage
	}

	// Update current stage
	c.currentStage = newStage
	return nil
}

// End marks the collector as complete and sets the current stage as terminal
func (c *DefaultMetricsCollector) End() error {
	c.completed = true
	return nil
}

// Merge merges this collector with another one
func (c *DefaultMetricsCollector) Merge(other MetricsCollector) (MetricsCollector, error) {
	otherDefault, ok := other.(*DefaultMetricsCollector)
	if !ok {
		return nil, fmt.Errorf("cannot merge with non-default collector")
	}

	if !c.completed || !otherDefault.completed {
		return nil, fmt.Errorf("cannot merge with incomplete collectors")
	}

	// Create merged collector with the current stage
	merged := NewDefaultMetricsCollector(reportingtypes.StageDetails{Stage: c.currentStage.Stage, Initial: false, Terminal: c.currentStage.Terminal}, nil, c.conf)
	merged.completed = c.completed

	// Copy the stage linked list from the current collector
	merged.currentStage = c.copyStageList(c.currentStage)

	// Merge connection details
	for key, details := range c.connectionDetailsMap {
		merged.connectionDetailsMap[key] = details
	}
	for key, details := range otherDefault.connectionDetailsMap {
		merged.connectionDetailsMap[key] = details
	}

	// Merge PU details
	for key, details := range c.puDetailsMap {
		merged.puDetailsMap[key] = details
	}
	for key, details := range otherDefault.puDetailsMap {
		merged.puDetailsMap[key] = details
	}

	// Merge status details
	for key, statusMap := range c.statusDetailsMap {
		if merged.statusDetailsMap[key] == nil {
			merged.statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
		}
		for statusKey, status := range statusMap {
			if existing, exists := merged.statusDetailsMap[key][statusKey]; exists {
				existing.Count += status.Count
			} else {
				merged.statusDetailsMap[key][statusKey] = status
			}
		}
	}
	for key, statusMap := range otherDefault.statusDetailsMap {
		if merged.statusDetailsMap[key] == nil {
			merged.statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
		}
		for statusKey, status := range statusMap {
			if existing, exists := merged.statusDetailsMap[key][statusKey]; exists {
				existing.Count += status.Count
			} else {
				merged.statusDetailsMap[key][statusKey] = status
			}
		}
	}

	return merged, nil
}

// copyStageList creates a deep copy of the stage linked list
func (c *DefaultMetricsCollector) copyStageList(head *StageNode) *StageNode {
	if head == nil {
		return nil
	}

	copied := &StageNode{
		Stage:    head.Stage,
		Initial:  head.Initial,
		Terminal: head.Terminal,
		Next:     c.copyStageList(head.Next),
	}

	return copied
}

// GetStageHistory returns all stages in the linked list as a slice
func (c *DefaultMetricsCollector) GetStageHistory() []string {
	var stages []string
	current := c.currentStage

	// Find the head of the linked list (initial stage)
	for current != nil && !current.Initial {
		current = current.Next
	}

	// Traverse from initial to current stage
	for current != nil {
		stages = append(stages, current.Stage)
		if current == c.currentStage {
			break
		}
		current = current.Next
	}

	return stages
}

// IsTerminalStage checks if the current stage is marked as terminal
func (c *DefaultMetricsCollector) IsTerminalStage() bool {
	return c.currentStage != nil && c.currentStage.Terminal
}

// GetCurrentStageNode returns the current stage node
func (c *DefaultMetricsCollector) GetCurrentStageNode() *StageNode {
	return c.currentStage
}

// Flush writes the collected metrics to the database
func (c *DefaultMetricsCollector) Flush(ctx context.Context, tx *Tx) error {
	if !c.completed {
		return fmt.Errorf("collector must be completed before flushing")
	}

	factory := &reporting.Factory{
		EnterpriseToken: "reporting-token",
	}
	reporter := factory.Setup(context.Background(), c.conf, nil)
	reporter.Report(ctx, c.getMetrics(), tx)

	return nil
}

// getMetrics returns the collected metrics
func (c *DefaultMetricsCollector) getMetrics() []*reportingtypes.PUReportedMetric {
	metrics := make([]*reportingtypes.PUReportedMetric, 0)
	for key, connectionDetails := range c.connectionDetailsMap {
		for _, statusDetail := range c.statusDetailsMap[key] {
			metric := &reportingtypes.PUReportedMetric{
				ConnectionDetails: *connectionDetails,
				PUDetails:         *c.puDetailsMap[key],
				StatusDetail:      statusDetail,
			}
			metrics = append(metrics, metric)
		}
	}
	return metrics
}

// collect processes a single output metric event
func (c *DefaultMetricsCollector) collect(event *reportingtypes.OutMetricEvent) error {
	// Generate key for this combination
	key := c.generateMetricKey(event)

	// Update connection details
	if _, exists := c.connectionDetailsMap[key]; !exists {
		c.connectionDetailsMap[key] = &reportingtypes.ConnectionDetails{
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
	if _, exists := c.statusDetailsMap[key]; !exists {
		c.statusDetailsMap[key] = make(map[string]*reportingtypes.StatusDetail)
	}

	// Create status details for the whole event
	statusKey := fmt.Sprintf("%s:%d:%s:%s:",
		event.StatusLabels.Status,
		event.StatusLabels.StatusCode,
		event.EventLabels.EventName,
		event.EventLabels.EventType)

	if _, exists := c.statusDetailsMap[key][statusKey]; !exists {
		// Only capture sample event and response for the first occurrence
		payloadBytes, err := jsonrs.Marshal(event.Event)
		if err != nil {
			return err
		}

		c.statusDetailsMap[key][statusKey] = &reportingtypes.StatusDetail{
			Status:         event.StatusLabels.Status,
			StatusCode:     event.StatusLabels.StatusCode,
			SampleResponse: event.StatusLabels.ErrorType,
			SampleEvent:    payloadBytes,
			EventName:      event.EventLabels.EventName,
			EventType:      event.EventLabels.EventType,
			Count:          0,
		}
	}
	c.statusDetailsMap[key][statusKey].Count++

	c.puDetailsMap[key] = &reportingtypes.PUDetails{
		PU:         c.currentStage.Stage,
		InitialPU:  c.currentStage.Initial,
		TerminalPU: c.currentStage.Terminal,
		// NextPU:     c.currentStage.Next.Stage,
		// InPU:       c.currentStage.Next.Stage,
	}

	return nil
}

// generateMetricKey creates a unique key for metric aggregation
func (c *DefaultMetricsCollector) generateMetricKey(event *reportingtypes.OutMetricEvent) string {
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%d:%s:%s:%d:%s:%s:%s",
		event.ConnectionLabels.SourceLabels.SourceID,
		event.ConnectionLabels.DestinationLabels.DestinationID,
		event.ConnectionLabels.SourceLabels.SourceJobRunID,
		event.ConnectionLabels.TransformationLabels.TransformationID,
		event.ConnectionLabels.TransformationLabels.TransformationVersionID,
		event.ConnectionLabels.TrackingPlanLabels.TrackingPlanID,
		event.ConnectionLabels.TrackingPlanLabels.TrackingPlanVersion,
		event.EventLabels.EventType,
		event.EventLabels.EventName,
		event.StatusLabels.StatusCode,
		c.currentStage.Stage,
		c.currentStage.Initial,
		c.currentStage.Terminal,
	)
}
