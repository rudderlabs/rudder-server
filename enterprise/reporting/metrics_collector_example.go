package reporting

import (
	"context"
	"database/sql"
)

// NewMetricsCollector initializes a new MetricsCollector for the given stage and input event metrics.
func NewMetricsCollector(stage string, input []*InEventMetric) MetricsCollector {
	return nil // Replace with actual implementation
}

type StageOption func()

// WithAllInputEventsSuccessful is an option for the MetricsCollector that indicates it should mark all input events as successful.
func WithAllInputEventsSuccessful() StageOption {
	return func() {
		// Implementation for handling all successful events
	}
}

// WithCaptureDiffMetrics is an option for the MetricsCollector that indicates it should compare input and output event metrics and capture the differences in separate metrics.
func WithCaptureInOutDiffMetrics() StageOption {
	return func() {
		// Implementation for capturing differences in metrics
	}
}

type MetricsCollector interface {

	// CurrentStage returns the current stage of the metrics collection.
	CurrentStage() string

	// Collect output event metrics for the current stage. An error will be returned if the stage is not active.
	Collect(events ...*OutEventMetric) error

	// Enter a new stage in the metrics collection process. The previous stage will be marked as complete.
	// Successful events of the previous stage will be treated as input events for the new stage.
	NextStage(stage string, options ...StageOption) error

	// End the current stage in the metrics collection process and mark the metrics collector as complete (readonly).
	End(terminal bool) error

	// Merge this MetricsCollector with another one and return a new MetricsCollector.
	// Both MetricsCollectors must be in the same stage and must have ended.
	Merge(other MetricsCollector) (MetricsCollector, error)

	// Persist the metrics to the database. This will finalize the metrics collection and persist the data.
	// The collector must be marked as complete (readonly) before committing, otherwise an error will be returned.
	Persist(ctx context.Context, tx *sql.Tx) error
}

type InEventMetric struct {
	ConnectionLabels ConnectionLabels `json:"connectionLabels"`
	EventLabels      EventLabels      `json:"eventLabels"`
	Event            Event            `json:"event"`
}

type OutEventMetric struct {
	ConnectionLabels ConnectionLabels `json:"connectionLabels"`
	EventLabels      EventLabels      `json:"eventLabels"`
	Event            Event            `json:"event"`
	StatusLabels     StatusLabels     `json:"statusLabels"`
}

type MetricsCollectorMediator struct {
	collectors []MetricsCollector
}

type StatusLabels struct {
	StatusCode int    `json:"statusCode"`
	Status     string `json:"status"`
	ErrorType  string `json:"errorCode"`
}
type ConnectionLabels struct {
	SourceLabels         SourceLabels         `json:"sourceLabels"`
	DestinationLabels    DestinationLabels    `json:"destinationLabels"`
	TransformationLabels TransformationLabels `json:"transformationLabels"`
	TrackingPlanLabels   TrackingPlanLabels   `json:"trackingPlanLabels"`
}

type DestinationLabels struct {
	DestinationID           string `json:"destinationId"`
	DestinationDefinitionID string `json:"destinationDefinitionId"`
}

type TransformationLabels struct {
	TransformationID        string `json:"transformationId"`
	TransformationVersionID string `json:"transformationVersionId"`
}

type TrackingPlanLabels struct {
	TrackingPlanID      string `json:"trackingPlanId"`
	TrackingPlanVersion int    `json:"trackingPlanVersion"`
}

type SourceLabels struct {
	SourceCategory     string `json:"sourceCategory"`
	SourceDefinitionID string `json:"sourceDefinitionId"`
	SourceID           string `json:"sourceId"`
	SourceJobID        string `json:"sourceJobId"`
	SourceJobRunID     string `json:"sourceJobRunId"`
	SourceTaskRunID    string `json:"sourceTaskRunId"`
}

type EventLabels struct {
	EventType string `json:"eventType"`
	EventName string `json:"eventName"`
}

type Event map[string]interface{}

func Example() {

	// processor
	{
		// gateway stage
		var gwEventMetrics []*InEventMetric
		c := NewMetricsCollector("gateway", gwEventMetrics)

		// CLARIFICATION:
		// Could we have c.Collect(gwEventMetrics...) here? and in df stage below we don't have WithAllInputEventsSuccessful ?

		// destination_filter stage
		_ = c.NextStage("destination_filter", WithAllInputEventsSuccessful())
		var dfEventMetrics []*OutEventMetric
		c.Collect(dfEventMetrics...)

		// tracking_plan_validator stage
		_ = c.NextStage("tracking_plan_validator", WithCaptureInOutDiffMetrics())

		var tpvEventMetrics []*OutEventMetric
		c.Collect(tpvEventMetrics...)

		// user_transformer stage
		_ = c.NextStage("user_transformer", WithCaptureInOutDiffMetrics())

		var utEventMetrics []*OutEventMetric
		c.Collect(utEventMetrics...)

		// event_filter stage
		_ = c.NextStage("event_filter", WithCaptureInOutDiffMetrics())

		var efEventMetrics []*OutEventMetric
		c.Collect(efEventMetrics...)

		// dest_transformer stage
		_ = c.NextStage("dest_transformer", WithCaptureInOutDiffMetrics())
		var dtEventMetrics []*OutEventMetric
		c.Collect(dtEventMetrics...)

		c.End(false)

		var o MetricsCollector
		merged, _ := c.Merge(o)
		_ = merged.Persist(context.Background(), nil)

	}

	// router
	{
		var rtEventMetrics []*InEventMetric
		c := NewMetricsCollector("router", rtEventMetrics)
		var rEventMetrics []*OutEventMetric
		c.Collect(rEventMetrics...)
		c.End(true)
		c.Persist(context.Background(), nil)
	}

	// batch router
	{
		var brtEventMetrics []*InEventMetric
		c := NewMetricsCollector("batch_router", brtEventMetrics)
		var rEventMetrics []*OutEventMetric
		c.Collect(rEventMetrics...)
		c.End(true)
		c.Persist(context.Background(), nil)
	}
}
