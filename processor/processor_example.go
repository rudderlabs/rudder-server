package processor

import (
	"context"

	"github.com/rudderlabs/rudder-server/enterprise/reporting/collector"
	rt "github.com/rudderlabs/rudder-server/utils/types"
)

type ProcessorStage interface {
	// Process takes input events and returns output events
	Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error)
}

type BotBlockingStage struct{}

func (s *BotBlockingStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type EventBlockingStage struct{}

func (s *EventBlockingStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type DeduplicationStage struct{}

func (s *DeduplicationStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type UserSuppressionStage struct{}

func (s *UserSuppressionStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type DestinationFilterStage struct{}

func (s *DestinationFilterStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type TrackingPlanValidatorStage struct{}

func (s *TrackingPlanValidatorStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type UserTransformerStage struct{}

func (s *UserTransformerStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type EventFilterStage struct{}

func (s *EventFilterStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type DestinationTransformerStage struct{}

func (s *DestinationTransformerStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type RouterStage struct{}

func (s *RouterStage) Process(ctx context.Context, input []*rt.InMetricEvent) ([]*rt.OutMetricEvent, error) {
	return nil, nil
}

type SubJob struct {
	InputEvents      []*rt.InMetricEvent
	MetricsCollector *collector.MetricsCollectorMediator
}

func Example() {
	batch := []*rt.InMetricEvent{}
	subJobs := splitIntoSubJobs(batch, 100)
	ctx := context.Background()

	for _, subJob := range subJobs {
		enabledStages := []string{"bot_blocking", "event_blocking", "deduplication", "user_suppression", "destination_filter", "tracking_plan_validator", "user_transformer", "event_filter", "destination_transformer"}

		inMetricEvents := subJob.InputEvents
		metricsCollector := collector.NewMetricsCollectorMediator(rt.StageDetails{Stage: "deduplication"}, inMetricEvents, nil)

		for _, s := range enabledStages {
			stage := getStageInstance(s)
			metricsCollector.NextStage(rt.StageDetails{Stage: s})
			outMetricEvents, _ := stage.Process(ctx, inMetricEvents)
			metricsCollector.Collect(outMetricEvents...)
			inMetricEvents = getInputEventsFromOutputEvents(outMetricEvents)
		}

		metricsCollector.End()
	}

	mergedCollector := collector.NewMetricsCollectorMediator(rt.StageDetails{Stage: "merged"}, nil, nil)
	mergedCollector.End()
	for _, subJob := range subJobs {
		mergedCollector.Merge(subJob.MetricsCollector)
	}
	mergedCollector.Flush(ctx, nil)

}

func getStageInstance(stage string) ProcessorStage {
	if stage == "bot_blocking" {
		return &BotBlockingStage{}
	}

	if stage == "event_blocking" {
		return &EventBlockingStage{}
	}

	if stage == "deduplication" {
		return &DeduplicationStage{}
	}

	if stage == "user_suppression" {
		return &UserSuppressionStage{}
	}

	if stage == "destination_filter" {
		return &DestinationFilterStage{}
	}

	if stage == "tracking_plan_validator" {
		return &TrackingPlanValidatorStage{}
	}

	if stage == "user_transformer" {
		return &UserTransformerStage{}
	}

	if stage == "event_filter" {
		return &EventFilterStage{}
	}

	if stage == "destination_transformer" {
		return &DestinationTransformerStage{}
	}

	if stage == "router" {
		return &RouterStage{}
	}

	return nil
}

func getInputEventsFromOutputEvents(outputEvents []*rt.OutMetricEvent) []*rt.InMetricEvent {
	return nil
}

func splitIntoSubJobs(events []*rt.InMetricEvent, chunkSize int) []*SubJob {
	return nil
}
