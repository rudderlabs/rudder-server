package processor

import (
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

// Converts processor types.Metadata to reportingtypes.MetricEvent
func ConvertMetadataToMetricEvent(metadata *types.Metadata, stage string, statusLabels *reportingtypes.StatusLabels, event map[string]interface{}) *reportingtypes.MetricEvent {
	return &reportingtypes.MetricEvent{
		ConnectionLabels: reportingtypes.ConnectionLabels{
			SourceLabels: reportingtypes.SourceLabels{
				SourceID:           metadata.SourceID,
				SourceTaskRunID:    metadata.SourceTaskRunID,
				SourceJobID:        metadata.SourceJobID,
				SourceJobRunID:     metadata.SourceJobRunID,
				SourceDefinitionID: metadata.SourceDefinitionID,
				SourceCategory:     metadata.SourceCategory,
			},
			DestinationLabels: reportingtypes.DestinationLabels{
				DestinationID:           metadata.DestinationID,
				DestinationDefinitionID: metadata.DestinationDefinitionID,
			},
			TransformationLabels: reportingtypes.TransformationLabels{
				TransformationID:        metadata.TransformationID,
				TransformationVersionID: metadata.TransformationVersionID,
			},
			TrackingPlanLabels: reportingtypes.TrackingPlanLabels{
				TrackingPlanID:      metadata.TrackingPlanID,
				TrackingPlanVersion: metadata.TrackingPlanVersion,
			},
		},
		EventLabels: reportingtypes.EventLabels{
			EventType: metadata.EventType,
			EventName: metadata.EventName,
		},
		StatusLabels: *statusLabels,
		Stage:        stage,
		Event:        event,
	}
}
