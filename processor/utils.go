package processor

import (
	"github.com/rudderlabs/rudder-server/processor/types"
	reportingtypes "github.com/rudderlabs/rudder-server/utils/types"
)

func ConvertMetadataToInMetricEvent(metadata *types.Metadata, statusLabels *reportingtypes.StatusLabels, event map[string]interface{}) *reportingtypes.InMetricEvent {
	return &reportingtypes.InMetricEvent{
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
		Event: reportingtypes.Event{
			ID: metadata.MessageID,
		},
	}
}

// Converts processor types.Metadata to reportingtypes.OutMetricEvent
func ConvertMetadataToOutMetricEvent(metadata *types.Metadata, statusLabels *reportingtypes.StatusLabels, event map[string]interface{}) *reportingtypes.OutMetricEvent {
	inMetricEvent := ConvertMetadataToInMetricEvent(metadata, statusLabels, event)
	return &reportingtypes.OutMetricEvent{
		ConnectionLabels: inMetricEvent.ConnectionLabels,
		EventLabels:      inMetricEvent.EventLabels,
		StatusLabels:     *statusLabels,
		Event:            inMetricEvent.Event,
	}
}
