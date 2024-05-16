package transformer

import (
	"context"

	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerpb "github.com/rudderlabs/rudder-server/proto/transformer"
)

// TODO: What should we do with these fields.
// SourceTpConfig      map[string]map[string]interface{} `json:"sourceTpConfig"`
// MergedTpConfig      map[string]interface{}
// RecordID            interface{}                       `json:"recordId"`
func (trans *handle) grpcRequest(ctx context.Context, data []TransformerEvent) []TransformerResponse {
	protoRes, err := trans.transformerSvc.Transform(ctx, &transformerpb.TransformRequest{
		Events: lo.Map(data, func(event TransformerEvent, _ int) *transformerpb.TransformerEvent {
			return convertTransformerEventToProto(event)
		}),
	})
	if err != nil {
		panic(err)
	}
	return convertProtoToTransformerResponse(protoRes)
}

func convertTransformerEventToProto(event TransformerEvent) *transformerpb.TransformerEvent {
	protoMsg, err := structpb.NewStruct(event.Message)
	if err != nil {
		panic(err)
	}

	return &transformerpb.TransformerEvent{
		Message:     protoMsg,
		Metadata:    convertMetadataToProto(&event.Metadata),
		Destination: convertDestinationToProto(&event.Destination),
		Libraries: lo.Map(event.Libraries, func(lib backendconfig.LibraryT, _ int) *transformerpb.Library {
			return &transformerpb.Library{
				VersionId: lib.VersionID,
			}
		}),
	}
}

func convertDestinationToProto(destination *backendconfig.DestinationT) *transformerpb.Destination {
	return &transformerpb.Destination{
		Id:                 destination.ID,
		Name:               destination.Name,
		Enabled:            destination.Enabled,
		WorkspaceId:        destination.WorkspaceID,
		IsProcessorEnabled: destination.IsProcessorEnabled,
		RevisionId:         destination.RevisionID,
	}
}

func convertMetadataToProto(metadata *Metadata) *transformerpb.Metadata {
	return &transformerpb.Metadata{
		SourceId:                metadata.SourceID,
		SourceName:              metadata.SourceName,
		WorkspaceId:             metadata.WorkspaceID,
		Namespace:               metadata.Namespace,
		InstanceId:              metadata.InstanceID,
		SourceType:              metadata.SourceType,
		SourceCategory:          metadata.SourceCategory,
		TrackingPlanId:          metadata.TrackingPlanId,
		TrackingPlanVersion:     int32(metadata.TrackingPlanVersion),
		DestinationId:           metadata.DestinationID,
		JobId:                   metadata.JobID,
		SourceJobId:             metadata.SourceJobID,
		SourceJobRunId:          metadata.SourceJobRunID,
		SourceTaskRunId:         metadata.SourceTaskRunID,
		DestinationType:         metadata.DestinationType,
		MessageId:               metadata.MessageID,
		OauthAccessToken:        metadata.OAuthAccessToken,
		TraceParent:             metadata.TraceParent,
		MessageIds:              metadata.MessageIDs,
		RudderId:                metadata.RudderID,
		ReceivedAt:              metadata.ReceivedAt,
		EventName:               metadata.EventName,
		EventType:               metadata.EventType,
		SourceDefinitionId:      metadata.SourceDefinitionID,
		DestinationDefinitionId: metadata.DestinationDefinitionID,
		TransformationId:        metadata.TransformationID,
		TransformationVersionId: metadata.TransformationVersionID,
	}
}

func convertProtoToTransformerResponse(res *transformerpb.TransformResponse) []TransformerResponse {
	transformerResponses := make([]TransformerResponse, len(res.Response))

	for i, protoRes := range res.Response {
		transformerResponses[i] = TransformerResponse{
			Output:     protoRes.Output.AsMap(),
			Metadata:   convertProtoMetadataToStruct(protoRes.Metadata),
			StatusCode: int(protoRes.StatusCode),
			Error:      protoRes.Error,
			ValidationErrors: lo.Map(protoRes.ValidationError, func(protoValidationError *transformerpb.ValidationError, _ int) ValidationError {
				return convertProtoValidationErrorToStruct(protoValidationError)
			}),
		}
	}

	return transformerResponses
}

// Helper function to convert proto Metadata to Struct
func convertProtoMetadataToStruct(protoMetadata *transformerpb.Metadata) Metadata {
	return Metadata{
		SourceID:                protoMetadata.SourceId,
		SourceName:              protoMetadata.SourceName,
		WorkspaceID:             protoMetadata.WorkspaceId,
		Namespace:               protoMetadata.Namespace,
		InstanceID:              protoMetadata.InstanceId,
		SourceType:              protoMetadata.SourceType,
		SourceCategory:          protoMetadata.SourceCategory,
		TrackingPlanId:          protoMetadata.TrackingPlanId,
		TrackingPlanVersion:     int(protoMetadata.TrackingPlanVersion),
		DestinationID:           protoMetadata.DestinationId,
		JobID:                   protoMetadata.JobId,
		SourceJobID:             protoMetadata.SourceJobId,
		SourceJobRunID:          protoMetadata.SourceJobRunId,
		SourceTaskRunID:         protoMetadata.SourceTaskRunId,
		DestinationType:         protoMetadata.DestinationType,
		MessageID:               protoMetadata.MessageId,
		OAuthAccessToken:        protoMetadata.OauthAccessToken,
		TraceParent:             protoMetadata.TraceParent,
		MessageIDs:              protoMetadata.MessageIds,
		RudderID:                protoMetadata.RudderId,
		ReceivedAt:              protoMetadata.ReceivedAt,
		EventName:               protoMetadata.EventName,
		EventType:               protoMetadata.EventType,
		SourceDefinitionID:      protoMetadata.SourceDefinitionId,
		DestinationDefinitionID: protoMetadata.DestinationDefinitionId,
		TransformationID:        protoMetadata.TransformationId,
		TransformationVersionID: protoMetadata.TransformationVersionId,
	}
}

// Helper function to convert proto ValidationError to Struct
func convertProtoValidationErrorToStruct(protoValidationError *transformerpb.ValidationError) ValidationError {
	if protoValidationError == nil {
		return ValidationError{}
	}
	return ValidationError{
		Type:    protoValidationError.Type,
		Message: protoValidationError.Message,
		Meta:    protoValidationError.Meta,
	}
}
