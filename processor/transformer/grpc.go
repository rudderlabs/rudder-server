package transformer

import (
	"context"

	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerpb "github.com/rudderlabs/rudder-server/proto/transformer"
)

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
	return lo.Map(res.Response, func(protoRes *transformerpb.Response, _ int) TransformerResponse {
		return TransformerResponse{} // TODO: implement me
	})
}

func ConvertValidationErrorToProto(validationError *ValidationError) *transformerpb.ValidationError {
	return &transformerpb.ValidationError{
		Type:    validationError.Type,
		Message: validationError.Message,
		Meta:    validationError.Meta,
	}
}
