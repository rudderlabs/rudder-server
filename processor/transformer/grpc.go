package transformer

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"google.golang.org/protobuf/types/known/structpb"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	transformerpb "github.com/rudderlabs/rudder-server/proto/transformer"
)

func (trans *handle) grpcRequest(ctx context.Context, data []TransformerEvent) []TransformerResponse {
	protoRes, err := trans.grpcClient.Transform(ctx, &transformerpb.TransformRequest{
		Events: lo.Map(data, func(event TransformerEvent, _ int) *transformerpb.TransformerEvent {
			return event.AsProto()
		}),
	})
	if err != nil {
		panic(err)
	}
	return protoToTransformerResponse(protoRes)
}

func (te *TransformerEvent) AsProto() *transformerpb.TransformerEvent {
	return &transformerpb.TransformerEvent{
		Message:     toProtoStruct(te.Message),
		Metadata:    metadataToProto(&te.Metadata),
		Destination: destinationToProto(&te.Destination),
		Libraries: lo.Map(te.Libraries, func(lib backendconfig.LibraryT, _ int) *transformerpb.Library {
			return &transformerpb.Library{
				VersionId: lib.VersionID,
			}
		}),
	}
}

func metadataToProto(metadata *Metadata) *transformerpb.Metadata {
	return &transformerpb.Metadata{
		SourceId:            metadata.SourceID,
		SourceName:          metadata.SourceName,
		WorkspaceId:         metadata.WorkspaceID,
		Namespace:           metadata.Namespace,
		InstanceId:          metadata.InstanceID,
		SourceType:          metadata.SourceType,
		SourceCategory:      metadata.SourceCategory,
		TrackingPlanId:      metadata.TrackingPlanId,
		TrackingPlanVersion: int32(metadata.TrackingPlanVersion),
		SourceTpConfig: lo.MapEntries(metadata.SourceTpConfig, func(key string, value map[string]interface{}) (string, *structpb.Struct) {
			return key, toProtoStruct(value)
		}),
		MergedTpConfig:          toProtoStruct(metadata.MergedTpConfig),
		DestinationId:           metadata.DestinationID,
		JobId:                   metadata.JobID,
		SourceJobId:             metadata.SourceJobID,
		SourceJobRunId:          metadata.SourceJobRunID,
		SourceTaskRunId:         metadata.SourceTaskRunID,
		RecordID:                toProtoValue(metadata.RecordID),
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

func destinationToProto(destination *backendconfig.DestinationT) *transformerpb.Destination {
	return &transformerpb.Destination{
		Id:   destination.ID,
		Name: destination.Name,
		DestinationDefinition: &transformerpb.DestinationDefinition{
			Id:            destination.DestinationDefinition.ID,
			Name:          destination.DestinationDefinition.Name,
			DisplayName:   destination.DestinationDefinition.DisplayName,
			Config:        toProtoStruct(destination.DestinationDefinition.Config),
			ResponseRules: toProtoStruct(destination.DestinationDefinition.ResponseRules),
		},
		Config:      toProtoStruct(destination.Config),
		Enabled:     destination.Enabled,
		WorkspaceId: destination.WorkspaceID,
		Transformation: lo.Map(destination.Transformations, func(item backendconfig.TransformationT, index int) *transformerpb.Transformation {
			return &transformerpb.Transformation{
				VersionId: item.VersionID,
				Id:        item.ID,
				Config:    toProtoStruct(item.Config),
			}
		}),
		IsProcessorEnabled: destination.IsProcessorEnabled,
		RevisionId:         destination.RevisionID,
	}
}

func protoToTransformerResponse(res *transformerpb.TransformResponse) []TransformerResponse {
	return lo.Map(res.Response, func(response *transformerpb.Response, _ int) TransformerResponse {
		return TransformerResponse{
			Output:     response.Output.AsMap(),
			Metadata:   convertProtoMetadataToStruct(response.Metadata),
			StatusCode: int(response.StatusCode),
			Error:      response.Error,
			ValidationErrors: lo.Map(response.ValidationError, func(protoValidationError *transformerpb.ValidationError, _ int) ValidationError {
				return convertProtoValidationErrorToStruct(protoValidationError)
			}),
		}
	})
}

func convertProtoMetadataToStruct(protoMetadata *transformerpb.Metadata) Metadata {
	return Metadata{
		SourceID:            protoMetadata.SourceId,
		SourceName:          protoMetadata.SourceName,
		WorkspaceID:         protoMetadata.WorkspaceId,
		Namespace:           protoMetadata.Namespace,
		InstanceID:          protoMetadata.InstanceId,
		SourceType:          protoMetadata.SourceType,
		SourceCategory:      protoMetadata.SourceCategory,
		TrackingPlanId:      protoMetadata.TrackingPlanId,
		TrackingPlanVersion: int(protoMetadata.TrackingPlanVersion),
		SourceTpConfig: lo.MapEntries(protoMetadata.SourceTpConfig, func(key string, value *structpb.Struct) (string, map[string]interface{}) {
			return key, value.AsMap()
		}),
		MergedTpConfig:          protoMetadata.MergedTpConfig.AsMap(),
		DestinationID:           protoMetadata.DestinationId,
		JobID:                   protoMetadata.JobId,
		SourceJobID:             protoMetadata.SourceJobId,
		SourceJobRunID:          protoMetadata.SourceJobRunId,
		SourceTaskRunID:         protoMetadata.SourceTaskRunId,
		RecordID:                protoMetadata.RecordID.GetStringValue(),
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

func toProtoStruct(data map[string]interface{}) *structpb.Struct {
	if protoMsg, err := structpb.NewStruct(data); err != nil {
		panic(fmt.Errorf("failed to convert %v to proto struct: %v", data, err))
	} else {
		return protoMsg
	}
}

func toProtoValue(data interface{}) *structpb.Value {
	if protoMsg, err := structpb.NewValue(data); err != nil {
		panic(fmt.Errorf("failed to convert %v to proto value: %v", data, err))
	} else {
		return protoMsg
	}
}
