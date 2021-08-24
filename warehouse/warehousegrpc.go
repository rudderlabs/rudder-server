package warehouse

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
)

type warehousegrpc struct {
	proto.UnimplementedWarehouseServer
}

func (w *warehousegrpc) GetWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.WHUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:     request.WorkspaceId,
		SourceID:        request.SourceId,
		DestinationID:   request.DestinationId,
		DestinationType: request.DestinationType,
		Status:          request.Status,
		Limit:           request.Limit,
		Offset:          request.Offset,
		API:             UploadAPI,
	}
	res, err := uploadsReq.GetWhUploads()
	return res, err
}

func (w *warehousegrpc) TriggerWHUploads(context context.Context, request *proto.WHUploadsRequest) (*emptypb.Empty, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:     request.WorkspaceId,
		SourceID:        request.SourceId,
		DestinationID:   request.DestinationId,
		API:             UploadAPI,
	}
	err := uploadsReq.TriggerWhUploads()
	return new(emptypb.Empty), err
}

func (w *warehousegrpc) GetWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.WHUploadResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.GetWHUpload()
	return res, err
}

func (w *warehousegrpc) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return wrapperspb.Bool(UploadAPI.enabled), nil
}

func (w *warehousegrpc) TriggerWHUpload(context context.Context, request *proto.WHUploadRequest) (*emptypb.Empty, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	err := uploadReq.TriggerWHUpload()
	return new(emptypb.Empty), err
}
