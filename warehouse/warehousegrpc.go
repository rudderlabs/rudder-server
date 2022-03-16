package warehouse

import (
	"context"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
)

type WarehouseGrpc struct {
	proto.UnimplementedWarehouseServer
}

func (w *WarehouseGrpc) GetWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.WHUploadsResponse, error) {
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

func (w *WarehouseGrpc) TriggerWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:   request.WorkspaceId,
		SourceID:      request.SourceId,
		DestinationID: request.DestinationId,
		API:           UploadAPI,
	}
	res, err := uploadsReq.TriggerWhUploads()
	return res, err
}

func (w *WarehouseGrpc) GetWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.WHUploadResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.GetWHUpload()
	return res, err
}

func (w *WarehouseGrpc) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return wrapperspb.Bool(UploadAPI.enabled), nil
}

func (w *WarehouseGrpc) TriggerWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.TriggerWHUpload()
	return res, err
}
