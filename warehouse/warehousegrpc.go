package warehouse

import (
	"context"
	"encoding/json"
	"fmt"

	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/configuration_testing"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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

func (w *warehousegrpc) TriggerWHUploads(context context.Context, request *proto.WHUploadsRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadsReq := UploadsReqT{
		WorkspaceID:   request.WorkspaceId,
		SourceID:      request.SourceId,
		DestinationID: request.DestinationId,
		API:           UploadAPI,
	}
	res, err := uploadsReq.TriggerWhUploads()
	return res, err
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

func (w *warehousegrpc) TriggerWHUpload(context context.Context, request *proto.WHUploadRequest) (*proto.TriggerWhUploadsResponse, error) {
	uploadReq := UploadReqT{
		UploadId:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
		API:         UploadAPI,
	}
	res, err := uploadReq.TriggerWHUpload()
	return res, err
}

func (w *warehousegrpc) Validate(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	handleT := configuration_testing.CTHandleT{}
	return handleT.Validating(req)
}

func (w *warehousegrpc) RetryWHUploads(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	retryReq := &RetryRequest{
		WorkspaceID:     req.WorkspaceId,
		SourceID:        req.SourceId,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		IntervalInHours: req.IntervalInHours,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		API:             UploadAPI,
	}
	r, err := retryReq.RetryWHUploads()
	response = &proto.RetryWHUploadsResponse{
		Message:    r.Message,
		StatusCode: r.StatusCode,
	}
	return
}

type ObjectStorageValidationRequest struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

func (w *warehousegrpc) ValidateObjectStorageDestination(context context.Context, request *proto.ValidateObjectStorageRequest) (response *proto.ValidateObjectStorageResponse, err error) {
	byt, err := json.Marshal(request)
	if err != nil {
		return &proto.ValidateObjectStorageResponse{
			IsValid: false,
			Status:  500,
			Error:   fmt.Errorf("unable to validate the request. unable to marshal the request proto message with error: %w", err).Error(),
		}, nil
	}

	r := ObjectStorageValidationRequest{}

	if err := json.Unmarshal(byt, &r); err != nil {
		return &proto.ValidateObjectStorageResponse{
			IsValid: false,
			Status:  500,
			Error:   fmt.Errorf("unable to validate the request. unable to extract data into validation request with error: %w", err).Error(),
		}, nil
	}

	valid, statusCode, err := validateObjectStorage(&r)
	if err != nil {
		errMessage := fmt.Errorf("unable to finish request to validate storage: %w", err)
		pkgLogger.Errorf(errMessage.Error())
		return &proto.ValidateObjectStorageResponse{
			IsValid: false,
			Status:  statusCode,
			Error:   errMessage.Error(),
		}, nil
	}

	if !valid {
		return &proto.ValidateObjectStorageResponse{
			IsValid: valid,
			Status:  200,
			Error:   "authentication with credentials provided failed",
		}, nil
	}

	return &proto.ValidateObjectStorageResponse{
		IsValid: true,
		Status:  200,
		Error:   "",
	}, nil
}
