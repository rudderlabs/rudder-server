package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"net/http"
)

const (
	TriggeredSuccessfully   = "Triggered successfully"
	NoPendingEvents         = "No pending events to sync for this destination"
	DownloadFileNamePattern = "downloadfile.*.tmp"
	NoSuchSync              = "No such sync exist"
)

type GRPCServer struct {
	proto.UnimplementedWarehouseServer
	GRPC
}

func (g *GRPCServer) GetWHUploads(ctx context.Context, request *proto.WHUploadsRequest) (*proto.WHUploadsResponse, error) {
	g.logger.Infow("[GetWHUploads]", lf.WorkspaceID, request.WorkspaceId, lf.SourceID, request.SourceId, lf.DestinationID, request.DestinationId)

	limit, offset := request.Limit, request.Offset
	if limit < 1 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.WHUploadsResponse{
			Uploads: []*proto.WHUploadResponse{},
			Pagination: &proto.Pagination{
				Limit:  limit,
				Offset: offset,
			},
		}, fmt.Errorf("no sources found for workspace %s", request.WorkspaceId)
	}

	var syncUploadInfos []model.SyncUploadInfo
	var totalUploads int64
	var err error

	if g.isMultiWorkspace {
		syncUploadInfos, totalUploads, err = g.uploadRepo.SyncsInfoForMultiTenant(ctx, int(limit), int(offset), repo.SyncUploadOptions{
			SourceIDs:       sourceIDs,
			DestinationID:   request.DestinationId,
			DestinationType: request.DestinationType,
			Status:          request.Status,
		})
	} else {
		syncUploadInfos, totalUploads, err = g.uploadRepo.SyncsInfoForNonMultiTenant(ctx, int(limit), int(offset), repo.SyncUploadOptions{
			SourceIDs:       sourceIDs,
			DestinationID:   request.DestinationId,
			DestinationType: request.DestinationType,
			Status:          request.Status,
		})
	}
	if err != nil {
		return &proto.WHUploadsResponse{
			Uploads: []*proto.WHUploadResponse{},
			Pagination: &proto.Pagination{
				Limit:  limit,
				Offset: offset,
			},
		}, fmt.Errorf("getting syncs info %s", err.Error())
	}

	var uploads []*proto.WHUploadResponse
	for _, syncUploadInfo := range syncUploadInfos {
		uploads = append(uploads, &proto.WHUploadResponse{
			Id:               syncUploadInfo.ID,
			SourceId:         syncUploadInfo.SourceID,
			DestinationId:    syncUploadInfo.DestinationID,
			DestinationType:  syncUploadInfo.DestinationType,
			Namespace:        syncUploadInfo.Namespace,
			Error:            syncUploadInfo.Error,
			Attempt:          syncUploadInfo.Attempt,
			Status:           syncUploadInfo.Status,
			CreatedAt:        timestamppb.New(syncUploadInfo.CreatedAt),
			FirstEventAt:     timestamppb.New(syncUploadInfo.FirstEventAt),
			LastEventAt:      timestamppb.New(syncUploadInfo.LastEventAt),
			LastExecAt:       timestamppb.New(syncUploadInfo.LastExecAt),
			NextRetryTime:    timestamppb.New(syncUploadInfo.NextRetryTime),
			Duration:         syncUploadInfo.Duration,
			Tables:           []*proto.WHTable{},
			IsArchivedUpload: syncUploadInfo.IsArchivedUpload,
		})
	}

	return &proto.WHUploadsResponse{
		Uploads: []*proto.WHUploadResponse{},
		Pagination: &proto.Pagination{
			Limit:  limit,
			Offset: offset,
			Total:  int32(totalUploads),
		},
	}, nil
}

func (g *GRPCServer) GetWHUpload(ctx context.Context, request *proto.WHUploadRequest) (*proto.WHUploadResponse, error) {
	g.logger.Infow("[GetWHUpload]", lf.WorkspaceID, request.WorkspaceId, lf.UploadJobID, request.UploadId)

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.WHUploadResponse{}, fmt.Errorf("no sources found for workspace %s", request.WorkspaceId)
	}

	syncUploadInfos, _, err := g.uploadRepo.SyncsInfoForMultiTenant(ctx, 1, 0, repo.SyncUploadOptions{
		UploadID: request.UploadId,
	})
	if err != nil {
		return &proto.WHUploadResponse{}, fmt.Errorf("getting syncs info %s", err.Error())
	}
	if len(syncUploadInfos) == 0 {
		return &proto.WHUploadResponse{}, fmt.Errorf("no sync found for upload id %d", request.UploadId)
	}

	syncUploadInfo := syncUploadInfos[0]

	if !slices.Contains(sourceIDs, syncUploadInfo.SourceID) {
		return &proto.WHUploadResponse{}, fmt.Errorf("unauthorized access to upload id %d", request.UploadId)
	}

	syncTableInfos, err := g.tableUploadsRepo.SyncsInfo(ctx, syncUploadInfo.ID)
	if err != nil {
		return &proto.WHUploadResponse{}, fmt.Errorf("getting tables %s", err.Error())
	}

	var tables []*proto.WHTable
	for _, syncTableInfo := range syncTableInfos {
		tables = append(tables, &proto.WHTable{
			Id:         syncTableInfo.ID,
			UploadId:   syncTableInfo.UploadID,
			Name:       syncTableInfo.Name,
			Status:     syncTableInfo.Status,
			Error:      syncTableInfo.Error,
			LastExecAt: timestamppb.New(syncUploadInfo.LastExecAt),
			Count:      int32(syncTableInfo.Count),
			Duration:   int32(syncTableInfo.Duration),
		})
	}

	return &proto.WHUploadResponse{
		Id:               syncUploadInfo.ID,
		SourceId:         syncUploadInfo.SourceID,
		DestinationId:    syncUploadInfo.DestinationID,
		DestinationType:  syncUploadInfo.DestinationType,
		Namespace:        syncUploadInfo.Namespace,
		Error:            syncUploadInfo.Error,
		Attempt:          syncUploadInfo.Attempt,
		Status:           syncUploadInfo.Status,
		CreatedAt:        timestamppb.New(syncUploadInfo.CreatedAt),
		FirstEventAt:     timestamppb.New(syncUploadInfo.FirstEventAt),
		LastEventAt:      timestamppb.New(syncUploadInfo.LastEventAt),
		LastExecAt:       timestamppb.New(syncUploadInfo.LastExecAt),
		NextRetryTime:    timestamppb.New(syncUploadInfo.NextRetryTime),
		Duration:         syncUploadInfo.Duration,
		Tables:           tables,
		IsArchivedUpload: syncUploadInfo.IsArchivedUpload,
	}, nil
}

func (g *GRPCServer) TriggerWHUploads(ctx context.Context, request *proto.WHUploadsRequest) (*proto.TriggerWhUploadsResponse, error) {
	g.logger.Infow("[TriggerWHUploads]", lf.WorkspaceID, request.WorkspaceId, lf.SourceID, request.SourceId, lf.DestinationID, request.DestinationId)

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("no sources found for workspace %s", request.WorkspaceId)
	}

	if request.DestinationId == "" {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("destination id is required")
	}

	filters := []repo.FilterBy{
		{Key: "destination_id", Value: request.DestinationId},
		{Key: "status", Value: model.ExportedData, NotEquals: true},
		{Key: "status", Value: model.Aborted, NotEquals: true},
	}

	var pendingUploadCount int64
	var pendingStagingFilesCount int64
	var err error

	pendingUploadCount, err = g.uploadRepo.Count(ctx, filters...)
	if err != nil {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("trigger uploads: pending uploads count: %w", err)
	}

	if pendingUploadCount == 0 {
		pendingStagingFilesCount, err = g.stagingRepo.CountPendingForDestination(ctx, request.DestinationId)
		if err != nil {
			return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("trigger uploads: pending staging files count: %w", err)
		}
	}
	if pendingUploadCount+pendingStagingFilesCount == 0 {
		return &proto.TriggerWhUploadsResponse{
			StatusCode: http.StatusOK,
			Message:    NoPendingEvents,
		}, nil
	}

	var wh []model.Warehouse
	if request.SourceId != "" && request.DestinationId == "" {
		wh = g.bcManager.WarehousesBySourceID(request.WorkspaceId)
	} else if request.DestinationId != "" {
		wh = g.bcManager.WarehousesByDestID(request.DestinationId)
	}
	if len(wh) == 0 {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("no warehouse found for sourceID: %s, destinationID: %s", request.SourceId, request.DestinationId)
	}

	for _, warehouse := range wh {
		triggerUpload(warehouse)
	}
	return &proto.TriggerWhUploadsResponse{
		StatusCode: http.StatusOK,
		Message:    TriggeredSuccessfully,
	}, nil
}

func (g *GRPCServer) TriggerWHUpload(ctx context.Context, request *proto.WHUploadRequest) (*proto.TriggerWhUploadsResponse, error) {
	g.logger.Infow("[TriggerWHUpload]", lf.WorkspaceID, request.WorkspaceId, lf.UploadJobID, request.UploadId)

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("no sources found for workspace %s", request.WorkspaceId)
	}

	upload, err := g.uploadRepo.Get(ctx, request.UploadId)
	if err == model.ErrUploadNotFound {
		return &proto.TriggerWhUploadsResponse{
			Message:    NoSuchSync,
			StatusCode: http.StatusOK,
		}, nil
	}
	if err != nil {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("trigger upload: get upload: %w", err)
	}
	if !slices.Contains(sourceIDs, upload.SourceID) {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("unauthorized access to upload id %d", request.UploadId)
	}

	err = g.uploadRepo.TriggerUpload(ctx, request.UploadId)
	if err != nil {
		return &proto.TriggerWhUploadsResponse{}, fmt.Errorf("trigger upload: %w", err)
	}

	return &proto.TriggerWhUploadsResponse{
		StatusCode: http.StatusOK,
		Message:    TriggeredSuccessfully,
	}, nil
}

func (g *GRPCServer) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return wrapperspb.Bool(true), nil
}

func (g *GRPCServer) RetryWHUploads(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	g.logger.Infow("[RetryWHUploads]", lf.WorkspaceID, req.WorkspaceId, lf.SourceID, req.SourceId, lf.DestinationID, req.DestinationId, lf.DestinationType, req.DestinationType, "intervalInHours", req.IntervalInHours)

	if req.SourceId == "" && req.DestinationId == "" && req.WorkspaceId == "" {
		return &proto.RetryWHUploadsResponse{}, errors.New("please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId")
	}

	if len(req.UploadIds) == 0 && req.IntervalInHours <= 0 {
		return &proto.RetryWHUploadsResponse{}, errors.New("please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.RetryWHUploadsResponse{}, fmt.Errorf("no sources found for workspace %s", req.WorkspaceId)
	}
	if req.SourceId != "" && !slices.Contains(sourceIDs, req.SourceId) {
		return &proto.RetryWHUploadsResponse{}, errors.New("no such sourceID exists")
	}

	retryCount, err := g.uploadRepo.Retry(ctx, repo.TriggerOptions{
		WorkspaceID:     req.WorkspaceId,
		SourceIDs:       sourceIDs,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		IntervalInHours: req.IntervalInHours,
	})
	if err != nil {
		return &proto.RetryWHUploadsResponse{}, fmt.Errorf("retry uploads: %w", err)
	}

	return &proto.RetryWHUploadsResponse{
		StatusCode: http.StatusOK,
		Count:      retryCount,
	}, nil
}

func (g *GRPCServer) CountWHUploadsToRetry(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	g.logger.Infow("[CountWHUploadsToRetry]", lf.WorkspaceID, req.WorkspaceId, lf.SourceID, req.SourceId, lf.DestinationID, req.DestinationId, lf.DestinationType, req.DestinationType, "intervalInHours", req.IntervalInHours)

	if req.SourceId == "" && req.DestinationId == "" && req.WorkspaceId == "" {
		return &proto.RetryWHUploadsResponse{}, errors.New("please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId")
	}

	if len(req.UploadIds) == 0 && req.IntervalInHours <= 0 {
		return &proto.RetryWHUploadsResponse{}, errors.New("please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.RetryWHUploadsResponse{}, fmt.Errorf("no sources found for workspace %s", req.WorkspaceId)
	}
	if req.SourceId != "" && !slices.Contains(sourceIDs, req.SourceId) {
		return &proto.RetryWHUploadsResponse{}, errors.New("no such sourceID exists")
	}

	retryCount, err := g.uploadRepo.RetryCount(ctx, repo.TriggerOptions{
		WorkspaceID:     req.WorkspaceId,
		SourceIDs:       sourceIDs,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		IntervalInHours: req.IntervalInHours,
	})
	if err != nil {
		return &proto.RetryWHUploadsResponse{}, fmt.Errorf("retry uploads: %w", err)
	}

	return &proto.RetryWHUploadsResponse{
		StatusCode: http.StatusOK,
		Count:      retryCount,
	}, nil
}

func (g *GRPCServer) ValidateObjectStorageDestination(ctx context.Context, request *proto.ValidateObjectStorageRequest) (response *proto.ValidateObjectStorageResponse, err error) {
	g.logger.Infow("[ValidateObjectStorageDestination]", "ObjectStorageType", request.Type)

	byt, err := json.Marshal(request)
	if err != nil {
		return &proto.ValidateObjectStorageResponse{}, fmt.Errorf("validate object storage: %w", err)
	}

	var validationRequest validateObjectStorageRequest
	if err := json.Unmarshal(byt, &validationRequest); err != nil {
		return &proto.ValidateObjectStorageResponse{}, fmt.Errorf("unable to extract data into validation request with error: \n%s", err)
	}

	err = g.validateObjectStorage(ctx, validationRequest)
	if err != nil {
		if errors.Is(err, &InvalidDestinationCredErr{}) {
			return &proto.ValidateObjectStorageResponse{
				IsValid: false,
				Error:   err.Error(),
			}, nil
		}
		return &proto.ValidateObjectStorageResponse{}, fmt.Errorf("unable to handle validate storage request call: %s", err)
	}

	return &proto.ValidateObjectStorageResponse{
		IsValid: true,
		Error:   "",
	}, nil
}

func (g *GRPCServer) Validate(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	g.logger.Infow("[Validate]", "Role", req.Role, "Path", req.Path, "Step", req.Step)

	res, err := g.validate(ctx, &validationRequest{
		Path: req.Path,
		Step: req.Step,
		Body: req.Body,
	})
	if err != nil {
		return &proto.WHValidationResponse{}, fmt.Errorf("validate: %w", err)
	}
	return &proto.WHValidationResponse{
		Error: res.Error,
		Data:  res.Data,
	}, nil
}
