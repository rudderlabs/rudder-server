package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/bcm"

	"github.com/samber/lo"

	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	sqlmw "github.com/rudderlabs/rudder-server/warehouse/integrations/middleware/sqlquerywrapper"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	lf "github.com/rudderlabs/rudder-server/warehouse/logfield"
	"github.com/rudderlabs/rudder-server/warehouse/multitenant"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

const (
	triggeredSuccessfully         = "Triggered successfully"
	noPendingEvents               = "No pending events to sync for this destination"
	downloadFileNamePattern       = "downloadfile.*.tmp"
	noSuchSync                    = "No such sync exist"
	syncFrequencyThresholdMinutes = 30
)

type GRPC struct {
	proto.UnimplementedWarehouseServer

	conf               *config.Config
	logger             logger.Logger
	isMultiWorkspace   bool
	cpClient           cpclient.InternalControlPlane
	connectionManager  *controlplane.ConnectionManager
	tenantManager      *multitenant.Manager
	bcManager          *bcm.BackendConfigManager
	tableUploadsRepo   *repo.TableUploads
	stagingRepo        *repo.StagingFiles
	uploadRepo         *repo.Uploads
	triggerStore       *sync.Map
	fileManagerFactory filemanager.Factory
	now                func() time.Time

	config struct {
		region         string
		cpRouterUseTLS bool
		instanceID     string
		controlPlane   struct {
			url      string
			userName string
			password string
		}
		enableTunnelling              bool
		defaultLatencyAggregationType model.LatencyAggregationType
		maxLatencyQueryLookbackDays   int
	}
}

func NewGRPCServer(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	db *sqlmw.DB,
	tenantManager *multitenant.Manager,
	bcManager *bcm.BackendConfigManager,
	triggerStore *sync.Map,
) (*GRPC, error) {
	g := &GRPC{
		conf:               conf,
		logger:             logger.Child("grpc"),
		tenantManager:      tenantManager,
		bcManager:          bcManager,
		stagingRepo:        repo.NewStagingFiles(db),
		uploadRepo:         repo.NewUploads(db),
		tableUploadsRepo:   repo.NewTableUploads(db),
		triggerStore:       triggerStore,
		fileManagerFactory: filemanager.New,
		now:                timeutil.Now,
	}

	g.config.region = conf.GetString("region", "")
	g.config.cpRouterUseTLS = conf.GetBool("CP_ROUTER_USE_TLS", true)
	g.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	g.config.controlPlane.url = conf.GetString("CONFIG_BACKEND_URL", "api.rudderlabs.com")
	g.config.controlPlane.userName = conf.GetString("CP_INTERNAL_API_USERNAME", "")
	g.config.controlPlane.password = conf.GetString("CP_INTERNAL_API_PASSWORD", "")
	g.config.enableTunnelling = conf.GetBool("ENABLE_TUNNELLING", true)
	g.config.maxLatencyQueryLookbackDays = conf.GetInt("Warehouse.grpc.maxLatencyQueryLookbackDays", 90)

	g.cpClient = cpclient.NewInternalClientWithCache(
		g.config.controlPlane.url,
		cpclient.BasicAuth{
			Username: g.config.controlPlane.userName,
			Password: g.config.controlPlane.password,
		},
	)

	connectionToken, tokenType, isMultiWorkspace, err := deployment.GetConnectionToken()
	if err != nil {
		return nil, fmt.Errorf("connection token: %w", err)
	}

	g.config.defaultLatencyAggregationType, err = model.GetLatencyAggregationType(conf.GetString("Warehouse.grpc.defaultLatencyAggregationType", "p90"))
	if err != nil {
		return nil, fmt.Errorf("default latency aggregation type: %w", err)
	}

	labels := map[string]string{}
	if g.config.region != "" {
		labels["region"] = g.config.region
	}

	g.isMultiWorkspace = isMultiWorkspace
	g.connectionManager = &controlplane.ConnectionManager{
		AuthInfo: controlplane.AuthInfo{
			Service:         "warehouse",
			ConnectionToken: connectionToken,
			InstanceID:      g.config.instanceID,
			TokenType:       tokenType,
			Labels:          labels,
		},
		RetryInterval: 0,
		UseTLS:        g.config.cpRouterUseTLS,
		Logger:        g.logger,
		Options: []grpc.ServerOption{
			grpc.UnaryInterceptor(statsInterceptor(statsFactory)),
		},
		RegisterService: func(srv *grpc.Server) {
			proto.RegisterWarehouseServer(srv, g)
		},
	}
	return g, nil
}

func (g *GRPC) Start(ctx context.Context) {
	configCh := g.tenantManager.WatchConfig(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-configCh:
			if !ok {
				return
			}
			g.processData(data)
		}
	}
}

func (g *GRPC) processData(configData map[string]backendconfig.ConfigT) {
	// Only 1 connection flag is enough, since they are all the same in multi-workspace environments
	for _, wConfig := range configData {
		connectionFlags := wConfig.ConnectionFlags
		if val, ok := connectionFlags.Services["warehouse"]; ok {
			g.connectionManager.Apply(connectionFlags.URL, val)
			break
		}
	}
}

func (*GRPC) GetHealth(context.Context, *emptypb.Empty) (*wrapperspb.BoolValue, error) {
	return wrapperspb.Bool(true), nil
}

func (g *GRPC) GetWHUploads(ctx context.Context, request *proto.WHUploadsRequest) (*proto.WHUploadsResponse, error) {
	g.logger.Infow(
		"Getting warehouse uploads",
		lf.WorkspaceID, request.WorkspaceId,
		lf.SourceID, request.SourceId,
		lf.DestinationID, request.DestinationId,
	)

	limit, offset := request.Limit, request.Offset
	if limit < 1 {
		limit = 10
	}
	if offset < 0 {
		offset = 0
	}

	var sourceIDs []string
	if request.SourceId != "" {
		sourceIDs = []string{request.SourceId}
	} else {
		sourceIDs = g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	}
	if len(sourceIDs) == 0 {
		return &proto.WHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", request.WorkspaceId)
	}

	syncOptions := model.SyncUploadOptions{
		SourceIDs:       sourceIDs,
		DestinationID:   request.DestinationId,
		DestinationType: request.DestinationType,
		Status:          request.Status,
		WorkspaceID:     request.WorkspaceId,
	}

	var uploadInfos []model.UploadInfo
	var totalUploads int64
	var err error

	if g.isMultiWorkspace {
		uploadInfos, totalUploads, err = g.uploadRepo.SyncsInfoForMultiTenant(
			ctx,
			int(limit),
			int(offset),
			syncOptions,
		)
	} else {
		uploadInfos, totalUploads, err = g.uploadRepo.SyncsInfoForNonMultiTenant(
			ctx,
			int(limit),
			int(offset),
			syncOptions,
		)
	}
	if err != nil {
		return &proto.WHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get syncs info: %v", err)
	}

	uploads := lo.Map(uploadInfos, func(item model.UploadInfo, index int) *proto.WHUploadResponse {
		ur := &proto.WHUploadResponse{
			Id:               item.ID,
			SourceId:         item.SourceID,
			DestinationId:    item.DestinationID,
			DestinationType:  item.DestinationType,
			Namespace:        item.Namespace,
			Error:            item.Error,
			Attempt:          int32(item.Attempt),
			Status:           item.Status,
			CreatedAt:        timestamppb.New(item.CreatedAt),
			FirstEventAt:     timestamppb.New(item.FirstEventAt),
			LastEventAt:      timestamppb.New(item.LastEventAt),
			Duration:         int32(item.Duration),
			Tables:           []*proto.WHTable{},
			IsArchivedUpload: item.IsArchivedUpload,
		}
		if !item.NextRetryTime.IsZero() {
			ur.NextRetryTime = timestamppb.New(item.NextRetryTime)
		}
		if !item.LastExecAt.IsZero() {
			ur.LastExecAt = timestamppb.New(item.LastExecAt)
		}
		return ur
	})

	response := &proto.WHUploadsResponse{Uploads: uploads, Pagination: &proto.Pagination{
		Limit:  limit,
		Offset: offset,
		Total:  int32(totalUploads),
	}}
	return response, nil
}

func (g *GRPC) GetWHUpload(ctx context.Context, request *proto.WHUploadRequest) (*proto.WHUploadResponse, error) {
	g.logger.Infow("Getting warehouse upload",
		lf.WorkspaceID, request.WorkspaceId,
		lf.UploadJobID, request.UploadId,
	)

	if request.UploadId < 1 {
		return &proto.WHUploadResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "upload_id should be greater than 0")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.WHUploadResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", request.WorkspaceId)
	}

	uploadInfos, _, err := g.uploadRepo.SyncsInfoForMultiTenant(ctx, 1, 0, model.SyncUploadOptions{
		UploadID:    request.UploadId,
		WorkspaceID: request.WorkspaceId,
	})
	if err != nil {
		return &proto.WHUploadResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get syncs info for id %d: %v", request.UploadId, err)
	}
	if len(uploadInfos) == 0 {
		return &proto.WHUploadResponse{},
			status.Errorf(codes.Code(code.Code_NOT_FOUND), "no sync found for id %d", request.UploadId)
	}

	syncUploadInfo := uploadInfos[0]

	if !slices.Contains(sourceIDs, syncUploadInfo.SourceID) {
		return &proto.WHUploadResponse{},
			status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	syncTableInfos, err := g.tableUploadsRepo.SyncsInfo(ctx, syncUploadInfo.ID)
	if err != nil {
		return &proto.WHUploadResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get table infos: %s", err.Error())
	}

	tables := lo.Map(syncTableInfos, func(item model.TableUploadInfo, index int) *proto.WHTable {
		wht := &proto.WHTable{
			Id:       item.ID,
			UploadId: item.UploadID,
			Name:     item.Name,
			Status:   item.Status,
			Error:    item.Error,
			Count:    int32(item.Count),
			Duration: int32(item.Duration),
		}
		if !item.LastExecAt.IsZero() {
			wht.LastExecAt = timestamppb.New(item.LastExecAt)
		}
		return wht
	})

	ur := &proto.WHUploadResponse{
		Id:               syncUploadInfo.ID,
		SourceId:         syncUploadInfo.SourceID,
		DestinationId:    syncUploadInfo.DestinationID,
		DestinationType:  syncUploadInfo.DestinationType,
		Namespace:        syncUploadInfo.Namespace,
		Error:            syncUploadInfo.Error,
		Attempt:          int32(syncUploadInfo.Attempt),
		Status:           syncUploadInfo.Status,
		CreatedAt:        timestamppb.New(syncUploadInfo.CreatedAt),
		FirstEventAt:     timestamppb.New(syncUploadInfo.FirstEventAt),
		LastEventAt:      timestamppb.New(syncUploadInfo.LastEventAt),
		Duration:         int32(syncUploadInfo.Duration),
		Tables:           tables,
		IsArchivedUpload: syncUploadInfo.IsArchivedUpload,
	}
	if !syncUploadInfo.NextRetryTime.IsZero() {
		ur.NextRetryTime = timestamppb.New(syncUploadInfo.NextRetryTime)
	}
	if !syncUploadInfo.LastExecAt.IsZero() {
		ur.LastExecAt = timestamppb.New(syncUploadInfo.LastExecAt)
	}
	return ur, nil
}

func (g *GRPC) TriggerWHUploads(ctx context.Context, request *proto.WHUploadsRequest) (*proto.TriggerWhUploadsResponse, error) {
	g.logger.Infow("Triggering warehouse uploads",
		lf.WorkspaceID, request.WorkspaceId,
		lf.SourceID, request.SourceId,
		lf.DestinationID, request.DestinationId,
	)

	if request.DestinationId == "" {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "destination id is required")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", request.WorkspaceId)
	}

	var (
		pendingUploadCount       int64
		pendingStagingFilesCount int64
		err                      error
	)

	filters := []repo.FilterBy{
		{Key: "destination_id", Value: request.DestinationId},
		{Key: "status", Value: model.ExportedData, NotEquals: true},
		{Key: "status", Value: model.Aborted, NotEquals: true},
	}
	pendingUploadCount, err = g.uploadRepo.Count(ctx, filters...)
	if err != nil {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get pending uploads count: %v", err)
	}

	if pendingUploadCount == 0 {
		pendingStagingFilesCount, err = g.stagingRepo.CountPendingForDestination(ctx, request.DestinationId)
		if err != nil {
			return &proto.TriggerWhUploadsResponse{},
				status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get pending staging files count: %v", err)
		}
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	if pendingUploadCount+pendingStagingFilesCount == 0 {
		return &proto.TriggerWhUploadsResponse{
			StatusCode: http.StatusOK,
			Message:    noPendingEvents,
		}, nil
	}

	var wh []model.Warehouse
	if request.SourceId != "" {
		wh = g.bcManager.WarehousesBySourceID(request.SourceId)
	} else {
		wh = g.bcManager.WarehousesByDestID(request.DestinationId)
	}
	if len(wh) == 0 {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no warehouse found for sourceID: %s, destinationID: %s", request.SourceId, request.DestinationId)
	}

	for _, warehouse := range wh {
		g.triggerStore.Store(warehouse.Identifier, struct{}{})
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	return &proto.TriggerWhUploadsResponse{
		StatusCode: http.StatusOK,
		Message:    triggeredSuccessfully,
	}, nil
}

func (g *GRPC) TriggerWHUpload(ctx context.Context, request *proto.WHUploadRequest) (*proto.TriggerWhUploadsResponse, error) {
	g.logger.Infow("Triggering warehouse upload",
		lf.WorkspaceID, request.WorkspaceId,
		lf.UploadJobID, request.UploadId,
	)

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[request.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", request.WorkspaceId)
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	upload, err := g.uploadRepo.Get(ctx, request.UploadId)
	if errors.Is(err, model.ErrUploadNotFound) {
		return &proto.TriggerWhUploadsResponse{
			Message:    noSuchSync,
			StatusCode: http.StatusOK,
		}, nil
	}
	if err != nil {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get sync id %d: %v", request.UploadId, err)
	}

	if !slices.Contains(sourceIDs, upload.SourceID) {
		return &proto.TriggerWhUploadsResponse{},
			status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	err = g.uploadRepo.TriggerUpload(ctx, request.UploadId)
	if errors.Is(err, model.ErrUploadNotFound) {
		return &proto.TriggerWhUploadsResponse{
			Message:    noSuchSync,
			StatusCode: http.StatusOK,
		}, nil
	}
	if err != nil {
		return &proto.TriggerWhUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to trigger sync for id %d: %v", request.UploadId, err)
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	return &proto.TriggerWhUploadsResponse{
		StatusCode: http.StatusOK,
		Message:    triggeredSuccessfully,
	}, nil
}

func (g *GRPC) RetryWHUploads(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	g.logger.Infow("Retrying warehouse syncs",
		lf.WorkspaceID, req.WorkspaceId,
		lf.SourceID, req.SourceId,
		lf.DestinationID, req.DestinationId,
		lf.DestinationType, req.DestinationType,
		lf.IntervalInHours, req.IntervalInHours,
	)

	if req.SourceId == "" && req.DestinationId == "" && req.WorkspaceId == "" {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId")
	}

	if len(req.UploadIds) == 0 && req.IntervalInHours <= 0 {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", req.WorkspaceId)
	}
	if req.SourceId != "" && !slices.Contains(sourceIDs, req.SourceId) {
		return &proto.RetryWHUploadsResponse{},
			status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	// Retry request should trigger on these cases.
	// 1. Either provide the retry interval.
	// 2. Or provide the List of Upload id's that needs to be re-triggered.
	retryCount, err := g.uploadRepo.Retry(ctx, model.RetryOptions{
		WorkspaceID:     req.WorkspaceId,
		SourceIDs:       sourceIDs,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		IntervalInHours: req.IntervalInHours,
	})
	if err != nil {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to retry: %v", err)
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	// TODO: Also, get rid of the message in here as well.
	return &proto.RetryWHUploadsResponse{
		StatusCode: http.StatusOK,
		Count:      retryCount,
	}, nil
}

func (g *GRPC) CountWHUploadsToRetry(ctx context.Context, req *proto.RetryWHUploadsRequest) (response *proto.RetryWHUploadsResponse, err error) {
	g.logger.Infow("Count syncs to retry",
		lf.WorkspaceID, req.WorkspaceId,
		lf.SourceID, req.SourceId,
		lf.DestinationID, req.DestinationId,
		lf.DestinationType, req.DestinationType,
		lf.IntervalInHours, req.IntervalInHours,
	)

	if req.SourceId == "" && req.DestinationId == "" && req.WorkspaceId == "" {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "please provide valid request parameters while retrying jobs with workspaceId or sourceId or destinationId")
	}

	if len(req.UploadIds) == 0 && req.IntervalInHours <= 0 {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "please provide valid request parameters while retrying jobs with UploadIds or IntervalInHours")
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.WorkspaceId]
	if len(sourceIDs) == 0 {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", req.WorkspaceId)
	}
	if req.SourceId != "" && !slices.Contains(sourceIDs, req.SourceId) {
		return &proto.RetryWHUploadsResponse{},
			status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	retryCount, err := g.uploadRepo.RetryCount(ctx, model.RetryOptions{
		WorkspaceID:     req.WorkspaceId,
		SourceIDs:       sourceIDs,
		DestinationID:   req.DestinationId,
		DestinationType: req.DestinationType,
		ForceRetry:      req.ForceRetry,
		UploadIds:       req.UploadIds,
		IntervalInHours: req.IntervalInHours,
	})
	if err != nil {
		return &proto.RetryWHUploadsResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get counts to retry: %v", err)
	}

	// TODO: Remove http status code and use grpc status code. Since it requires compatibility on the cp router side, leaving it as it is for now.
	// TODO: Also, get rid of the message in here as well.
	return &proto.RetryWHUploadsResponse{
		StatusCode: http.StatusOK,
		Count:      retryCount,
	}, nil
}

func (g *GRPC) Validate(ctx context.Context, req *proto.WHValidationRequest) (*proto.WHValidationResponse, error) {
	g.logger.Infow("Validating destination", "Role", req.Role, "Path", req.Path, "Step", req.Step)

	var (
		err      error
		reqModel struct {
			Destination backendconfig.DestinationT `json:"destination"`
		}
	)

	err = json.Unmarshal(json.RawMessage(req.Body), &reqModel)
	if err != nil {
		return &proto.WHValidationResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "invalid JSON in request body")
	}

	if len(reqModel.Destination.Config) == 0 {
		return &proto.WHValidationResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "destination config is empty")
	}

	// adding ssh tunnelling info, given we have
	// useSSH enabled from upstream
	if g.config.enableTunnelling {
		err = g.manageTunnellingSecrets(ctx, reqModel.Destination.Config)
		if err != nil {
			return &proto.WHValidationResponse{},
				status.Errorf(codes.Code(code.Code_INTERNAL), "unable to fetch ssh keys: %v", err)
		}
	}

	res, err := validations.Validate(ctx, &model.ValidationRequest{
		Path:        req.Path,
		Step:        req.Step,
		Destination: &reqModel.Destination,
	})
	if err != nil {
		return &proto.WHValidationResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to validate: %v", err)
	}
	if res.Error != "" {
		return &proto.WHValidationResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "unable to validate: %v", res.Error)
	}

	// TODO: We can get rid of the Error field in the response. Since it requires compatibility on the cp router side, leaving it as it is for now.
	return &proto.WHValidationResponse{
		Data: res.Data,
	}, nil
}

func (g *GRPC) manageTunnellingSecrets(ctx context.Context, config map[string]interface{}) error {
	if !warehouseutils.ReadAsBool("useSSH", config) {
		return nil
	}

	sshKeyId, ok := config["sshKeyId"]
	if !ok {
		return fmt.Errorf("missing sshKeyId in validation payload")
	}

	keys, err := g.cpClient.GetSSHKeys(ctx, sshKeyId.(string))
	if err != nil {
		return fmt.Errorf("fetching destination ssh keys: %w", err)
	}

	config["sshPrivateKey"] = keys.PrivateKey

	return nil
}

type validateObjectStorageRequest struct {
	Type   string                 `json:"type"`
	Config map[string]interface{} `json:"config"`
}

type invalidDestinationCredErr struct {
	Base      error
	Operation string
}

func (err invalidDestinationCredErr) Error() string {
	return fmt.Sprintf("Invalid destination creds, failed for operation: %s with err: \n%s", err.Operation, err.Base.Error())
}

func (g *GRPC) ValidateObjectStorageDestination(ctx context.Context, request *proto.ValidateObjectStorageRequest) (response *proto.ValidateObjectStorageResponse, err error) {
	g.logger.Infow("validating object storage", "ObjectStorageType", request.Type)

	byt, err := json.Marshal(request)
	if err != nil {
		return &proto.ValidateObjectStorageResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "unable to marshal the request proto message with error: \n%s", err.Error())
	}

	var validateRequest validateObjectStorageRequest
	if err := json.Unmarshal(byt, &validateRequest); err != nil {
		return &proto.ValidateObjectStorageResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "unable to extract data into validation request with error: \n%s", err)
	}

	switch request.Type {
	case warehouseutils.AzureBlob:
		if !checkMapForValidKey(validateRequest.Config, "containerName") {
			err = errors.New("containerName invalid or not present")
		}
	case warehouseutils.GCS, warehouseutils.MINIO, warehouseutils.S3, warehouseutils.DigitalOceanSpaces:
		if !checkMapForValidKey(validateRequest.Config, "bucketName") {
			err = errors.New("bucketName invalid or not present")
		}
	default:
		err = fmt.Errorf("type: %v not supported", request.Type)
	}
	if err != nil {
		return &proto.ValidateObjectStorageResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "invalid argument err: \n%s", err.Error())
	}

	err = g.validateObjectStorage(ctx, validateRequest)
	if err != nil {
		if errors.As(err, &invalidDestinationCredErr{}) {
			return &proto.ValidateObjectStorageResponse{
				Error: err.Error(),
			}, nil
		}

		return &proto.ValidateObjectStorageResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to handle validate storage request call: %s", err)
	}

	return &proto.ValidateObjectStorageResponse{
		IsValid: true,
	}, nil
}

// checkMapForValidKey checks the presence of key in map
// and if yes verifies that the key is string and non-empty.
func checkMapForValidKey(configMap map[string]interface{}, key string) bool {
	if value, ok := configMap[key]; !ok {
		return false
	} else if valStr, ok := value.(string); ok {
		return valStr != ""
	}
	return false
}

func (g *GRPC) validateObjectStorage(ctx context.Context, request validateObjectStorageRequest) error {
	settings := &filemanager.Settings{
		Provider: request.Type,
		Config:   request.Config,
	}

	overrideWithEnv(ctx, settings)

	fileManager, err := g.fileManagerFactory(settings)
	if err != nil {
		return fmt.Errorf("unable to create file manager: \n%s", err.Error())
	}

	it := filemanager.IterateFilesWithPrefix(ctx, fileManager.Prefix(), "", 1, fileManager)
	_ = it.Next()
	if err = it.Err(); err != nil {
		return invalidDestinationCredErr{Base: err, Operation: "list"}
	}

	tempFilePath, err := validations.CreateTempLoadFile(&backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{
			Name: request.Type,
		},
	})
	if err != nil {
		return fmt.Errorf("unable to create temp load file: \n%w", err)
	}
	defer func() {
		_ = os.Remove(tempFilePath)
	}()

	f, err := os.Open(tempFilePath)
	if err != nil {
		return fmt.Errorf("unable to open path to temporary file: \n%w", err)
	}

	uploadOutput, err := fileManager.Upload(ctx, f)
	if err != nil {
		return invalidDestinationCredErr{Base: err, Operation: "upload"}
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("unable to close file: \n%w", err)
	}

	tmpDirectory, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("unable to create temp directory: \n%w", err)
	}

	f, err = os.CreateTemp(tmpDirectory, downloadFileNamePattern)
	if err != nil {
		return fmt.Errorf("unable to create temp file: \n%w", err)
	}
	defer func() {
		_ = os.Remove(f.Name())
	}()

	err = fileManager.Download(
		ctx,
		f,
		fileManager.GetDownloadKeyFromFileLocation(uploadOutput.Location),
	)
	if err != nil {
		return invalidDestinationCredErr{Base: err, Operation: "download"}
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("unable to close file: \n%w", err)
	}
	return nil
}

// overrideWithEnv overrides the config keys in the fileManager settings
// with fallback values pulled from env. Only supported for S3 for now.
func overrideWithEnv(ctx context.Context, settings *filemanager.Settings) {
	envConfig := filemanager.GetProviderConfigFromEnv(filemanagerutil.ProviderConfigOpts(
		ctx,
		settings.Provider,
		config.Default,
	))
	if settings.Provider == warehouseutils.S3 {
		ifNotExistThenSet("prefix", envConfig["prefix"], settings.Config)
		ifNotExistThenSet("accessKeyID", envConfig["accessKeyID"], settings.Config)
		ifNotExistThenSet("accessKey", envConfig["accessKey"], settings.Config)
		ifNotExistThenSet("enableSSE", envConfig["enableSSE"], settings.Config)
		ifNotExistThenSet("iamRoleARN", envConfig["iamRoleArn"], settings.Config)
		ifNotExistThenSet("externalID", envConfig["externalID"], settings.Config)
		ifNotExistThenSet("regionHint", envConfig["regionHint"], settings.Config)
	}
}

func ifNotExistThenSet(keyToReplace string, replaceWith interface{}, configMap map[string]interface{}) {
	if _, ok := configMap[keyToReplace]; !ok {
		// In case we don't have the key, simply replace it with replaceWith
		configMap[keyToReplace] = replaceWith
	}
}

func (g *GRPC) RetrieveFailedBatches(
	ctx context.Context,
	req *proto.RetrieveFailedBatchesRequest,
) (*proto.RetrieveFailedBatchesResponse, error) {
	log := g.logger.With(
		lf.WorkspaceID, req.GetWorkspaceID(),
		lf.DestinationID, req.GetDestinationID(),
		lf.StartTime, req.GetStart(),
		lf.EndTime, req.GetEnd(),
	)
	log.Infow("Retrieving failed batches")

	if req.GetWorkspaceID() == "" || req.GetDestinationID() == "" {
		return &proto.RetrieveFailedBatchesResponse{},
			status.Error(codes.Code(code.Code_INVALID_ARGUMENT), "workspaceId and destinationId cannot be empty")
	}

	var startTime, endTime time.Time
	var err error

	startTime, err = time.Parse(time.RFC3339, req.GetStart())
	if err != nil {
		return &proto.RetrieveFailedBatchesResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time should be in correct %s format", time.RFC3339)
	}
	if req.GetEnd() != "" {
		endTime, err = time.Parse(time.RFC3339, req.GetEnd())
		if err != nil {
			return &proto.RetrieveFailedBatchesResponse{},
				status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "end time should be in correct %s format", time.RFC3339)
		}
		if !endTime.After(startTime) {
			return &proto.RetrieveFailedBatchesResponse{},
				status.Error(codes.Code(code.Code_INVALID_ARGUMENT), "end time should be after start time")
		}
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.GetWorkspaceID()]
	if len(sourceIDs) == 0 {
		return &proto.RetrieveFailedBatchesResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", req.GetWorkspaceID())
	}

	batches, err := g.uploadRepo.RetrieveFailedBatches(ctx, model.RetrieveFailedBatchesRequest{
		WorkspaceID:   req.GetWorkspaceID(),
		DestinationID: req.GetDestinationID(),
		Start:         startTime,
		End:           endTime,
	})
	if err != nil {
		log.Warnw("unable to get failed batches", lf.Error, err.Error())

		return &proto.RetrieveFailedBatchesResponse{},
			status.Error(codes.Code(code.Code_INTERNAL), "unable to get failed batches")
	}

	failedBatches := lo.Map(batches, func(item model.RetrieveFailedBatchesResponse, index int) *proto.FailedBatchInfo {
		return &proto.FailedBatchInfo{
			Error:             item.Error,
			ErrorCategory:     item.ErrorCategory,
			SourceID:          item.SourceID,
			FailedEventsCount: item.TotalEvents,
			FailedSyncsCount:  item.TotalSyncs,
			LastHappened:      timestamppb.New(item.LastHappenedAt),
			FirstHappened:     timestamppb.New(item.FirstHappenedAt),
			Status:            item.Status,
		}
	})
	return &proto.RetrieveFailedBatchesResponse{FailedBatches: failedBatches}, nil
}

func (g *GRPC) RetryFailedBatches(
	ctx context.Context,
	req *proto.RetryFailedBatchesRequest,
) (*proto.RetryFailedBatchesResponse, error) {
	log := g.logger.With(
		lf.WorkspaceID, req.GetWorkspaceID(),
		lf.DestinationID, req.GetDestinationID(),
		lf.StartTime, req.GetStart(),
		lf.EndTime, req.GetEnd(),
		lf.ErrorCategory, req.GetErrorCategory(),
		lf.SourceID, req.GetSourceID(),
		lf.Status, req.GetStatus(),
	)
	log.Infow("Retrying failed batches")

	if req.GetWorkspaceID() == "" || req.GetDestinationID() == "" {
		return &proto.RetryFailedBatchesResponse{},
			status.Error(codes.Code(code.Code_INVALID_ARGUMENT), "workspaceId and destinationId cannot be empty")
	}

	var startTime, endTime time.Time
	var err error

	startTime, err = time.Parse(time.RFC3339, req.GetStart())
	if err != nil {
		return &proto.RetryFailedBatchesResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time should be in correct %s format", time.RFC3339)
	}
	if req.GetEnd() != "" {
		endTime, err = time.Parse(time.RFC3339, req.GetEnd())
		if err != nil {
			return &proto.RetryFailedBatchesResponse{},
				status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "end time should be in correct %s format", time.RFC3339)
		}
		if !endTime.After(startTime) {
			return &proto.RetryFailedBatchesResponse{},
				status.Error(codes.Code(code.Code_INVALID_ARGUMENT), "end time should be after start time")
		}
	}

	sourceIDs := g.bcManager.SourceIDsByWorkspace()[req.GetWorkspaceID()]
	if len(sourceIDs) == 0 {
		return &proto.RetryFailedBatchesResponse{},
			status.Errorf(codes.Code(code.Code_UNAUTHENTICATED), "no sources found for workspace: %v", req.GetWorkspaceID())
	}
	if req.GetSourceID() != "" && !slices.Contains(sourceIDs, req.GetSourceID()) {
		return &proto.RetryFailedBatchesResponse{},
			status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	retriedCount, err := g.uploadRepo.RetryFailedBatches(ctx, model.RetryFailedBatchesRequest{
		WorkspaceID:   req.GetWorkspaceID(),
		DestinationID: req.GetDestinationID(),
		Start:         startTime,
		End:           endTime,
		ErrorCategory: req.GetErrorCategory(),
		SourceID:      req.GetSourceID(),
		Status:        req.GetStatus(),
	})
	if err != nil {
		log.Warnw("unable to retry failed batches", lf.Error, err.Error())

		return &proto.RetryFailedBatchesResponse{},
			status.Error(codes.Code(code.Code_INTERNAL), "unable to retry failed batches")
	}

	resp := &proto.RetryFailedBatchesResponse{
		RetriedSyncsCount: retriedCount,
	}
	return resp, nil
}

func statsInterceptor(statsFactory stats.Stats) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		res, err := handler(ctx, req)
		statusCode := codes.Unknown
		if s, ok := status.FromError(err); ok {
			statusCode = s.Code()
		}
		tags := stats.Tags{
			"reqType": info.FullMethod,
			"code":    strconv.Itoa(runtime.HTTPStatusFromCode(statusCode)),
		}
		statsFactory.NewTaggedStat("warehouse.grpc.response_time", stats.TimerType, tags).Since(start)
		return res, err
	}
}

func (g *GRPC) GetFirstAbortedUploadInContinuousAbortsByDestination(
	ctx context.Context,
	request *proto.FirstAbortedUploadInContinuousAbortsByDestinationRequest,
) (*proto.FirstAbortedUploadInContinuousAbortsByDestinationResponse, error) {
	g.logger.Infon(
		"Getting first aborted uploads in a series of continuous aborts",
		obskit.WorkspaceID(request.WorkspaceId),
		logger.NewStringField("startTime", request.Start),
	)

	var startTime time.Time
	var err error

	if request.GetStart() != "" {
		startTime, err = time.Parse(time.RFC3339, request.GetStart())
		if err != nil {
			return &proto.FirstAbortedUploadInContinuousAbortsByDestinationResponse{},
				status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time should be in correct %s format", time.RFC3339)
		}
	} else {
		startTime = time.Now().AddDate(0, 0, -30)
	}

	abortedUploadsInfo, err := g.uploadRepo.GetFirstAbortedUploadInContinuousAbortsByDestination(ctx, request.WorkspaceId, startTime)
	if err != nil {
		return &proto.FirstAbortedUploadInContinuousAbortsByDestinationResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to find first aborted uploads in a series of continuous aborts info: %v", err)
	}

	uploads := lo.Map(abortedUploadsInfo, func(item model.FirstAbortedUploadResponse, index int) *proto.FirstAbortedUploadResponse {
		return &proto.FirstAbortedUploadResponse{
			Id:            item.ID,
			SourceId:      item.SourceID,
			DestinationId: item.DestinationID,
			CreatedAt:     timestamppb.New(item.CreatedAt),
			FirstEventAt:  timestamppb.New(item.FirstEventAt),
			LastEventAt:   timestamppb.New(item.LastEventAt),
		}
	})

	return &proto.FirstAbortedUploadInContinuousAbortsByDestinationResponse{Uploads: uploads}, nil
}

// GetSyncLatency returns the sync latency for the given workspace, destination, start time and aggregation minutes
// If sourceID is provided, it will return the sync latency for the given sourceID
// If sourceID is not provided, it will return the sync latency for all sources of the given destination
func (g *GRPC) GetSyncLatency(ctx context.Context, request *proto.SyncLatencyRequest) (*proto.SyncLatencyResponse, error) {
	log := g.logger.Withn(
		obskit.WorkspaceID(request.WorkspaceId),
		obskit.SourceID(request.SourceId),
		obskit.DestinationID(request.DestinationId),
		logger.NewStringField("startTime", request.GetStartTime()),
		logger.NewStringField("aggregationMinutes", request.GetAggregationMinutes()),
	)
	log.Infon("Getting sync latency")

	if request.GetWorkspaceId() == "" || request.GetDestinationId() == "" {
		return &proto.SyncLatencyResponse{},
			status.Error(codes.Code(code.Code_INVALID_ARGUMENT), "workspaceID and destinationID cannot be empty")
	}
	if request.GetStartTime() == "" {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time cannot be empty")
	}
	if request.GetAggregationMinutes() == "" {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "aggregation minutes cannot be empty")
	}

	startTime, err := time.Parse(time.RFC3339, request.GetStartTime())
	if err != nil {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time %s should be in correct %s format", request.GetStartTime(), time.RFC3339)
	}
	minStartTime := g.now().AddDate(0, -g.config.maxLatencyQueryLookbackDays, 0)
	if startTime.Before(minStartTime) {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "start time cannot be older than %d days", g.config.maxLatencyQueryLookbackDays)
	}

	aggregationMinutes, err := strconv.Atoi(request.GetAggregationMinutes())
	if err != nil {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "aggregation minutes %s should be an integer", request.GetAggregationMinutes())
	}

	srcMap, _ := g.bcManager.ConnectionSourcesMap(request.GetDestinationId())
	if len(request.GetSourceId()) > 0 {
		if _, ok := srcMap[request.GetSourceId()]; !ok {
			return &proto.SyncLatencyResponse{},
				status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
		}
	}

	aggregationType, err := g.getLatencyAggregationType(srcMap, request.GetSourceId())
	if err != nil {
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), "unable to get latency aggregation type: %s", err)
	}

	syncLatencies, err := g.uploadRepo.GetSyncLatencies(ctx, model.SyncLatencyRequest{
		WorkspaceID:        request.GetWorkspaceId(),
		SourceID:           request.GetSourceId(),
		DestinationID:      request.GetDestinationId(),
		StartTime:          startTime,
		AggregationMinutes: int64(aggregationMinutes),
		AggregationType:    aggregationType,
	})
	if err != nil {
		log.Warnw("unable to get sync latencies", lf.Error, err.Error())
		return &proto.SyncLatencyResponse{},
			status.Errorf(codes.Code(code.Code_INTERNAL), "unable to get sync latencies: %v", err)
	}

	resp := &proto.SyncLatencyResponse{
		TimeSeriesDataPoints: lo.Map(syncLatencies, func(item model.LatencyTimeSeriesDataPoint, index int) *proto.LatencyTimeSeriesDataPoint {
			return &proto.LatencyTimeSeriesDataPoint{
				TimestampMillis: wrapperspb.Double(item.TimestampMillis),
				LatencySeconds:  wrapperspb.Double(item.LatencySeconds),
			}
		}),
	}
	return resp, nil
}

func (g *GRPC) getLatencyAggregationType(
	srcMap map[string]model.Warehouse, sourceID string,
) (model.LatencyAggregationType, error) {
	var minSyncFrequency int64 = math.MaxInt64
	for _, src := range srcMap {
		if len(sourceID) > 0 && src.Source.ID != sourceID {
			continue
		}

		freqInMin, err := strconv.ParseInt(src.GetStringDestinationConfig(g.conf, model.SyncFrequencySetting), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unable to parse sync frequency: %v", err)
		}

		minSyncFrequency = min(minSyncFrequency, freqInMin)
	}

	aggregationType := model.MaxLatency
	if minSyncFrequency < syncFrequencyThresholdMinutes {
		aggregationType = g.config.defaultLatencyAggregationType
	}
	return aggregationType, nil
}
