package warehouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/exp/slices"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/filemanager"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/filemanagerutil"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/utils/types/deployment"
	cpclient "github.com/rudderlabs/rudder-server/warehouse/client/controlplane"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

type UploadsReq struct {
	WorkspaceID     string
	SourceID        string
	DestinationID   string
	DestinationType string
	Status          string
	Limit           int32
	Offset          int32
	API             UploadAPIT
}

type UploadReq struct {
	WorkspaceID string
	UploadId    int64
	API         UploadAPIT
}

type UploadsRes struct {
	Uploads    []UploadRes      `json:"uploads"`
	Pagination UploadPagination `json:"pagination"`
}

type UploadPagination struct {
	Total  int32 `json:"total"`
	Limit  int32 `json:"limit"`
	Offset int32 `json:"offset"`
}
type UploadRes struct {
	ID              int64            `json:"id"`
	Namespace       string           `json:"namespace"`
	SourceID        string           `json:"source_id"`
	DestinationID   string           `json:"destination_id"`
	DestinationType string           `json:"destination_type"`
	Status          string           `json:"status"`
	Error           string           `json:"error"`
	Attempt         int32            `json:"attempt"`
	Duration        int32            `json:"duration"`
	NextRetryTime   string           `json:"nextRetryTime"`
	FirstEventAt    time.Time        `json:"first_event_at"`
	LastEventAt     time.Time        `json:"last_event_at"`
	Tables          []TableUploadRes `json:"tables,omitempty"`
}

type TablesRes struct {
	Tables []TableUploadRes `json:"tables,omitempty"`
}

type TableUploadReq struct {
	UploadID int64
	Name     string
	API      UploadAPIT
}

type TableUploadRes struct {
	ID         int64     `json:"id"`
	UploadID   int64     `json:"upload_id"`
	Name       string    `json:"name"`
	Error      string    `json:"error"`
	Status     string    `json:"status"`
	Count      int32     `json:"count"`
	LastExecAt time.Time `json:"last_exec_at"`
	Duration   int32     `json:"duration"`
}

type UploadAPIT struct {
	enabled           bool
	dbHandle          *sql.DB
	warehouseDBHandle *DB
	log               logger.Logger
	connectionManager *controlplane.ConnectionManager
	bcManager         *backendConfigManager
	isMultiWorkspace  bool
}

var UploadAPI UploadAPIT

const (
	TriggeredSuccessfully   = "Triggered successfully"
	NoPendingEvents         = "No pending events to sync for this destination"
	DownloadFileNamePattern = "downloadfile.*.tmp"
	NoSuchSync              = "No such sync exist"
)

func InitWarehouseAPI(dbHandle *sql.DB, bcManager *backendConfigManager, log logger.Logger) error {
	connectionToken, tokenType, isMultiWorkspace, err := deployment.GetConnectionToken()
	if err != nil {
		return err
	}
	labels := map[string]string{}
	if region := config.GetString("region", ""); region != "" {
		labels["region"] = region
	}

	client := cpclient.NewInternalClientWithCache(
		config.GetString("CONFIG_BACKEND_URL", "api.rudderlabs.com"),
		cpclient.BasicAuth{
			Username: config.GetString("CP_INTERNAL_API_USERNAME", ""),
			Password: config.GetString("CP_INTERNAL_API_PASSWORD", ""),
		},
	)

	UploadAPI = UploadAPIT{
		enabled:           true,
		dbHandle:          dbHandle,
		warehouseDBHandle: NewWarehouseDB(wrappedDBHandle),
		log:               log,
		isMultiWorkspace:  isMultiWorkspace,
		connectionManager: &controlplane.ConnectionManager{
			AuthInfo: controlplane.AuthInfo{
				Service:         "warehouse",
				ConnectionToken: connectionToken,
				InstanceID:      config.GetString("INSTANCE_ID", "1"),
				TokenType:       tokenType,
				Labels:          labels,
			},
			RetryInterval: 0,
			UseTLS:        config.GetBool("CP_ROUTER_USE_TLS", true),
			Logger:        log,
			RegisterService: func(srv *grpc.Server) {
				proto.RegisterWarehouseServer(srv, &warehouseGRPC{
					EnableTunnelling: config.GetBool("ENABLE_TUNNELLING", true),
					CPClient:         client,
				})
			},
		},
		bcManager: bcManager,
	}
	return nil
}

func (uploadsReq *UploadsReq) validateReq() error {
	if !uploadsReq.API.enabled || uploadsReq.API.log == nil || uploadsReq.API.dbHandle == nil {
		return errors.New(`warehouse api are not initialized`)
	}
	if uploadsReq.Limit < 1 {
		uploadsReq.Limit = 10
	}
	if uploadsReq.Offset < 0 {
		uploadsReq.Offset = 0
	}
	return nil
}

var statusMap = map[string]string{
	"success": model.ExportedData,
	"waiting": model.Waiting,
	"aborted": model.Aborted,
	"failed":  "%failed%",
}

func (uploadsReq *UploadsReq) GetWhUploads(ctx context.Context) (uploadsRes *proto.WHUploadsResponse, err error) {
	uploadsRes = &proto.WHUploadsResponse{
		Uploads: make([]*proto.WHUploadResponse, 0),
	}

	err = uploadsReq.validateReq()
	if err != nil {
		return
	}

	uploadsRes.Pagination = &proto.Pagination{
		Limit:  uploadsReq.Limit,
		Offset: uploadsReq.Offset,
	}

	// TODO: workspace ID can be used
	authorizedSourceIDs := uploadsReq.authorizedSources()
	if len(authorizedSourceIDs) == 0 {
		return uploadsRes, nil
	}

	if UploadAPI.isMultiWorkspace {
		uploadsRes, err = uploadsReq.warehouseUploadsForHosted(ctx, authorizedSourceIDs, `id, source_id, destination_id, destination_type, namespace, status, error, first_event_at, last_event_at, last_exec_at, updated_at, timings, metadata->>'nextRetryTime', metadata->>'archivedStagingAndLoadFiles'`)
		return
	}

	uploadsRes, err = uploadsReq.warehouseUploads(ctx, `id, source_id, destination_id, destination_type, namespace, status, error, first_event_at, last_event_at, last_exec_at, updated_at, timings, metadata->>'nextRetryTime', metadata->>'archivedStagingAndLoadFiles'`)
	return
}

func (uploadsReq *UploadsReq) TriggerWhUploads(ctx context.Context) (response *proto.TriggerWhUploadsResponse, err error) {
	err = uploadsReq.validateReq()
	defer func() {
		if err != nil {
			response = &proto.TriggerWhUploadsResponse{
				Message:    err.Error(),
				StatusCode: http.StatusBadRequest,
			}
		}
	}()

	if err != nil {
		return
	}
	authorizedSourceIDs := uploadsReq.authorizedSources()
	if len(authorizedSourceIDs) == 0 {
		err = fmt.Errorf("no authorized sourceId's")
		return
	}
	if uploadsReq.DestinationID == "" {
		err = fmt.Errorf("valid destinationId must be provided")
		return
	}
	var pendingStagingFileCount int64

	filterBy := []warehouseutils.FilterBy{{Key: "destination_id", Value: uploadsReq.DestinationID}}
	pendingUploadCount, err := getPendingUploadCount(filterBy...)
	if err != nil {
		return
	}
	if pendingUploadCount == int64(0) {
		pendingStagingFileCount, err = repo.NewStagingFiles(wrappedDBHandle).CountPendingForDestination(ctx, uploadsReq.DestinationID)
		if err != nil {
			return
		}
	}
	if (pendingUploadCount + pendingStagingFileCount) == int64(0) {
		err = nil
		response = &proto.TriggerWhUploadsResponse{
			Message:    NoPendingEvents,
			StatusCode: http.StatusOK,
		}
		return
	}
	err = TriggerUploadHandler(uploadsReq.SourceID, uploadsReq.DestinationID)
	if err != nil {
		return
	}
	response = &proto.TriggerWhUploadsResponse{
		Message:    TriggeredSuccessfully,
		StatusCode: http.StatusOK,
	}
	return
}

func (uploadReq *UploadReq) GetWHUpload(ctx context.Context) (*proto.WHUploadResponse, error) {
	err := uploadReq.validateReq()
	if err != nil {
		return &proto.WHUploadResponse{}, status.Errorf(codes.Code(code.Code_INVALID_ARGUMENT), err.Error())
	}

	query := uploadReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, status, error, created_at, first_event_at, last_event_at, last_exec_at, updated_at, timings, metadata->>'nextRetryTime', metadata->>'archivedStagingAndLoadFiles'`)
	uploadReq.API.log.Debug(query)

	var (
		upload                                                      proto.WHUploadResponse
		nextRetryTimeStr                                            sql.NullString
		firstEventAt, lastEventAt, createdAt, lastExecAt, updatedAt sql.NullTime
		timingsObject                                               sql.NullString
		uploadError                                                 string
		isUploadArchived                                            sql.NullBool
	)

	row := uploadReq.API.dbHandle.QueryRowContext(ctx, query)
	err = row.Scan(
		&upload.Id,
		&upload.SourceId,
		&upload.DestinationId,
		&upload.DestinationType,
		&upload.Namespace,
		&upload.Status,
		&uploadError,
		&createdAt,
		&firstEventAt,
		&lastEventAt,
		&lastExecAt,
		&updatedAt,
		&timingsObject,
		&nextRetryTimeStr,
		&isUploadArchived,
	)
	if err == sql.ErrNoRows {
		return &proto.WHUploadResponse{}, status.Errorf(codes.Code(code.Code_NOT_FOUND), "sync not found")
	}
	if err != nil {
		uploadReq.API.log.Errorf(err.Error())
		return &proto.WHUploadResponse{}, status.Errorf(codes.Code(code.Code_INTERNAL), err.Error())
	}

	if !uploadReq.authorizeSource(upload.SourceId) {
		pkgLogger.Errorf(`Unauthorized request for upload:%d with sourceId:%s in workspaceId:%s`, uploadReq.UploadId, upload.SourceId, uploadReq.WorkspaceID)
		return &proto.WHUploadResponse{}, status.Error(codes.Code(code.Code_UNAUTHENTICATED), "unauthorized request")
	}

	upload.CreatedAt = timestamppb.New(createdAt.Time)
	upload.FirstEventAt = timestamppb.New(firstEventAt.Time)
	upload.LastEventAt = timestamppb.New(lastEventAt.Time)
	upload.LastExecAt = timestamppb.New(lastExecAt.Time)
	upload.IsArchivedUpload = isUploadArchived.Bool
	gjson.Parse(uploadError).ForEach(func(key, value gjson.Result) bool {
		upload.Attempt += int32(gjson.Get(value.String(), "attempt").Int())
		return true
	})
	// do not return error on successful upload
	if upload.Status != model.ExportedData {
		var timings model.Timings
		_ = json.Unmarshal([]byte(timingsObject.String), &timings)

		lastFailedStatus := model.GetLastFailedStatus(timings)
		errorPath := fmt.Sprintf("%s.errors", lastFailedStatus)
		errs := gjson.Get(uploadError, errorPath).Array()
		if len(errs) > 0 {
			upload.Error = errs[len(errs)-1].String()
		}
	}
	// set nextRetryTime for non-aborted failed uploads
	if upload.Status != model.ExportedData && upload.Status != model.Aborted && nextRetryTimeStr.Valid {
		if nextRetryTime, err := time.Parse(time.RFC3339, nextRetryTimeStr.String); err == nil {
			upload.NextRetryTime = timestamppb.New(nextRetryTime)
		}
	}
	// set duration as time between updatedAt and lastExec recorded timings
	// for ongoing/retrying uploads set diff between lastExec and current time
	if upload.Status == model.ExportedData || upload.Status == model.Aborted {
		upload.Duration = int32(updatedAt.Time.Sub(lastExecAt.Time) / time.Second)
	} else {
		upload.Duration = int32(timeutil.Now().Sub(lastExecAt.Time) / time.Second)
	}

	tableUploadReq := TableUploadReq{
		UploadID: upload.Id,
		Name:     "",
		API:      uploadReq.API,
	}
	upload.Tables, err = tableUploadReq.GetWhTableUploads(ctx)
	if err != nil {
		return &proto.WHUploadResponse{}, status.Errorf(codes.Code(code.Code_INTERNAL), err.Error())
	}

	return &upload, nil
}

func (uploadReq *UploadReq) TriggerWHUpload(ctx context.Context) (response *proto.TriggerWhUploadsResponse, err error) {
	err = uploadReq.validateReq()
	defer func() {
		if err != nil {
			response = &proto.TriggerWhUploadsResponse{
				Message:    err.Error(),
				StatusCode: http.StatusBadRequest,
			}
		}
	}()
	if err != nil {
		return
	}

	upload, err := repo.NewUploads(uploadReq.API.warehouseDBHandle.handle).Get(ctx, uploadReq.UploadId)
	if err == model.ErrUploadNotFound {
		return &proto.TriggerWhUploadsResponse{
			Message:    NoSuchSync,
			StatusCode: http.StatusOK,
		}, nil
	}
	if err != nil {
		uploadReq.API.log.Errorf(err.Error())
		return
	}

	if !uploadReq.authorizeSource(upload.SourceID) {
		pkgLogger.Errorf(`Unauthorized request for upload:%d with sourceId:%s in workspaceId:%s`, uploadReq.UploadId, upload.SourceID, uploadReq.WorkspaceID)
		err = errors.New("unauthorized request")
		return
	}

	uploadJobT := UploadJob{
		upload:   upload,
		dbHandle: uploadReq.API.warehouseDBHandle.handle,
		now:      timeutil.Now,
		ctx:      ctx,
	}

	err = uploadJobT.triggerUploadNow()
	if err != nil {
		return
	}
	response = &proto.TriggerWhUploadsResponse{
		Message:    TriggeredSuccessfully,
		StatusCode: http.StatusOK,
	}
	return
}

func (tableUploadReq TableUploadReq) GetWhTableUploads(ctx context.Context) ([]*proto.WHTable, error) {
	err := tableUploadReq.validateReq()
	if err != nil {
		return []*proto.WHTable{}, err
	}
	query := tableUploadReq.generateQuery(`id, wh_upload_id, table_name, total_events, status, error, last_exec_time, updated_at`)
	tableUploadReq.API.log.Debug(query)
	rows, err := tableUploadReq.API.dbHandle.QueryContext(ctx, query)
	if err != nil {
		tableUploadReq.API.log.Errorf(err.Error())
		return []*proto.WHTable{}, err
	}
	var tableUploads []*proto.WHTable
	for rows.Next() {
		var tableUpload proto.WHTable
		var count sql.NullInt32
		var lastExecTime, updatedAt sql.NullTime
		err = rows.Scan(
			&tableUpload.Id,
			&tableUpload.UploadId,
			&tableUpload.Name,
			&count,
			&tableUpload.Status,
			&tableUpload.Error,
			&lastExecTime,
			&updatedAt,
		)
		if err != nil {
			tableUploadReq.API.log.Errorf(err.Error())
			return []*proto.WHTable{}, err
		}
		if count.Valid {
			tableUpload.Count = count.Int32
		}
		// do not include rudder_discards table if no events are present
		if strings.ToLower(tableUpload.Name) == warehouseutils.DiscardsTable && !count.Valid {
			continue
		}
		// do not return error on successful upload
		if tableUpload.Status == model.ExportedData {
			tableUpload.Error = ""
		}
		if lastExecTime.Valid {
			tableUpload.LastExecAt = timestamppb.New(lastExecTime.Time)
			tableUpload.Duration = int32(updatedAt.Time.Sub(lastExecTime.Time) / time.Second)
		}
		tableUploads = append(tableUploads, &tableUpload)
	}
	if err = rows.Err(); err != nil {
		tableUploadReq.API.log.Errorf(err.Error())
		return []*proto.WHTable{}, err
	}

	return tableUploads, nil
}

func (tableUploadReq TableUploadReq) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`
	SELECT
	  %s
	FROM
	  %s
	WHERE
	  wh_upload_id = %d
`,
		selectFields,
		warehouseutils.WarehouseTableUploadsTable,
		tableUploadReq.UploadID,
	)
	if len(strings.TrimSpace(tableUploadReq.Name)) > 0 {
		query = fmt.Sprintf(`%s and table_name = %s`, query, tableUploadReq.Name)
	}
	return query
}

func (tableUploadReq TableUploadReq) validateReq() error {
	if !tableUploadReq.API.enabled || tableUploadReq.API.log == nil || tableUploadReq.API.dbHandle == nil {
		return errors.New("warehouse api are not initialized")
	}
	if tableUploadReq.UploadID == 0 {
		return errors.New("upload_id is empty or should be greater than 0")
	}
	return nil
}

func (uploadReq *UploadReq) generateQuery(selectedFields string) string {
	return fmt.Sprintf(`
		SELECT
		  %s
		FROM
		  %s
		WHERE
		  id = %d
`,
		selectedFields,
		warehouseutils.WarehouseUploadsTable,
		uploadReq.UploadId,
	)
}

func (uploadReq *UploadReq) validateReq() error {
	if !uploadReq.API.enabled || uploadReq.API.log == nil || uploadReq.API.dbHandle == nil {
		return errors.New("warehouse api are not initialized")
	}
	if uploadReq.UploadId < 1 {
		return errors.New(`upload_id is empty or should be greater than 0 `)
	}
	return nil
}

func (uploadReq *UploadReq) authorizeSource(sourceID string) bool {
	currentList := uploadReq.API.bcManager.SourceIDsByWorkspace()
	authorizedSourceIDs, ok := currentList[uploadReq.WorkspaceID]
	if !ok {
		pkgLogger.Errorf(`Could not find sourceID in workspace %q: %v`, uploadReq.WorkspaceID, currentList)
		return false
	}

	pkgLogger.Debugf(`Authorized sourceID for workspace %q: %v`, uploadReq.WorkspaceID, authorizedSourceIDs)
	return slices.Contains(authorizedSourceIDs, sourceID)
}

func (uploadsReq *UploadsReq) authorizedSources() []string {
	pkgLogger.Debugf(`Getting authorizedSourceIDs for workspace:%s`, uploadsReq.WorkspaceID)
	return uploadsReq.API.bcManager.SourceIDsByWorkspace()[uploadsReq.WorkspaceID]
}

func (uploadsReq *UploadsReq) getUploadsFromDB(ctx context.Context, isMultiWorkspace bool, query string) ([]*proto.WHUploadResponse, int32, error) {
	var totalUploadCount int32
	var err error
	uploads := make([]*proto.WHUploadResponse, 0)

	rows, err := uploadsReq.API.dbHandle.QueryContext(ctx, query)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return nil, 0, err
	}

	for rows.Next() {
		var upload proto.WHUploadResponse
		var nextRetryTimeStr sql.NullString
		var uploadError string
		var timingsObject sql.NullString
		var totalUploads int32
		var firstEventAt, lastEventAt, lastExecAt, updatedAt sql.NullTime
		var isUploadArchived sql.NullBool

		// total upload count is also a part of these rows if the query was made for a hosted workspace
		if isMultiWorkspace {
			err = rows.Scan(
				&upload.Id,
				&upload.SourceId,
				&upload.DestinationId,
				&upload.DestinationType,
				&upload.Namespace,
				&upload.Status,
				&uploadError,
				&firstEventAt,
				&lastEventAt,
				&lastExecAt,
				&updatedAt,
				&timingsObject,
				&nextRetryTimeStr,
				&isUploadArchived,
				&totalUploads,
			)
			if err != nil {
				uploadsReq.API.log.Errorf(err.Error())
				return nil, totalUploadCount, err
			}
			totalUploadCount = totalUploads
		} else {
			err = rows.Scan(
				&upload.Id,
				&upload.SourceId,
				&upload.DestinationId,
				&upload.DestinationType,
				&upload.Namespace,
				&upload.Status,
				&uploadError,
				&firstEventAt,
				&lastEventAt,
				&lastExecAt,
				&updatedAt,
				&timingsObject,
				&nextRetryTimeStr,
				&isUploadArchived,
			)
			if err != nil {
				uploadsReq.API.log.Errorf(err.Error())
				return nil, totalUploadCount, err
			}
		}

		upload.FirstEventAt = timestamppb.New(firstEventAt.Time)
		upload.LastEventAt = timestamppb.New(lastEventAt.Time)
		upload.IsArchivedUpload = isUploadArchived.Bool // will be false if archivedStagingAndLoadFiles is not set
		gjson.Parse(uploadError).ForEach(func(key, value gjson.Result) bool {
			upload.Attempt += int32(gjson.Get(value.String(), "attempt").Int())
			return true
		})
		// set error only for failed uploads. skip for retried and then successful uploads
		if upload.Status != model.ExportedData {

			// TODO: remove this once we use repository to get upload
			var timings model.Timings
			_ = json.Unmarshal([]byte(timingsObject.String), &timings)

			lastFailedStatus := model.GetLastFailedStatus(timings)
			errorPath := fmt.Sprintf("%s.errors", lastFailedStatus)
			errs := gjson.Get(uploadError, errorPath).Array()
			if len(errs) > 0 {
				upload.Error = errs[len(errs)-1].String()
			}
		}
		// set nextRetryTime for non-aborted failed uploads
		if upload.Status != model.ExportedData && upload.Status != model.Aborted && nextRetryTimeStr.Valid {
			if nextRetryTime, err := time.Parse(time.RFC3339, nextRetryTimeStr.String); err == nil {
				upload.NextRetryTime = timestamppb.New(nextRetryTime)
			}
		}
		// set duration as time between updatedAt and lastExec recorded timings
		// for ongoing/retrying uploads set diff between lastExec and current time
		if upload.Status == model.ExportedData || upload.Status == model.Aborted {
			upload.Duration = int32(updatedAt.Time.Sub(lastExecAt.Time) / time.Second)
		} else {
			upload.Duration = int32(timeutil.Now().Sub(lastExecAt.Time) / time.Second)
		}
		upload.Tables = make([]*proto.WHTable, 0)
		uploads = append(uploads, &upload)
	}
	if err = rows.Err(); err != nil {
		return nil, 0, err
	}

	return uploads, totalUploadCount, err
}

func (uploadsReq *UploadsReq) getTotalUploadCount(ctx context.Context, whereClause string) (int32, error) {
	var totalUploadCount int32
	query := fmt.Sprintf(`
	select
	  count(*)
	from
	  %s
`,
		warehouseutils.WarehouseUploadsTable,
	)
	if whereClause != "" {
		query += fmt.Sprintf(` %s`, whereClause)
	}
	uploadsReq.API.log.Info(query)
	err := uploadsReq.API.dbHandle.QueryRowContext(ctx, query).Scan(&totalUploadCount)
	return totalUploadCount, err
}

// for hosted workspaces - we get the uploads and the total upload count using the same query
func (uploadsReq *UploadsReq) warehouseUploadsForHosted(ctx context.Context, authorizedSourceIDs []string, selectFields string) (uploadsRes *proto.WHUploadsResponse, err error) {
	var (
		uploads          []*proto.WHUploadResponse
		totalUploadCount int32
		whereClauses     []string
		subQuery         string
		query            string
	)

	// create query
	subQuery = fmt.Sprintf(`
		SELECT
		  %s,
		  COUNT(*) OVER() AS total_uploads
		FROM
		  %s
		WHERE
`,
		selectFields,
		warehouseutils.WarehouseUploadsTable,
	)
	if uploadsReq.SourceID == "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`source_id IN (%v)`, misc.SingleQuoteLiteralJoin(authorizedSourceIDs)))
	} else if slices.Contains(authorizedSourceIDs, uploadsReq.SourceID) {
		whereClauses = append(whereClauses, fmt.Sprintf(`source_id = '%s'`, uploadsReq.SourceID))
	}
	if uploadsReq.DestinationID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`destination_id = '%s'`, uploadsReq.DestinationID))
	}
	if uploadsReq.DestinationType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`destination_type = '%s'`, uploadsReq.DestinationType))
	}
	if uploadsReq.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`status like '%s'`, statusMap[uploadsReq.Status]))
	}

	subQuery = subQuery + strings.Join(whereClauses, " AND ")
	query = fmt.Sprintf(`
		SELECT
		  *
		FROM
		  (%s) p
		ORDER BY
		  id DESC
		LIMIT
		  %d OFFSET %d
`,
		subQuery,
		uploadsReq.Limit,
		uploadsReq.Offset,
	)
	uploadsReq.API.log.Info(query)

	// get uploads from db
	uploads, totalUploadCount, err = uploadsReq.getUploadsFromDB(ctx, true, query)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return &proto.WHUploadsResponse{}, err
	}

	// create response
	uploadsRes = &proto.WHUploadsResponse{
		Uploads: uploads,
		Pagination: &proto.Pagination{
			Limit:  uploadsReq.Limit,
			Offset: uploadsReq.Offset,
			Total:  totalUploadCount,
		},
	}
	return
}

// for non hosted workspaces - we get the uploads and the total upload count using separate queries
func (uploadsReq *UploadsReq) warehouseUploads(ctx context.Context, selectFields string) (uploadsRes *proto.WHUploadsResponse, err error) {
	var (
		uploads          []*proto.WHUploadResponse
		totalUploadCount int32
		query            string
		whereClause      string
		whereClauses     []string
	)

	// create query
	query = fmt.Sprintf(`
		select
		  %s
		from
		  %s
`,
		selectFields,
		warehouseutils.WarehouseUploadsTable,
	)
	if uploadsReq.SourceID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`source_id = '%s'`, uploadsReq.SourceID))
	}
	if uploadsReq.DestinationID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`destination_id = '%s'`, uploadsReq.DestinationID))
	}
	if uploadsReq.DestinationType != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`destination_type = '%s'`, uploadsReq.DestinationType))
	}
	if uploadsReq.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`status like '%s'`, statusMap[uploadsReq.Status]))
	}
	if len(whereClauses) > 0 {
		whereClause = fmt.Sprintf(` WHERE %s`, strings.Join(whereClauses, " AND "))
	}

	query = query + whereClause + fmt.Sprintf(` order by id desc limit %d offset %d`, uploadsReq.Limit, uploadsReq.Offset)
	uploadsReq.API.log.Info(query)

	// we get uploads for non hosted workspaces in two steps
	// this is because getting this info via 2 queries is faster than getting it via one query(using the 'count(*) OVER()' clause)
	// step1 - get all uploads
	uploads, _, err = uploadsReq.getUploadsFromDB(ctx, false, query)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return &proto.WHUploadsResponse{}, err
	}
	// step2 - get total upload count
	totalUploadCount, err = uploadsReq.getTotalUploadCount(ctx, whereClause)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return &proto.WHUploadsResponse{}, err
	}

	// create response
	uploadsRes = &proto.WHUploadsResponse{
		Uploads: uploads,
		Pagination: &proto.Pagination{
			Limit:  uploadsReq.Limit,
			Offset: uploadsReq.Offset,
			Total:  totalUploadCount,
		},
	}
	return
}

// checkMapForValidKey checks the presence of key in map
// and if yes verifies that the key is string and non-empty.
func checkMapForValidKey(configMap map[string]interface{}, key string) bool {
	value, ok := configMap[key]
	if !ok {
		return false
	}

	if valStr, ok := value.(string); ok {
		return valStr != ""
	}
	return false
}

func validateObjectStorage(ctx context.Context, request *ObjectStorageValidationRequest) error {
	pkgLogger.Infof("Received call to validate object storage for type: %s\n", request.Type)

	settings, err := getFileManagerSettings(ctx, request.Type, request.Config)
	if err != nil {
		return fmt.Errorf("unable to create file manager settings: \n%s", err.Error())
	}

	fileManager, err := filemanager.New(settings)
	if err != nil {
		return fmt.Errorf("unable to create file manager: \n%s", err.Error())
	}

	req := backendconfig.DestinationT{
		DestinationDefinition: backendconfig.DestinationDefinitionT{Name: request.Type},
	}

	filePath, err := validations.CreateTempLoadFile(&req)
	if err != nil {
		return fmt.Errorf("unable to create temp load file: \n%w", err)
	}
	defer os.Remove(filePath)

	f, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("unable to open path to temporary file: \n%w", err)
	}

	uploadOutput, err := fileManager.Upload(ctx, f)
	if err != nil {
		return InvalidDestinationCredErr{Base: err, Operation: "upload"}
	}
	_ = f.Close()

	key := fileManager.GetDownloadKeyFromFileLocation(uploadOutput.Location)

	tmpDirectory, err := misc.CreateTMPDIR()
	if err != nil {
		return fmt.Errorf("error while Creating file to download data")
	}
	f, err = os.CreateTemp(tmpDirectory, DownloadFileNamePattern)
	if err != nil {
		return fmt.Errorf("error while Creating file to download data")
	}

	defer os.Remove(f.Name())

	err = fileManager.Download(ctx, f, key)
	if err != nil {
		return InvalidDestinationCredErr{Base: err, Operation: "download"}
	}
	_ = f.Close()

	return nil
}

func getFileManagerSettings(ctx context.Context, provider string, inputConfig map[string]interface{}) (*filemanager.Settings, error) {
	settings := &filemanager.Settings{
		Provider: provider,
		Config:   inputConfig,
	}

	if err := overrideWithEnv(ctx, settings); err != nil {
		return nil, fmt.Errorf("overriding config with env: %w", err)
	}
	return settings, nil
}

// overrideWithEnv overrides the config keys in the fileManager settings
// with fallback values pulled from env. Only supported for S3 for now.
func overrideWithEnv(ctx context.Context, settings *filemanager.Settings) error {
	envConfig := filemanager.GetProviderConfigFromEnv(filemanagerutil.ProviderConfigOpts(ctx, settings.Provider, config.Default))

	if settings.Provider == "S3" {
		ifNotExistThenSet("prefix", envConfig["prefix"], settings.Config)
		ifNotExistThenSet("accessKeyID", envConfig["accessKeyID"], settings.Config)
		ifNotExistThenSet("accessKey", envConfig["accessKey"], settings.Config)
		ifNotExistThenSet("enableSSE", envConfig["enableSSE"], settings.Config)
		ifNotExistThenSet("iamRoleARN", envConfig["iamRoleArn"], settings.Config)
		ifNotExistThenSet("externalID", envConfig["externalID"], settings.Config)
		ifNotExistThenSet("regionHint", envConfig["regionHint"], settings.Config)
	}
	return ctx.Err()
}

func ifNotExistThenSet(keyToReplace string, replaceWith interface{}, configMap map[string]interface{}) {
	if _, ok := configMap[keyToReplace]; !ok {
		// In case we don't have the key, simply replace it with replaceWith
		configMap[keyToReplace] = replaceWith
	}
}
