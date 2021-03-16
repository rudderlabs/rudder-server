package warehouse

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UploadsReqT struct {
	SourceID        string
	DestinationID   string
	DestinationType string
	Status          string
	Limit           int32
	Offset          int32
	API             UploadAPIT
}

type UploadReqT struct {
	UploadId int64
	API      UploadAPIT
}

type UploadsResT struct {
	Uploads    []UploadResT     `json:"uploads"`
	Pagination UploadPagination `json:"pagination"`
}

type UploadPagination struct {
	Total  int32 `json:"total"`
	Limit  int32 `json:"limit"`
	Offset int32 `json:"offset"`
}
type UploadResT struct {
	ID              int64             `json:"id"`
	Namespace       string            `json:"namespace"`
	SourceID        string            `json:"source_id"`
	DestinationID   string            `json:"destination_id"`
	DestinationType string            `json:"destination_type"`
	Status          string            `json:"status"`
	Error           string            `json:"error"`
	Attempt         int32             `json:"attempt"`
	Duration        int32             `json:"duration"`
	NextRetryTime   string            `json:"nextRetryTime"`
	FirstEventAt    time.Time         `json:"first_event_at"`
	LastEventAt     time.Time         `json:"last_event_at"`
	Tables          []TableUploadResT `json:"tables,omitempty"`
}

type TablesResT struct {
	Tables []TableUploadResT `json:"tables,omitempty"`
}
type TableUploadReqT struct {
	UploadID int64
	Name     string
	API      UploadAPIT
}

type TableUploadResT struct {
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
	log               logger.LoggerI
	connectionManager *controlplane.ConnectionManager
}

var UploadAPI UploadAPIT

func InitWarehouseAPI(dbHandle *sql.DB, log logger.LoggerI) {
	UploadAPI = UploadAPIT{
		enabled:  true,
		dbHandle: dbHandle,
		log:      log,
		connectionManager: &controlplane.ConnectionManager{
			AuthInfo: controlplane.AuthInfo{
				Service:        "warehouse",
				WorkspaceToken: config.GetWorkspaceToken(),
				InstanceID:     config.GetEnv("instance_id", "1"),
			},
			RetryInterval: 0,
			UseTLS:        config.GetEnvAsBool("CP_ROUTER_USE_TLS", true),
			Logger:        log,
			RegisterService: func(srv *grpc.Server) {
				proto.RegisterWarehouseServer(srv, &warehousegrpc{})
			},
		},
	}
}

func (uploadsReq *UploadsReqT) validateReq() error {
	if !uploadsReq.API.enabled || uploadsReq.API.log == nil || uploadsReq.API.dbHandle == nil {
		return errors.New(fmt.Sprint(`warehouse api's are not initialized`))
	}
	if uploadsReq.Limit < 1 {
		uploadsReq.Limit = 10
	}
	if uploadsReq.Offset < 0 {
		uploadsReq.Offset = 0
	}
	return nil
}

func (uploadsReq *UploadsReqT) getUploadsCount() (int32, error) {
	var count sql.NullInt32
	row := uploadsReq.API.dbHandle.QueryRow(fmt.Sprintf(`select count(*) from %s`, warehouseutils.WarehouseUploadsTable))
	err := row.Scan(&count)
	return count.Int32, err
}

var statusMap = map[string]string{
	"success": ExportedData,
	"waiting": Waiting,
	"aborted": Aborted,
	"failed":  "%failed%",
}

func (uploadsReq *UploadsReqT) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`select %s, count(*) OVER() AS total_uploads from %s WHERE `, selectFields, warehouseutils.WarehouseUploadsTable)
	var whereClauses []string
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
	} else {
		whereClauses = append(whereClauses, fmt.Sprintf(`status != '%s'`, "waiting"))
	}

	query = query + strings.Join(whereClauses, " AND ") + fmt.Sprintf(` order by id desc limit %d offset %d`, uploadsReq.Limit, uploadsReq.Offset)
	uploadsReq.API.log.Info(query)
	return query
}

func (uploadsReq *UploadsReqT) GetWhUploads() (uploadsRes *proto.WHUploadsResponse, err error) {
	uploads := make([]*proto.WHUploadResponse, 0)

	uploadsRes = &proto.WHUploadsResponse{
		Uploads: uploads,
	}
	err = uploadsReq.validateReq()
	if err != nil {
		return
	}
	uploadsRes.Pagination = &proto.Pagination{
		Limit:  uploadsReq.Limit,
		Offset: uploadsReq.Offset,
	}
	if err != nil {
		return
	}
	query := uploadsReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, status, error, first_event_at, last_event_at, timings, metadata->>'nextRetryTime'`)
	uploadsReq.API.log.Info(query)
	rows, err := uploadsReq.API.dbHandle.Query(query)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return
	}
	for rows.Next() {
		var upload proto.WHUploadResponse
		var nextRetryTimeStr sql.NullString
		var uploadError string
		var timingsObject sql.NullString
		var totalUploads int32
		var firstEventAt, lastEventAt sql.NullTime
		err = rows.Scan(&upload.Id, &upload.SourceId, &upload.DestinationId, &upload.DestinationType, &upload.Namespace, &upload.Status, &uploadError, &firstEventAt, &lastEventAt, &timingsObject, &nextRetryTimeStr, &totalUploads)
		if err != nil {
			uploadsReq.API.log.Errorf(err.Error())
			return &proto.WHUploadsResponse{}, err
		}
		uploadsRes.Pagination.Total = totalUploads
		upload.FirstEventAt = timestamppb.New(firstEventAt.Time)
		upload.LastEventAt = timestamppb.New(lastEventAt.Time)
		gjson.Parse(uploadError).ForEach(func(key gjson.Result, value gjson.Result) bool {
			upload.Attempt += int32(gjson.Get(value.String(), "attempt").Int())
			return true
		})
		// set error only for failed uploads. skip for retried and then successful uploads
		if upload.Status != ExportedData {
			lastFailedStatus := warehouseutils.GetLastFailedStatus(timingsObject)
			errorPath := fmt.Sprintf("%s.errors", lastFailedStatus)
			errors := gjson.Get(uploadError, errorPath).Array()
			if len(errors) > 0 {
				upload.Error = errors[len(errors)-1].String()
			}
		}
		// set nextRetryTime for non-aborted failed uploads
		if upload.Status != ExportedData && upload.Status != Aborted && nextRetryTimeStr.Valid {
			if nextRetryTime, err := time.Parse(time.RFC3339, nextRetryTimeStr.String); err == nil {
				upload.NextRetryTime = timestamppb.New(nextRetryTime)
			}
		}
		// set duration as time between first and last recorded timings
		// for ongoing/retrying uploads set diff between first and current time
		_, firstTime := warehouseutils.GetFirstTiming(timingsObject)
		if upload.Status == ExportedData || upload.Status == Aborted {
			_, lastTime := warehouseutils.GetLastTiming(timingsObject)
			upload.Duration = int32(lastTime.Sub(firstTime) / time.Second)
		} else {
			upload.Duration = int32(timeutil.Now().Sub(firstTime) / time.Second)
		}
		upload.Tables = make([]*proto.WHTable, 0)
		uploads = append(uploads, &upload)
	}
	uploadsRes.Uploads = uploads
	return
}

func (uploadReq UploadReqT) GetWHUpload() (*proto.WHUploadResponse, error) {
	err := uploadReq.validateReq()
	if err != nil {
		return &proto.WHUploadResponse{}, err
	}
	query := uploadReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, status, error, created_at, first_event_at, last_event_at, last_exec_at, timings, metadata->>'nextRetryTime'`)
	uploadReq.API.log.Debug(query)
	var upload proto.WHUploadResponse
	var nextRetryTimeStr sql.NullString
	var firstEventAt, lastEventAt, createdAt, lastExecAt sql.NullTime
	var timingsObject sql.NullString
	var uploadError string
	row := uploadReq.API.dbHandle.QueryRow(query)
	err = row.Scan(&upload.Id, &upload.SourceId, &upload.DestinationId, &upload.DestinationType, &upload.Namespace, &upload.Status, &uploadError, &createdAt, &firstEventAt, &lastEventAt, &lastExecAt, &timingsObject, &nextRetryTimeStr)
	if err != nil {
		uploadReq.API.log.Errorf(err.Error())
		return &proto.WHUploadResponse{}, err
	}
	upload.CreatedAt = timestamppb.New(createdAt.Time)
	upload.FirstEventAt = timestamppb.New(firstEventAt.Time)
	upload.LastEventAt = timestamppb.New(lastEventAt.Time)
	upload.LastExecAt = timestamppb.New(lastExecAt.Time)
	gjson.Parse(uploadError).ForEach(func(key gjson.Result, value gjson.Result) bool {
		upload.Attempt += int32(gjson.Get(value.String(), "attempt").Int())
		return true
	})
	// do not return error on successful upload
	if upload.Status != ExportedData {
		lastFailedStatus := warehouseutils.GetLastFailedStatus(timingsObject)
		errorPath := fmt.Sprintf("%s.errors", lastFailedStatus)
		errors := gjson.Get(uploadError, errorPath).Array()
		if len(errors) > 0 {
			upload.Error = errors[len(errors)-1].String()
		}
	}
	// set nextRetryTime for non-aborted failed uploads
	if upload.Status != ExportedData && upload.Status != Aborted && nextRetryTimeStr.Valid {
		if nextRetryTime, err := time.Parse(time.RFC3339, nextRetryTimeStr.String); err == nil {
			upload.NextRetryTime = timestamppb.New(nextRetryTime)
		}
	}
	// set duration as time between first and last recorded timings
	// for ongoing/retrying uploads set diff between first and current time
	_, firstTime := warehouseutils.GetFirstTiming(timingsObject)
	if upload.Status == ExportedData || upload.Status == Aborted {
		_, lastTime := warehouseutils.GetLastTiming(timingsObject)
		upload.Duration = int32(lastTime.Sub(firstTime) / time.Second)
	} else {
		upload.Duration = int32(timeutil.Now().Sub(firstTime) / time.Second)
	}
	tableUploadReq := TableUploadReqT{
		UploadID: upload.Id,
		Name:     "",
		API:      uploadReq.API,
	}
	tables, err := tableUploadReq.GetWhTableUploads()
	if err != nil {
		return &proto.WHUploadResponse{}, err
	}
	upload.Tables = tables
	return &upload, nil
}

func (tableUploadReq TableUploadReqT) GetWhTableUploads() ([]*proto.WHTable, error) {
	err := tableUploadReq.validateReq()
	if err != nil {
		return []*proto.WHTable{}, err
	}
	query := tableUploadReq.generateQuery(`id, wh_upload_id, table_name, total_events, status, error, last_exec_time, updated_at`)
	tableUploadReq.API.log.Debug(query)
	rows, err := tableUploadReq.API.dbHandle.Query(query)
	if err != nil {
		tableUploadReq.API.log.Errorf(err.Error())
		return []*proto.WHTable{}, err
	}
	var tableUploads []*proto.WHTable
	for rows.Next() {
		var tableUpload proto.WHTable
		var count sql.NullInt32
		var lastExecTime, updatedAt sql.NullTime
		err = rows.Scan(&tableUpload.Id, &tableUpload.UploadId, &tableUpload.Name, &count, &tableUpload.Status, &tableUpload.Error, &lastExecTime, &updatedAt)
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
		if tableUpload.Status == ExportedData {
			tableUpload.Error = ""
		}
		if lastExecTime.Valid {
			tableUpload.LastExecAt = timestamppb.New(lastExecTime.Time)
			tableUpload.Duration = int32(updatedAt.Time.Sub(lastExecTime.Time) / time.Second)
		}
		tableUploads = append(tableUploads, &tableUpload)
	}
	return tableUploads, nil
}

func (tableUploadReq TableUploadReqT) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`select %s from %s where wh_upload_id = %d`, selectFields, warehouseutils.WarehouseTableUploadsTable, tableUploadReq.UploadID)
	if len(strings.TrimSpace(tableUploadReq.Name)) > 0 {
		query = fmt.Sprintf(`%s and table_name = %s`, query, tableUploadReq.Name)
	}
	return query
}

func (tableUploadReq TableUploadReqT) validateReq() error {
	if !tableUploadReq.API.enabled || tableUploadReq.API.log == nil || tableUploadReq.API.dbHandle == nil {
		return errors.New(fmt.Sprint(`warehouse api's are not initialized`))
	}
	if tableUploadReq.UploadID == 0 {
		return errors.New(fmt.Sprint(`upload_id is empty or should be greater than 0 `))
	}
	return nil
}

func (uploadReq UploadReqT) generateQuery(selectedFields string) string {
	return fmt.Sprintf(`select %s from %s  where id = %d`, selectedFields, warehouseutils.WarehouseUploadsTable, uploadReq.UploadId)
}

func (uploadReq UploadReqT) validateReq() error {
	if !uploadReq.API.enabled || uploadReq.API.log == nil || uploadReq.API.dbHandle == nil {
		return errors.New(fmt.Sprint(`warehouse api's are not initialized`))
	}
	if uploadReq.UploadId < 1 {
		return errors.New(fmt.Sprint(`upload_id is empty or should be greater than 0 `))
	}
	return nil
}
