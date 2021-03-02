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
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/tidwall/gjson"
	"google.golang.org/grpc"
)

type UploadsReqT struct {
	SourceID           string
	DestinationID      string
	DestinationType    string
	Status             string
	IncludeTablesInRes bool
	Limit              int32
	Offset             int32
	API                UploadAPIT
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
			UseTLS:        false,
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
	"success": "exported_data",
	"waiting": "waiting",
	"aborted": "aborted",
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

func (uploadsReq *UploadsReqT) GetWhUploads() (uploadsRes UploadsResT, err error) {
	uploads := make([]UploadResT, 0)

	uploadsRes = UploadsResT{
		Uploads: uploads,
	}
	err = uploadsReq.validateReq()
	if err != nil {
		return
	}
	uploadsRes.Pagination = UploadPagination{
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
		var upload UploadResT
		var nextRetryTimeStr sql.NullString
		var uploadError string
		var timingsObject sql.NullString
		var totalUploads int32
		err = rows.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.Namespace, &upload.Status, &uploadError, &upload.FirstEventAt, &upload.LastEventAt, &timingsObject, &nextRetryTimeStr, &totalUploads)
		if err != nil {
			uploadsReq.API.log.Errorf(err.Error())
			return UploadsResT{}, err
		}
		uploadsRes.Pagination.Total = totalUploads
		gjson.Parse(uploadError).ForEach(func(key gjson.Result, value gjson.Result) bool {
			upload.Attempt += int32(gjson.Get(value.String(), "attempt").Int())
			return true
		})
		_, firstTime := warehouseutils.GetFirstTiming(timingsObject)
		_, lastTime := warehouseutils.GetLastTiming(timingsObject)
		lastFailedStatus := warehouseutils.GetLastFailedStatus(timingsObject)
		errorPath := fmt.Sprintf("%s.errors", lastFailedStatus)
		errors := gjson.Get(uploadError, errorPath).Array()
		if len(errors) > 0 {
			upload.Error = errors[len(errors)-1].String()
		}
		upload.NextRetryTime = nextRetryTimeStr.String
		upload.Duration = int32(lastTime.Sub(firstTime) / time.Second)
		upload.Tables = make([]TableUploadResT, 0)
		uploads = append(uploads, upload)
	}

	if uploadsReq.IncludeTablesInRes {
		for index, upload := range uploads {
			tableUploadReq := TableUploadReqT{
				UploadID: upload.ID,
				Name:     "",
				API:      uploadsReq.API,
			}
			var tablesRes TablesResT
			tablesRes, err = tableUploadReq.GetWhTableUploads()
			if err != nil {
				return
			}
			uploads[index].Tables = tablesRes.Tables

		}
	}
	uploadsRes.Uploads = uploads

	return
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

func (uploadReq UploadReqT) GetWHUpload() (UploadResT, error) {
	err := uploadReq.validateReq()
	if err != nil {
		return UploadResT{}, err
	}
	query := uploadReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, status, error, first_event_at, last_event_at, timings->0, timings->-1, metadata->>'nextRetryTime'`)
	uploadReq.API.log.Debug(query)
	var upload UploadResT
	var firstTimingObject sql.NullString
	var lastTimingObject sql.NullString
	var nextRetryTimeStr sql.NullString
	row := uploadReq.API.dbHandle.QueryRow(query)
	err = row.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.Namespace, &upload.Status, &upload.Error, &upload.FirstEventAt, &upload.LastEventAt, &firstTimingObject, &lastTimingObject, &nextRetryTimeStr)
	if err != nil {
		uploadReq.API.log.Errorf(err.Error())
		return UploadResT{}, err
	}
	_, firstTime := warehouseutils.TimingFromJSONString(firstTimingObject)
	_, lastTime := warehouseutils.TimingFromJSONString(lastTimingObject)
	upload.NextRetryTime = nextRetryTimeStr.String
	upload.Duration = int32(lastTime.Sub(firstTime) / time.Second)
	tableUploadReq := TableUploadReqT{
		UploadID: upload.ID,
		Name:     "",
		API:      uploadReq.API,
	}
	tablesRes, err := tableUploadReq.GetWhTableUploads()
	if err != nil {
		return UploadResT{}, err
	}
	upload.Tables = tablesRes.Tables
	return upload, nil
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

func (tableUploadReq TableUploadReqT) GetWhTableUploads() (TablesResT, error) {
	err := tableUploadReq.validateReq()
	if err != nil {
		return TablesResT{}, err
	}
	query := tableUploadReq.generateQuery(`id, wh_upload_id, table_name, total_events, status, error, last_exec_time`)
	tableUploadReq.API.log.Debug(query)
	rows, err := tableUploadReq.API.dbHandle.Query(query)
	if err != nil {
		tableUploadReq.API.log.Errorf(err.Error())
		return TablesResT{}, err
	}
	var tableUploads []TableUploadResT
	for rows.Next() {
		var tableUpload TableUploadResT
		var count sql.NullInt32
		var lastExecTime sql.NullTime
		err = rows.Scan(&tableUpload.ID, &tableUpload.UploadID, &tableUpload.Name, &count, &tableUpload.Status, &tableUpload.Error, &lastExecTime)
		if err != nil {
			tableUploadReq.API.log.Errorf(err.Error())
			return TablesResT{}, err
		}
		if count.Valid {
			tableUpload.Count = count.Int32
		}
		if lastExecTime.Valid {
			tableUpload.LastExecAt = lastExecTime.Time
		}
		tableUploads = append(tableUploads, tableUpload)
	}
	return TablesResT{Tables: tableUploads}, nil
}
