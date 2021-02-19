package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/controlplane"
	proto "github.com/rudderlabs/rudder-server/proto/warehouse"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/grpc"
	"strings"
	"time"
)

type UploadsReqT struct {
	SourceID           string
	DestinationID      string
	DestinationType    string
	Status             string
	IncludeTablesInRes bool
	From               time.Time
	To                 time.Time
	API                UploadAPIT
}

type UploadsResT struct {
	Uploads []UploadResT `json:"uploads,omitempty"`
}

type UploadResT struct {
	ID              int64             `json:"id"`
	Namespace       string            `json:"namespace"`
	SourceID        string            `json:"source_id"`
	DestinationID   string            `json:"destination_id"`
	DestinationType string            `json:"destination_type"`
	Status          string            `json:"status"`
	Schema          string            `json:"schema"`
	Error           string            `json:"error"`
	Timings         string            `json:"timings"`
	Metadata        string            `json:"metadata"`
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
	Count      int64     `json:"count"`
	LastExecAt time.Time `json:"last_exec_at"`
}

type UploadAPIT struct {
	enabled           bool
	dbHandle          *sql.DB
	log               logger.LoggerI
	connectionManager *controlplane.ConnectionManager
}

var UploadAPI UploadAPIT

func InitWarehouseApis(dbHandle *sql.DB, log logger.LoggerI) {
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
	fmt.Println(uploadsReq)
	if !uploadsReq.API.enabled || uploadsReq.API.log == nil || uploadsReq.API.dbHandle == nil {
		return errors.New(fmt.Sprint(`warehouse api's are not initialized`))
	}
	if uploadsReq.From.IsZero() {
		uploadsReq.From = time.Now().UTC()
	}
	if uploadsReq.To.IsZero() {
		uploadsReq.To = uploadsReq.From.Add(-24 * time.Hour)
	}
	if uploadsReq.From.Before(uploadsReq.To) {
		return errors.New(fmt.Sprintf(`from should be greater than to. from: %s, to: %s`, uploadsReq.From, uploadsReq.To))
	}
	return nil
}

func (uploadsReq *UploadsReqT) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`select %s  from %s where created_at <= '%s' and created_at >= '%s'`, selectFields, warehouseutils.WarehouseUploadsTable, uploadsReq.From.Format(time.RFC3339), uploadsReq.To.Format(time.RFC3339))
	if uploadsReq.SourceID != "" {
		query = fmt.Sprintf(`%s and source_id = '%s' `, query, uploadsReq.SourceID)
	}
	if uploadsReq.DestinationID != "" {
		query = fmt.Sprintf(`%s and destination_id = '%s' `, query, uploadsReq.DestinationID)
	}
	if uploadsReq.DestinationType != "" {
		query = fmt.Sprintf(`%s and destination_type = '%s' `, query, uploadsReq.DestinationType)
	}
	if uploadsReq.Status != "" {
		query = fmt.Sprintf(`%s and status = '%s' `, query, uploadsReq.Status)
	}
	query = query + ` order by id desc`
	uploadsReq.API.log.Info(query)
	return query
}

func (uploadsReq UploadsReqT) GetWhUploads() (UploadsResT, error) {
	err := uploadsReq.validateReq()
	if err != nil {
		return UploadsResT{}, err
	}
	query := uploadsReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, schema, status, error, first_event_at, last_event_at, timings, metadata`)
	uploadsReq.API.log.Debug(query)
	rows, err := uploadsReq.API.dbHandle.Query(query)
	if err != nil {
		uploadsReq.API.log.Errorf(err.Error())
		return UploadsResT{}, err
	}
	uploads := make([]UploadResT, 0)
	for rows.Next() {
		var upload UploadResT
		var schema string
		var timings json.RawMessage
		var metadata json.RawMessage
		err = rows.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.Namespace, &schema, &upload.Status, &upload.Error, &upload.FirstEventAt, &upload.LastEventAt, &timings, &metadata)
		if err != nil {
			uploadsReq.API.log.Errorf(err.Error())
			return UploadsResT{}, err
		}
		upload.Schema = string(schema)
		upload.Timings = string(timings)
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
			tablesRes, err := tableUploadReq.GetWhTableUploads()
			if err != nil {
				return UploadsResT{}, err
			}
			uploads[index].Tables = tablesRes.Tables

		}
	}
	return UploadsResT{
		Uploads: uploads,
	}, nil
}

func (tableUploadReq TableUploadReqT) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`select %s from %s where wh_upload_id = %d`, selectFields, warehouseutils.WarehouseTableUploadsTable, tableUploadReq.UploadID)
	if len(strings.TrimSpace(tableUploadReq.Name)) > 0 {
		query = fmt.Sprintf(`%s and table_name = %s`, query, tableUploadReq.Name)
	}
	return query
}

func (tableUploadReq TableUploadReqT) validateReq() error {
	fmt.Println(tableUploadReq)
	if !tableUploadReq.API.enabled || tableUploadReq.API.log == nil || tableUploadReq.API.dbHandle == nil {
		return errors.New(fmt.Sprint(`warehouse api's are not initialized`))
	}
	if tableUploadReq.UploadID == 0 {
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
		var count sql.NullInt64
		var lastExecTime sql.NullTime
		err = rows.Scan(&tableUpload.ID, &tableUpload.UploadID, &tableUpload.Name, &count, &tableUpload.Status, &tableUpload.Error, &lastExecTime)
		if err != nil {
			tableUploadReq.API.log.Errorf(err.Error())
			return TablesResT{}, err
		}
		if count.Valid {
			tableUpload.Count = count.Int64
		}
		if lastExecTime.Valid {
			tableUpload.LastExecAt = lastExecTime.Time
		}
		tableUploads = append(tableUploads, tableUpload)
	}
	return TablesResT{Tables: tableUploads}, nil
}
