package warehouse

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
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
}

type UploadsResT struct {
	Uploads []UploadResT `json:uploads`
}

type UploadResT struct {
	ID              int64                  `json:"id"`
	Namespace       string                 `json:"namespace"`
	SourceID        string                 `json:"source_id"`
	DestinationID   string                 `json:"destination_id"`
	DestinationType string                 `json:"destination_type"`
	Status          string                 `json:"status"`
	Schema          warehouseutils.SchemaT `json:"schema"`
	Error           json.RawMessage        `json:"error"`
	Timings         []map[string]string    `json:"timings"`
	Metadata        json.RawMessage        `json:"metadata"`
	FirstEventAt    time.Time              `json:"first_event_at"`
	LastEventAt     time.Time              `json:"last_event_at"`
	Tables          []TableUploadResT      `json:"tables;omitempty"`
}

type TableUploadReqT struct {
	UploadID int64
	Name     string
}

type TableUploadResT struct {
	ID         int64     `json: "id"`
	UploadID   int64     `json:"upload_id"`
	Name       string    `json:"name"`
	Error      string    `json:"error"`
	Status     string    `json:"status"`
	Count      int64     `json:"count"`
	LastExecAt time.Time `json:"lastExecAt"`
}

func (uploadsReq *UploadsReqT) validateUploadsReq() error {
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
	return query
}

func (uploadsReq UploadsReqT) GetWhUploads() (UploadsResT, error) {
	err := uploadsReq.validateUploadsReq()
	if err != nil {
		return UploadsResT{}, err
	}
	query := uploadsReq.generateQuery(`id, source_id, destination_id, destination_type, namespace, schema, status, error, first_event_at, last_event_at, timings`)
	pkgLogger.Debug(query)
	rows, err := dbHandle.Query(query)
	if err != nil {
		pkgLogger.Errorf(err.Error())
		return UploadsResT{}, err
	}
	uploads := make([]UploadResT, 0)
	for rows.Next() {
		var upload UploadResT
		var schema json.RawMessage
		var timings json.RawMessage
		err = rows.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.Namespace, &schema, &upload.Status, &upload.Error, &upload.FirstEventAt, &upload.LastEventAt, &timings)
		if err != nil {
			pkgLogger.Errorf(err.Error())
			return UploadsResT{}, err
		}
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
		upload.Timings = warehouseutils.JSONTimingsToMap(timings)
		upload.Tables = make([]TableUploadResT, 0)
		uploads = append(uploads, upload)
	}
	if uploadsReq.IncludeTablesInRes {
		for index, upload := range uploads {
			tableUploadReq := TableUploadReqT{
				UploadID: upload.ID,
				Name:     "",
			}
			uploads[index].Tables, err = tableUploadReq.GetWhTableUploads()

		}
	}
	return UploadsResT{
		Uploads: uploads,
	}, nil
}

func (tableUploadReq TableUploadReqT) generateQuery(selectFields string) string {
	query := fmt.Sprintf(`select %s from wh_table_uploads where wh_upload_id = %d`, selectFields, tableUploadReq.UploadID)
	if len(strings.TrimSpace(tableUploadReq.Name)) > 0 {
		query = fmt.Sprintf(`%s and table_name = %s`, query, tableUploadReq.Name)
	}
	return query
}

func (tableUploadReq TableUploadReqT) GetWhTableUploads() ([]TableUploadResT, error) {
	query := tableUploadReq.generateQuery(`id, wh_upload_id, table_name, total_events, status, error, last_exec_time`)
	pkgLogger.Debug(query)
	rows, err := dbHandle.Query(query)
	if err != nil {
		pkgLogger.Errorf(err.Error())
		return []TableUploadResT{}, err
	}
	var tableUploads []TableUploadResT
	for rows.Next() {
		var tableUpload TableUploadResT
		var count sql.NullInt64
		var lastExecTime sql.NullTime
		err = rows.Scan(&tableUpload.ID, &tableUpload.UploadID, &tableUpload.Name, &count, &tableUpload.Status, &tableUpload.Error, &lastExecTime)
		if err != nil {
			pkgLogger.Errorf(err.Error())
			return []TableUploadResT{}, err
		}
		if count.Valid {
			tableUpload.Count = count.Int64
		}
		if lastExecTime.Valid {
			tableUpload.LastExecAt = lastExecTime.Time
		}
		tableUploads = append(tableUploads, tableUpload)
	}
	return tableUploads, nil
}
