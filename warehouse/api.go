package warehouse

import (
	"encoding/json"
	"fmt"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WHUploadReqT struct {
	SourceID           string
	DestinationID      string
	DestinationType    string
	Status             string
	IncludeTablesInRes bool
	PageNo             int
	PageSize           int
}

type WHUploadResT struct {
	uploads []UploadT
}

func SetSourceID(id string) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.SourceID = id
	}
}

func SetDestinationID(id string) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.DestinationID = id
	}
}

func SetDestinationType(destType string) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.DestinationType = destType
	}
}

func SetStatus(status string) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.Status = status
	}
}

func SetIncludeTablesInRes(val bool) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.IncludeTablesInRes = val
	}
}

func SetPageNo(val int) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.PageNo = val
	}
}

func SetPageSize(val int) WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.PageSize = val
	}
}
func SetDefault() WHUploadOption {
	return func(upload *WHUploadReqT) {
		upload.PageSize = 10
		upload.PageNo = 0
	}
}

type WHTableUpload struct {
}

type UploadOptions struct {
}

func (upload *WHUploadReqT) validateUploadReq() error {
	return nil
}

func (upload *WHUploadReqT) generateQuery(selectFields string) string {
	whereClauseAdded := false
	query := `select` + selectFields + ` from  wh_uploads`
	if upload.SourceID != "" {
		whereClauseAdded = true
		query = query + `where source_id = ` + upload.SourceID
	}
	if upload.DestinationID != "" {
		if whereClauseAdded {
			query = query + `and destination_id = ` + upload.DestinationID
		} else {
			query = query + `where destination_id = ` + upload.DestinationID
		}
	}
	if upload.DestinationType != "" {
		if whereClauseAdded {
			query = query + `and destination_type = ` + upload.DestinationType
		} else {
			query = query + `where destination_type = ` + upload.DestinationType
		}
	}
	if upload.DestinationType != "" {
		if whereClauseAdded {
			query = query + `and destination_type = ` + upload.DestinationType
		} else {
			query = query + `where destination_type = ` + upload.DestinationType
		}
	}
	if upload.Status != "" {
		if whereClauseAdded {
			query = query + `and status = ` + upload.Status
		} else {
			query = query + `where status = ` + upload.Status
		}
	}
	query = query + fmt.Sprintf(` order by id desc limit %d offset %d`, upload.PageSize, upload.PageSize*upload.PageNo)
	fmt.Println(query)
	return query
}

type WHUploadOption func(upload *WHUploadReqT)

func GetWhUploads(options ...WHUploadOption) (WHUploadResT, error) {
	uploadsReq := &WHUploadReqT{}
	for _, option := range options {
		option(uploadsReq)
	}
	uploadsReq.validateUploadReq()
	query := uploadsReq.generateQuery(` id, source_id, destination_id, destination_type, namespace, schema, status, error, first_event_at, last_event_at, timings`)
	pkgLogger.Debug(query)
	rows, err := dbHandle.Query(query)
	if err != nil {
		pkgLogger.Errorf(err.Error())
		return WHUploadResT{}, err
	}
	var uploads []UploadT
	for rows.Next() {
		var upload UploadT
		var schema json.RawMessage
		var timings json.RawMessage
		err = rows.Scan(&upload.ID, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.Namespace, &schema, &upload.Status, &upload.Error, &upload.FirstEventAt, &upload.LastEventAt, &timings)
		upload.Schema = warehouseutils.JSONSchemaToMap(schema)
		upload.Timings = warehouseutils.JSONTimingsToMap(timings)
		if err != nil {
			pkgLogger.Errorf(err.Error())
		}
		uploads = append(uploads, upload)
	}
	return WHUploadResT{
		uploads: uploads,
	}, nil
}

func GetWhTableUploads() {

}
