package warehouseutils

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rudderlabs/rudder-server/config"
	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// constants for wh_upload_state_type
const (
	GeneratingCsvState        = "generating_csv"
	GeneratingCsvFailedState  = "generating_csv_failed"
	GeneratedCsvState         = "generated_csv"
	UpdatingSchemaState       = "updating_schema"
	UpdatingSchemaFailedState = "updating_schema_failed"
	UpdatedSchemaState        = "updated_schema"
	ExportingDataState        = "exporting_data"
	ExportingDataFailedState  = "exporting_data_failed"
	ExportedDataState         = "exported_data"
)

var (
	warehouseUploadsTable    string
	warehouseSchemasTable    string
	warehouseCSVUploadsTable string
)

func init() {
	config.Initialize()
	loadConfig()
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("BatchRouter.warehouseUploadsTable", "wh_uploads")
	warehouseSchemasTable = config.GetString("BatchRouter.warehouseSchemasTable", "wh_schemas")
	warehouseCSVUploadsTable = config.GetString("BatchRouter.warehouseCSVUploadsTable", "wh_schemas")
}

// WarehouseT ...
type WarehouseT struct {
	Source      backendconfig.SourceT
	Destination backendconfig.DestinationT
}

type ConfigT struct {
	DbHandle   *sql.DB
	UploadID   int64
	StartCSVID int64
	EndCSVID   int64
	Schema     map[string]map[string]string
	Warehouse  WarehouseT
}

// GetCurrentSchema ...
func GetCurrentSchema(dbHandle *sql.DB, warehouse WarehouseT) (map[string]map[string]string, error) {
	var rawSchema json.RawMessage
	sqlStatement := fmt.Sprintf(`SELECT schema FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s') ORDER BY %[1]s.id DESC`, warehouseSchemasTable, warehouse.Source.ID, warehouse.Destination.ID)
	err := dbHandle.QueryRow(sqlStatement).Scan(&rawSchema)
	if err != nil {
		if err == sql.ErrNoRows {
			return make(map[string]map[string]string), nil
		}
		misc.AssertError(err)
	}
	var schemaMapInterface map[string]interface{}
	err = json.Unmarshal(rawSchema, &schemaMapInterface)
	misc.AssertError(err)
	schema := make(map[string]map[string]string)
	for key, val := range schemaMapInterface {
		y := make(map[string]string)
		x := val.(map[string]interface{})
		for k, v := range x {
			y[k] = v.(string)
		}
		schema[key] = y
	}
	return schema, nil
}

// SchemaDiffT ...
type SchemaDiffT struct {
	Tables        []string
	ColumnMaps    map[string]map[string]string
	UpdatedSchema map[string]map[string]string
}

// GetSchemaDiff ...
func GetSchemaDiff(currentSchema, uploadSchema map[string]map[string]string) (diff SchemaDiffT) {
	diff = SchemaDiffT{
		Tables:        []string{},
		ColumnMaps:    make(map[string]map[string]string),
		UpdatedSchema: make(map[string]map[string]string),
	}
	for tableName, uploadColumnMap := range uploadSchema {
		currentColumnsMap, ok := currentSchema[tableName]
		if !ok {
			diff.Tables = append(diff.Tables, tableName)
			diff.ColumnMaps[tableName] = uploadColumnMap
			diff.UpdatedSchema[tableName] = uploadColumnMap
		} else {
			diff.UpdatedSchema[tableName] = currentSchema[tableName]
			diff.ColumnMaps[tableName] = make(map[string]string)
			for columnName, columnVal := range uploadColumnMap {
				if _, ok := currentColumnsMap[columnName]; !ok {
					diff.ColumnMaps[tableName][columnName] = columnVal
					diff.UpdatedSchema[tableName][columnName] = columnVal
				}
			}
		}
	}
	return
}

// SetUploadStatus ...
func SetUploadStatus(id int64, status string, dbHandle *sql.DB) (err error) {
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1 WHERE id=$2`, warehouseUploadsTable)
	_, err = dbHandle.Exec(sqlStatement, status, id)
	misc.AssertError(err)
	return
}

// UpdateCurrentSchema ...
func UpdateCurrentSchema(wh WarehouseT, uploadID int64, currentSchema, schema map[string]map[string]string, dbHandle *sql.DB) (err error) {
	marshalledSchema, err := json.Marshal(schema)
	if len(currentSchema) == 0 {
		sqlStatement := fmt.Sprintf(`INSERT INTO %s (wh_upload_id, source_id, source_schema_name, destination_id, destination_type, schema, created_at)
									   VALUES ($1, $2, $3, $4, $5, $6, $7)`, warehouseSchemasTable)
		stmt, err := dbHandle.Prepare(sqlStatement)
		misc.AssertError(err)
		defer stmt.Close()

		_, err = stmt.Exec(uploadID, wh.Source.ID, strings.ToLower(wh.Source.Name), wh.Destination.ID, wh.Destination.DestinationDefinition.Name, marshalledSchema, time.Now())
	} else {
		sqlStatement := fmt.Sprintf(`UPDATE %s SET schema=$1 WHERE source_id=$2 AND destination_id=$3`, warehouseSchemasTable)
		_, err = dbHandle.Exec(sqlStatement, marshalledSchema, wh.Source.ID, wh.Destination.ID)
	}
	misc.AssertError(err)
	return
}

// GetCSVLocations ...
func GetCSVLocations(dbHandle *sql.DB, tableName string, start, end int64) (locations []string, err error) {
	sqlStatement := fmt.Sprintf(`SELECT location FROM %[1]s WHERE (%[1]s.table_name='%[2]s' AND %[1]s.id > %[3]v AND %[1]s.id <= %[4]v)`, warehouseCSVUploadsTable, tableName, start, end)
	fmt.Println(sqlStatement)
	rows, err := dbHandle.Query(sqlStatement)
	defer rows.Close()
	misc.AssertError(err)

	for rows.Next() {
		var location string
		err := rows.Scan(&location)
		misc.AssertError(err)
		locations = append(locations, location)
	}

	fmt.Println("^^^")
	fmt.Println(locations)
	fmt.Println("^^^")

	return
}

func GetS3Location(location string) string {
	str1 := strings.Replace(location, "https", "s3", 1)
	str2 := strings.Replace(str1, ".s3.amazonaws.com", "", 1)
	return str2
}

// GetS3Locations ...
func GetS3Locations(locations []string) (s3Locations []string, err error) {
	for _, location := range locations {
		s3Locations = append(s3Locations, GetS3Location((location)))
	}
	return
}
