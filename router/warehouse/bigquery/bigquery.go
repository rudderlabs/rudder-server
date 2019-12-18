package bigquery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/iancoleman/strcase"
	"github.com/rudderlabs/rudder-server/config"
	warehouseutils "github.com/rudderlabs/rudder-server/router/warehouse/utils"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var (
	warehouseUploadsTable string
)

type HandleT struct {
	BQContext     context.Context
	DbHandle      *sql.DB
	Db            *bigquery.Client
	UploadSchema  map[string]map[string]string
	CurrentSchema map[string]map[string]string
	UploadID      int64
	Warehouse     warehouseutils.WarehouseT
	SchemaName    string
	StartCSVID    int64
	EndCSVID      int64
	ProjectID     string
}

var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.IntegerFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

var primaryKeyMap = map[string]string{
	"identifies": "user_id",
}

func (bq *HandleT) setUploadError(err error, state string) {
	warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.ExportingDataFailedState, bq.DbHandle)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2 WHERE id=$3`, warehouseUploadsTable)
	_, err = bq.DbHandle.Exec(sqlStatement, state, err.Error(), bq.UploadID)
	misc.AssertError(err)
}

func getTableSchema(columns map[string]string) []*bigquery.FieldSchema {
	var schema []*bigquery.FieldSchema
	for columnName, columnType := range columns {
		schema = append(schema, &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]})
	}
	return schema
}

func (bq *HandleT) createTable(name string, columns map[string]string) (err error) {
	logger.Debugf("BQ: Creating table: %s in bigquery dataset: %s in project: %s\n", name, bq.SchemaName, bq.ProjectID)
	sampleSchema := getTableSchema(columns)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{Expiration: time.Duration(24*60) * time.Hour},
	}
	tableRef := bq.Db.Dataset(bq.SchemaName).Table(name)
	err = tableRef.Create(bq.BQContext, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		fmt.Printf("%+v\n", err)
		return err
	}

	primaryKey := "id"
	if column, ok := primaryKeyMap[name]; ok {
		primaryKey = column
	}

	// assuming it has field named id upon which dedup is done in view
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + primaryKey + `) AS __row_number FROM ` + "`" + bq.ProjectID + "." + bq.SchemaName + "." + name + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
					AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
			)
		WHERE __row_number = 1`
	metaData = &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	tableRef = bq.Db.Dataset(bq.SchemaName).Table(name + "_view")
	err = tableRef.Create(bq.BQContext, metaData)
	return
}

func (bq *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	logger.Debugf("BQ: Adding columns in table %s in bigquery dataset: %s in project: %s\n", tableName, bq.SchemaName, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.SchemaName).Table(tableName)
	meta, err := tableRef.Metadata(bq.BQContext)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		&bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]},
	)
	update := bigquery.TableMetadataToUpdate{
		Schema: newSchema,
	}
	_, err = tableRef.Update(bq.BQContext, update, meta.ETag)
	return
}

func (bq *HandleT) createSchema() (err error) {
	logger.Debugf("BQ: Creating bigquery dataset: %s in project: %s\n", bq.SchemaName, bq.ProjectID)
	ds := bq.Db.Dataset(bq.SchemaName)
	err = ds.Create(bq.BQContext, nil)
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 409 || e.Code == 400 {
				return true
			}
		}
		return false
	}
	return true
}

func (bq *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(bq.CurrentSchema, bq.UploadSchema)
	updatedSchema = diff.UpdatedSchema
	if len(bq.CurrentSchema) == 0 {
		err = bq.createSchema()
		if !checkAndIgnoreAlreadyExistError(err) {
			return nil, err
		}
	}
	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		err = bq.createTable(tableName, diff.ColumnMaps[tableName])
		if !checkAndIgnoreAlreadyExistError(err) {
			return nil, err
		}
		processedTables[tableName] = true
	}
	for tableName, columnMap := range diff.ColumnMaps {
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			var err error
			for columnName, columnType := range columnMap {
				err = bq.addColumn(tableName, columnName, columnType)
				if !checkAndIgnoreAlreadyExistError(err) {
					return nil, err
				}
			}
		}
	}
	return updatedSchema, nil
}

func (bq *HandleT) load(schema map[string]map[string]string) (err error) {
	for tableName := range schema {
		locations, err := warehouseutils.GetCSVLocations(bq.DbHandle, tableName, bq.StartCSVID, bq.EndCSVID)
		misc.AssertError(err)
		locations, err = warehouseutils.GetGCSLocations(locations)
		logger.Debugf("Loading data into table: %s in bigquery dataset: %s in project: %s from %v\n", tableName, bq.SchemaName, bq.ProjectID, locations)
		gcsRef := bigquery.NewGCSReference(locations...)
		gcsRef.SourceFormat = bigquery.JSON
		gcsRef.MaxBadRecords = 100
		gcsRef.IgnoreUnknownValues = true
		// create partitioned table in format tableName$20191221
		loader := bq.Db.Dataset(bq.SchemaName).Table(fmt.Sprintf(`%s$%v`, tableName, strings.ReplaceAll(time.Now().Format("2006-01-02"), "-", ""))).LoaderFrom(gcsRef)

		job, err := loader.Run(bq.BQContext)
		if err != nil {
			return err
		}
		status, err := job.Wait(bq.BQContext)
		if err != nil {
			return err
		}

		if status.Err() != nil {
			misc.AssertError(status.Err())
			return fmt.Errorf("job completed with error: %v", status.Err())
		}
	}
	return
}

type BQCredentialsT struct {
	projectID   string
	credentials string
}

func (bq *HandleT) connect(cred BQCredentialsT) (*bigquery.Client, error) {
	logger.Debugf("BQ: Connecting to BigQuery in project: %s\n", cred.projectID)
	bq.BQContext = context.Background()
	client, err := bigquery.NewClient(bq.BQContext, cred.projectID, option.WithCredentialsJSON([]byte(cred.credentials)))
	return client, err
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
}

func init() {
	config.Initialize()
	loadConfig()
}

func (bq *HandleT) MigrateSchema() (err error) {
	warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.UpdatingSchemaState, bq.DbHandle)
	logger.Debugf("BQ: Updaing schema for bigquery in project: %s\n", bq.ProjectID)
	updatedSchema, err := bq.updateSchema()
	if err != nil {
		bq.setUploadError(err, warehouseutils.UpdatingSchemaFailedState)
		return
	}
	err = warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.UpdatedSchemaState, bq.DbHandle)
	misc.AssertError(err)
	err = warehouseutils.UpdateCurrentSchema(bq.Warehouse, bq.UploadID, bq.CurrentSchema, updatedSchema, bq.DbHandle)
	if err != nil {
		bq.setUploadError(err, warehouseutils.UpdatingSchemaFailedState)
		return
	}
	return
}

func (bq *HandleT) Export() {
	logger.Debugf("BQ: Starting export to bigquery: ")
	err := warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.ExportingDataState, bq.DbHandle)
	misc.AssertError(err)
	err = bq.load(bq.UploadSchema)
	if err != nil {
		bq.setUploadError(err, warehouseutils.ExportingDataFailedState)
		return
	}
	err = warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.ExportedDataState, bq.DbHandle)
	misc.AssertError(err)
}

func (bq *HandleT) Process(config warehouseutils.ConfigT) {
	var err error
	bq.DbHandle = config.DbHandle
	bq.UploadID = config.UploadID
	bq.UploadSchema = config.Schema
	bq.Warehouse = config.Warehouse
	bq.StartCSVID = config.StartCSVID
	bq.EndCSVID = config.EndCSVID
	bq.ProjectID = strings.TrimSpace(bq.Warehouse.Destination.Config.(map[string]interface{})["project"].(string))

	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: bq.Warehouse.Destination.Config.(map[string]interface{})["credentials"].(string),
	})
	if err != nil {
		bq.setUploadError(err, warehouseutils.UpdatingSchemaFailedState)
		return
	}
	bq.CurrentSchema, err = warehouseutils.GetCurrentSchema(bq.DbHandle, bq.Warehouse)
	misc.AssertError(err)
	bq.SchemaName = strings.ToLower(strcase.ToSnake(bq.Warehouse.Source.Name))

	if config.Stage == "ExportData" {
		bq.Export()
	} else {
		err := bq.MigrateSchema()
		if err == nil {
			bq.Export()
		}
	}
}
