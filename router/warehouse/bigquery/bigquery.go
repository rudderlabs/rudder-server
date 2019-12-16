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

func (bq *HandleT) setUploadError(err error) {
	warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.ExportingDataFailedState, bq.DbHandle)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2 WHERE id=$3`, warehouseUploadsTable)
	_, err = bq.DbHandle.Exec(sqlStatement, warehouseutils.ExportingDataFailedState, err.Error(), bq.UploadID)
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
	if err != nil {
		return err
	}

	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY id) AS __row_number FROM ` + "`" + bq.ProjectID + "." + bq.SchemaName + "." + name + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
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

func (bq *HandleT) addColumns(tableName string, columns map[string]string) (err error) {
	logger.Debugf("BQ: Adding columns in table %s in bigquery dataset: %s in project: %s\n", tableName, bq.SchemaName, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.SchemaName).Table(tableName)
	meta, err := tableRef.Metadata(bq.BQContext)
	if err != nil {
		return err
	}
	newSchema := append(meta.Schema,
		getTableSchema(columns)...,
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

func (bq *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(bq.CurrentSchema, bq.UploadSchema)
	updatedSchema = diff.UpdatedSchema
	if len(bq.CurrentSchema) == 0 {
		err = bq.createSchema()
		if err != nil {
			return nil, err
		}
	}
	processedTables := make(map[string]bool)
	for _, tableName := range diff.Tables {
		err = bq.createTable(tableName, diff.ColumnMaps[tableName])
		if err != nil {
			return nil, err
		}
		processedTables[tableName] = true
	}
	for tableName, columnMap := range diff.ColumnMaps {
		if _, ok := processedTables[tableName]; ok {
			continue
		}
		if len(columnMap) > 0 {
			err := bq.addColumns(tableName, columnMap)
			if err != nil {
				logger.Errorf("BQ: Error creating columns in bigquery table %s: %+v\n", tableName, err)
			}
		}
	}
	return
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
		bq.setUploadError(err)
		return
	}
	err = warehouseutils.SetUploadStatus(bq.UploadID, warehouseutils.UpdatedSchemaState, bq.DbHandle)
	misc.AssertError(err)
	err = warehouseutils.UpdateCurrentSchema(bq.Warehouse, bq.UploadID, bq.CurrentSchema, updatedSchema, bq.DbHandle)
	if err != nil {
		bq.setUploadError(err)
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
		bq.setUploadError(err)
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
		bq.setUploadError(err)
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
