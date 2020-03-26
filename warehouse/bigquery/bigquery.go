package bigquery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

var (
	warehouseUploadsTable string
	maxParallelLoads      int
)

type HandleT struct {
	BQContext     context.Context
	DbHandle      *sql.DB
	Db            *bigquery.Client
	Namespace     string
	CurrentSchema map[string]map[string]string
	Warehouse     warehouseutils.WarehouseT
	ProjectID     string
	Upload        warehouseutils.UploadT
}

var dataTypesMap = map[string]bigquery.FieldType{
	"boolean":  bigquery.BooleanFieldType,
	"int":      bigquery.NumericFieldType,
	"float":    bigquery.FloatFieldType,
	"string":   bigquery.StringFieldType,
	"datetime": bigquery.TimestampFieldType,
}

var primaryKeyMap = map[string]string{
	"identifies": "user_id",
}

func (bq *HandleT) setUploadError(err error, state string) {
	warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportingDataFailedState, bq.DbHandle)
	sqlStatement := fmt.Sprintf(`UPDATE %s SET status=$1, error=$2, updated_at=$3 WHERE id=$4`, warehouseUploadsTable)
	_, err = bq.DbHandle.Exec(sqlStatement, state, err.Error(), time.Now(), bq.Upload.ID)
	if err != nil {
		panic(err)
	}
}

func getTableSchema(columns map[string]string) []*bigquery.FieldSchema {
	var schema []*bigquery.FieldSchema
	for columnName, columnType := range columns {
		schema = append(schema, &bigquery.FieldSchema{Name: columnName, Type: dataTypesMap[columnType]})
	}
	return schema
}

func (bq *HandleT) createTable(name string, columns map[string]string) (err error) {
	logger.Infof("BQ: Creating table: %s in bigquery dataset: %s in project: %s", name, bq.Namespace, bq.ProjectID)
	sampleSchema := getTableSchema(columns)
	metaData := &bigquery.TableMetadata{
		Schema:           sampleSchema,
		TimePartitioning: &bigquery.TimePartitioning{Expiration: time.Duration(24*60) * time.Hour},
	}
	tableRef := bq.Db.Dataset(bq.Namespace).Table(name)
	err = tableRef.Create(bq.BQContext, metaData)
	if !checkAndIgnoreAlreadyExistError(err) {
		return err
	}

	primaryKey := "id"
	if column, ok := primaryKeyMap[name]; ok {
		primaryKey = column
	}

	// assuming it has field named id upon which dedup is done in view
	viewQuery := `SELECT * EXCEPT (__row_number) FROM (
			SELECT *, ROW_NUMBER() OVER (PARTITION BY ` + primaryKey + `) AS __row_number FROM ` + "`" + bq.ProjectID + "." + bq.Namespace + "." + name + "`" + ` WHERE _PARTITIONTIME BETWEEN TIMESTAMP_TRUNC(TIMESTAMP_MICROS(UNIX_MICROS(CURRENT_TIMESTAMP()) - 60 * 60 * 60 * 24 * 1000000), DAY, 'UTC')
					AND TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'UTC')
			)
		WHERE __row_number = 1`
	metaData = &bigquery.TableMetadata{
		ViewQuery: viewQuery,
	}
	tableRef = bq.Db.Dataset(bq.Namespace).Table(name + "_view")
	err = tableRef.Create(bq.BQContext, metaData)
	return
}

func (bq *HandleT) addColumn(tableName string, columnName string, columnType string) (err error) {
	logger.Infof("BQ: Adding columns in table %s in bigquery dataset: %s in project: %s", tableName, bq.Namespace, bq.ProjectID)
	tableRef := bq.Db.Dataset(bq.Namespace).Table(tableName)
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
	logger.Infof("BQ: Creating bigquery dataset: %s in project: %s", bq.Namespace, bq.ProjectID)
	ds := bq.Db.Dataset(bq.Namespace)
	err = ds.Create(bq.BQContext, nil)
	return
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 409 || e.Code == 400 {
				logger.Debugf("BQ: Google API returned error with code: %v", e.Code)
				return true
			}
		}
		return false
	}
	return true
}

func (bq *HandleT) updateSchema() (updatedSchema map[string]map[string]string, err error) {
	diff := warehouseutils.GetSchemaDiff(bq.CurrentSchema, bq.Upload.Schema)
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

func (bq *HandleT) loadTable(tableName string) (err error) {
	uploadStatus, err := warehouseutils.GetTableUploadStatus(bq.Upload.ID, tableName, bq.DbHandle)
	if uploadStatus == warehouseutils.ExportedDataState {
		logger.Infof("BQ: Skipping load for table:%s as it has been succesfully loaded earlier", tableName)
		return
	}

	logger.Infof("BQ: Starting load for table:%s\n", tableName)
	warehouseutils.SetTableUploadStatus(warehouseutils.ExecutingState, bq.Upload.ID, tableName, bq.DbHandle)

	locations, err := warehouseutils.GetLoadFileLocations(bq.DbHandle, bq.Warehouse.Source.ID, bq.Warehouse.Destination.ID, tableName, bq.Upload.StartLoadFileID, bq.Upload.EndLoadFileID)
	if err != nil {
		panic(err)
	}
	locations, err = warehouseutils.GetGCSLocations(locations)
	logger.Infof("BQ: Loading data into table: %s in bigquery dataset: %s in project: %s from %v", tableName, bq.Namespace, bq.ProjectID, locations)
	gcsRef := bigquery.NewGCSReference(locations...)
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.MaxBadRecords = 100
	gcsRef.IgnoreUnknownValues = true
	// create partitioned table in format tableName$20191221
	loader := bq.Db.Dataset(bq.Namespace).Table(fmt.Sprintf(`%s$%v`, tableName, strings.ReplaceAll(time.Now().Format("2006-01-02"), "-", ""))).LoaderFrom(gcsRef)

	job, err := loader.Run(bq.BQContext)
	if err != nil {
		logger.Errorf("BQ: Error initiating load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}
	status, err := job.Wait(bq.BQContext)
	if err != nil {
		logger.Errorf("BQ: Error running load job: %v\n", err)
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}

	if status.Err() != nil {
		warehouseutils.SetTableUploadError(warehouseutils.ExportingDataFailedState, bq.Upload.ID, tableName, err, bq.DbHandle)
		return
	}
	warehouseutils.SetTableUploadStatus(warehouseutils.ExportedDataState, bq.Upload.ID, tableName, bq.DbHandle)
	return
}

func (bq *HandleT) load() (errList []error) {
	logger.Infof("BQ: Starting load for all %v tables\n", len(bq.Upload.Schema))
	var wg sync.WaitGroup
	wg.Add(len(bq.Upload.Schema))
	loadChan := make(chan struct{}, maxParallelLoads)
	for tableName := range bq.Upload.Schema {
		tName := tableName
		loadChan <- struct{}{}
		rruntime.Go(func() {
			err := bq.loadTable(tName)
			if err != nil {
				errList = append(errList, err)
			}
			wg.Done()
			<-loadChan
		})
	}
	wg.Wait()
	logger.Infof("BQ: Completed load for all tables\n")
	return
}

type BQCredentialsT struct {
	projectID   string
	credentials string
}

func (bq *HandleT) connect(cred BQCredentialsT) (*bigquery.Client, error) {
	logger.Infof("BQ: Connecting to BigQuery in project: %s", cred.projectID)
	bq.BQContext = context.Background()
	client, err := bigquery.NewClient(bq.BQContext, cred.projectID, option.WithCredentialsJSON([]byte(cred.credentials)))
	return client, err
}

func loadConfig() {
	warehouseUploadsTable = config.GetString("Warehouse.uploadsTable", "wh_uploads")
	maxParallelLoads = config.GetInt("Warehose.bigquery.maxParallelLoads", 20)
}

func init() {
	config.Initialize()
	loadConfig()
}

func (bq *HandleT) MigrateSchema() (err error) {
	timer := warehouseutils.DestStat(stats.TimerType, "migrate_schema_time", bq.Warehouse.Destination.ID)
	timer.Start()
	warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.UpdatingSchemaState, bq.DbHandle)
	logger.Infof("BQ: Updaing schema for bigquery in project: %s", bq.ProjectID)
	updatedSchema, err := bq.updateSchema()
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return
	}
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.UpdatedSchemaState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	err = warehouseutils.UpdateCurrentSchema(bq.Namespace, bq.Warehouse, bq.Upload.ID, bq.CurrentSchema, updatedSchema, bq.DbHandle)
	timer.End()
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return
	}
	return
}

func (bq *HandleT) Export() (err error) {
	logger.Infof("BQ: Starting export to Bigquery for source:%s and wh_upload:%v", bq.Warehouse.Source.ID, bq.Upload.ID)
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportingDataState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	timer := warehouseutils.DestStat(stats.TimerType, "upload_time", bq.Warehouse.Destination.ID)
	timer.Start()
	errList := bq.load()
	timer.End()
	if len(errList) > 0 {
		errStr := ""
		for idx, err := range errList {
			errStr += err.Error()
			if idx < len(errList)-1 {
				errStr += ", "
			}
		}
		warehouseutils.SetUploadError(bq.Upload, errors.New(errStr), warehouseutils.ExportingDataFailedState, bq.DbHandle)
		return errors.New(errStr)
	}
	err = warehouseutils.SetUploadStatus(bq.Upload, warehouseutils.ExportedDataState, bq.DbHandle)
	if err != nil {
		panic(err)
	}
	return
}

func (bq *HandleT) CrashRecover(config warehouseutils.ConfigT) (err error) {
	return
}

func (bq *HandleT) Process(config warehouseutils.ConfigT) (err error) {
	bq.DbHandle = config.DbHandle
	bq.Warehouse = config.Warehouse
	bq.Upload = config.Upload
	bq.ProjectID = strings.TrimSpace(bq.Warehouse.Destination.Config.(map[string]interface{})["project"].(string))

	bq.Db, err = bq.connect(BQCredentialsT{
		projectID:   bq.ProjectID,
		credentials: bq.Warehouse.Destination.Config.(map[string]interface{})["credentials"].(string),
	})
	if err != nil {
		warehouseutils.SetUploadError(bq.Upload, err, warehouseutils.UpdatingSchemaFailedState, bq.DbHandle)
		return err
	}
	currSchema, err := warehouseutils.GetCurrentSchema(bq.DbHandle, bq.Warehouse)
	if err != nil {
		panic(err)
	}
	bq.CurrentSchema = currSchema.Schema
	bq.Namespace = currSchema.Namespace
	if bq.Namespace == "" {
		bq.Namespace = bq.Upload.Namespace
	}

	if config.Stage == "ExportData" {
		err = bq.Export()
	} else {
		err = bq.MigrateSchema()
		if err == nil {
			err = bq.Export()
		}
	}
	return
}
