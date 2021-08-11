package s3datalake

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/client"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const (
	AWSAccessKey        = "accessKey"
	AWSAccessKeyID      = "accessKeyID"
	AWSBucketNameConfig = "bucketName"
	AWSRegion           = "region"
	GlueDatabase        = "database"
)

var (
	pkgLogger logger.LoggerI
)

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("s3-datalake")
}

type HandleT struct {
	glueClient *glue.Glue
	Warehouse  warehouseutils.WarehouseT
	Uploader   warehouseutils.UploaderI
	Database   string
}

func (wh *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error {
	wh.Warehouse = warehouse
	wh.Uploader = uploader

	cl, err := wh.SgetGlueClient()
	if err != nil {
		return err
	}
	wh.glueClient = cl

	wh.Database = warehouseutils.GetConfigValue(GlueDatabase, wh.Warehouse)

	return nil
}

func (wh *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return nil
}

func (wh *HandleT) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	var schema = warehouseutils.SchemaT{}
	wh.Warehouse = warehouse

	glueClient, err := wh.SgetGlueClient()
	if err != nil {
		return nil, err
	}

	database := warehouseutils.GetConfigValue(GlueDatabase, wh.Warehouse)

	var getTablesOutput *glue.GetTablesOutput
	var getTablesInput *glue.GetTablesInput
	for true {
		getTablesInput = &glue.GetTablesInput{DatabaseName: &database}

		if getTablesOutput != nil && getTablesOutput.NextToken != nil {
			// add nextToken to the request if there are multiple list segments
			getTablesInput.NextToken = getTablesOutput.NextToken
		}

		getTablesOutput, err = glueClient.GetTables(getTablesInput)
		if err != nil {
			return schema, err
		}

		for _, table := range getTablesOutput.TableList {
			if table.Name != nil && table.StorageDescriptor != nil && table.StorageDescriptor.Columns != nil {
				tableName := *table.Name
				if _, ok := schema[tableName]; !ok {
					schema[tableName] = map[string]string{}
				}

				for _, col := range table.StorageDescriptor.Columns {
					schema[tableName][*col.Name] = *col.Type
				}
			}
		}

		if getTablesOutput.NextToken == nil {
			// break out of the loop if there are no more list segments
			break
		}
	}

	return schema, err
}

func (wh *HandleT) CreateSchema() (err error) {
	_, err = wh.glueClient.CreateDatabase(&glue.CreateDatabaseInput{
		DatabaseInput: &glue.DatabaseInput{
			Name: &wh.Database,
		},
	})
	return
}

func (wh *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// td: assign table owner as rudderstack?
	// td: add location too when load file name is finalized.
	// create table request
	input := glue.CreateTableInput{
		DatabaseName: aws.String(wh.Database),
		TableInput: &glue.TableInput{
			Name: aws.String(tableName),
		},
	}

	// add columns to request
	storageDescriptor := glue.StorageDescriptor{
		Columns: []*glue.Column{},
	}
	for colName, colType := range columnMap {
		storageDescriptor.Columns = append(storageDescriptor.Columns, &glue.Column{
			Name: aws.String(colName),
			Type: aws.String(colType),
		})
	}
	input.TableInput.StorageDescriptor = &storageDescriptor

	_, err = wh.glueClient.CreateTable(&input)
	if err != nil {
		_, ok := err.(*glue.AlreadyExistsException)
		if ok {
			err = nil
		}
	}
	return
}

func (wh *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	updateTableInput := glue.UpdateTableInput{
		DatabaseName: aws.String(wh.Database),
		TableInput: &glue.TableInput{
			Name: aws.String(tableName),
		},
	}

	// fetch schema from glue
	schema, err := wh.FetchSchema(wh.Warehouse)
	if err != nil {
		return err
	}

	// get table schema
	tableSchema, ok := schema[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in schema", tableName)
	}

	// add new column to tableSchema
	tableSchema[columnName] = columnType

	// add all columns in tableSchema to table update request
	storageDescriptor := glue.StorageDescriptor{
		Columns: []*glue.Column{},
	}
	for colName, colType := range tableSchema {
		storageDescriptor.Columns = append(storageDescriptor.Columns, &glue.Column{
			Name: aws.String(colName),
			Type: aws.String(colType),
		})
	}
	updateTableInput.TableInput.StorageDescriptor = &storageDescriptor

	// update table
	_, err = wh.glueClient.UpdateTable(&updateTableInput)
	return
}

func (wh *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return wh.AlterColumn(tableName, columnName, columnType)
}

func (wh *HandleT) LoadTable(tableName string) error {
	pkgLogger.Infof("Skipping load for table %s : %s is a s3 datalake destination", tableName, wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) LoadUserTables() map[string]error {
	pkgLogger.Infof("Skipping load for user tables : %s is a s3 datalake destination", wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) LoadIdentityMergeRulesTable() error {
	pkgLogger.Infof("Skipping load for identity merge rules : %s is a s3 datalake destination", wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) LoadIdentityMappingsTable() error {
	pkgLogger.Infof("Skipping load for identity mappings : %s is a s3 datalake destination", wh.Warehouse.Destination.ID)
	return nil
}

func (wh *HandleT) Cleanup() {
}

func (wh *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error) {
	return false, nil
}

func (wh *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) error {
	return fmt.Errorf("s3_datalake err :not implemented")
}

func (wh *HandleT) DownloadIdentityRules(*misc.GZipWriter) error {
	return fmt.Errorf("s3_datalake err :not implemented")
}

func (wh *HandleT) GetTotalCountInTable(tableName string) (int64, error) {
	return 0, nil
}

func (wh *HandleT) Connect(warehouse warehouseutils.WarehouseT) (client.Client, error) {
	return client.Client{}, fmt.Errorf("s3_datalake err :not implemented")
}

func (wh *HandleT) SgetGlueClient() (*glue.Glue, error) {
	var accessKey, accessKeyID string

	// create session using default credentials - for vpc and open source deployments
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	// create config for glue
	config := aws.NewConfig()

	// read credentials from config if they exist
	if misc.HasAWSKeysInConfig(wh.Warehouse.Destination.Config) {
		accessKey = warehouseutils.GetConfigValue(AWSAccessKey, wh.Warehouse)
		accessKeyID = warehouseutils.GetConfigValue(AWSAccessKeyID, wh.Warehouse)
		config = config.WithCredentials(credentials.NewStaticCredentials(accessKeyID, accessKey, ""))
	}

	// read region from config
	if misc.HasAWSRegionInConfig(wh.Warehouse.Destination.Config) {
		region := warehouseutils.GetConfigValue(AWSRegion, wh.Warehouse)
		config = config.WithRegion(region)
	}

	// td: need to read region and accountId or one of them??
	svc := glue.New(sess, config)
	return svc, nil
}
