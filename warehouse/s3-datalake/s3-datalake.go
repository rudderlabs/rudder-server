package s3datalake

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
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

type HandleT struct {
	glueClient *glue.Glue
	Warehouse  warehouseutils.WarehouseT
	Uploader   warehouseutils.UploaderI
}

func (wh *HandleT) Setup(warehouse warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) error {
	wh.Warehouse = warehouse
	wh.Uploader = uploader

	cl, err := wh.SgetGlueClient()
	if err != nil {
		return err
	}
	wh.glueClient = cl

	return nil
}
func (wh *HandleT) CrashRecover(warehouse warehouseutils.WarehouseT) (err error) {
	return fmt.Errorf("s3_datalake err :not implemented")
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
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) CreateTable(tableName string, columnMap map[string]string) (err error) {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) AddColumn(tableName string, columnName string, columnType string) (err error) {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) LoadTable(tableName string) error {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) LoadUserTables() map[string]error {
	return nil
}
func (wh *HandleT) LoadIdentityMergeRulesTable() error {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) LoadIdentityMappingsTable() error {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) Cleanup() {
}
func (wh *HandleT) IsEmpty(warehouse warehouseutils.WarehouseT) (bool, error) {
	return false, fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) TestConnection(warehouse warehouseutils.WarehouseT) error {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) DownloadIdentityRules(*misc.GZipWriter) error {
	return fmt.Errorf("s3_datalake err :not implemented")
}
func (wh *HandleT) GetTotalCountInTable(tableName string) (int64, error) {
	return 0, fmt.Errorf("s3_datalake err :not implemented")
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
