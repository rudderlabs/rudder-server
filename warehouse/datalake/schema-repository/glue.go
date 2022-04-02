package schemarepository

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	// config
	AWSAccessKey        = "accessKey"
	AWSAccessKeyID      = "accessKeyID"
	AWSBucketNameConfig = "bucketName"
	AWSS3Prefix         = "prefix"
	AWSRegion           = "region"
	UseGlueConfig       = "useGlue"

	// glue
	glueSerdeName             = "ParquetHiveSerDe"
	glueSerdeSerializationLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	glueParquetInputFormat    = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	glueParquetOutputFormat   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
)

type GlueSchemaRepository struct {
	glueClient *glue.Glue
	s3bucket   string
	s3prefix   string
	Warehouse  warehouseutils.WarehouseT
	Namespace  string
}

func NewGlueSchemaRepository(wh warehouseutils.WarehouseT) (*GlueSchemaRepository, error) {
	gl := GlueSchemaRepository{
		s3bucket:  warehouseutils.GetConfigValue(AWSBucketNameConfig, wh),
		s3prefix:  warehouseutils.GetConfigValue(AWSS3Prefix, wh),
		Warehouse: wh,
		Namespace: wh.Namespace,
	}

	glueClient, err := getGlueClient(wh)
	if err != nil {
		return nil, err
	}
	gl.glueClient = glueClient

	return &gl, nil
}

func (gl *GlueSchemaRepository) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	var schema = warehouseutils.SchemaT{}
	var err error

	var getTablesOutput *glue.GetTablesOutput
	var getTablesInput *glue.GetTablesInput
	for true {
		getTablesInput = &glue.GetTablesInput{DatabaseName: &warehouse.Namespace}

		if getTablesOutput != nil && getTablesOutput.NextToken != nil {
			// add nextToken to the request if there are multiple list segments
			getTablesInput.NextToken = getTablesOutput.NextToken
		}

		getTablesOutput, err = gl.glueClient.GetTables(getTablesInput)
		if err != nil {
			if _, ok := err.(*glue.EntityNotFoundException); ok {
				pkgLogger.Debugf("FetchSchema: database %s not found in glue. returning empty schema", warehouse.Namespace)
				err = nil
			}
			return schema, err
		}

		for _, table := range getTablesOutput.TableList {
			if table.Name != nil && table.StorageDescriptor != nil && table.StorageDescriptor.Columns != nil {
				tableName := *table.Name
				if _, ok := schema[tableName]; !ok {
					schema[tableName] = map[string]string{}
				}

				for _, col := range table.StorageDescriptor.Columns {
					schema[tableName][*col.Name] = dataTypesMapToRudder[*col.Type]
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

func (gl *GlueSchemaRepository) CreateSchema() (err error) {
	_, err = gl.glueClient.CreateDatabase(&glue.CreateDatabaseInput{
		DatabaseInput: &glue.DatabaseInput{
			Name: &gl.Namespace,
		},
	})
	if err != nil {
		if _, ok := err.(*glue.AlreadyExistsException); ok {
			pkgLogger.Infof("Skipping database creation : database %s already exists", gl.Namespace)
			err = nil
		}
	}
	return
}

func (gl *GlueSchemaRepository) CreateTable(tableName string, columnMap map[string]string) (err error) {
	// create table request
	input := glue.CreateTableInput{
		DatabaseName: aws.String(gl.Namespace),
		TableInput: &glue.TableInput{
			Name: aws.String(tableName),
		},
	}

	// add storage descriptor to create table request
	input.TableInput.StorageDescriptor = gl.getStorageDescriptor(tableName, columnMap)

	_, err = gl.glueClient.CreateTable(&input)
	if err != nil {
		_, ok := err.(*glue.AlreadyExistsException)
		if ok {
			err = nil
		}
	}
	return
}

func (gl *GlueSchemaRepository) AddColumn(tableName string, columnName string, columnType string) (err error) {
	updateTableInput := glue.UpdateTableInput{
		DatabaseName: aws.String(gl.Namespace),
		TableInput: &glue.TableInput{
			Name: aws.String(tableName),
		},
	}

	// fetch schema from glue
	schema, err := gl.FetchSchema(gl.Warehouse)
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

	// add storage descriptor to update table request
	updateTableInput.TableInput.StorageDescriptor = gl.getStorageDescriptor(tableName, tableSchema)

	// update table
	_, err = gl.glueClient.UpdateTable(&updateTableInput)
	return
}

func (gl *GlueSchemaRepository) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return gl.AddColumn(tableName, columnName, columnType)
}

func getGlueClient(wh warehouseutils.WarehouseT) (*glue.Glue, error) {
	var accessKey, accessKeyID string

	// create session using default credentials - for vpc and open source deployments
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	// create config for glue
	config := aws.NewConfig()

	// read credentials from config if they exist
	if misc.HasAWSKeysInConfig(wh.Destination.Config) {
		accessKey = warehouseutils.GetConfigValue(AWSAccessKey, wh)
		accessKeyID = warehouseutils.GetConfigValue(AWSAccessKeyID, wh)
		config = config.WithCredentials(credentials.NewStaticCredentials(accessKeyID, accessKey, ""))
	}

	// read region from config
	if misc.HasAWSRegionInConfig(wh.Destination.Config) {
		region := warehouseutils.GetConfigValue(AWSRegion, wh)
		config = config.WithRegion(region)
	}

	svc := glue.New(sess, config)
	return svc, nil
}

func (gl *GlueSchemaRepository) getStorageDescriptor(tableName string, columnMap map[string]string) *glue.StorageDescriptor {
	storageDescriptor := glue.StorageDescriptor{
		Columns:  []*glue.Column{},
		Location: aws.String(gl.getS3LocationForTable(tableName)),
		SerdeInfo: &glue.SerDeInfo{
			Name:                 aws.String(glueSerdeName),
			SerializationLibrary: aws.String(glueSerdeSerializationLib),
		},
		InputFormat:  aws.String(glueParquetInputFormat),
		OutputFormat: aws.String(glueParquetOutputFormat),
	}

	// add columns to storage descriptor
	for colName, colType := range columnMap {
		storageDescriptor.Columns = append(storageDescriptor.Columns, &glue.Column{
			Name: aws.String(colName),
			Type: aws.String(dataTypesMap[colType]),
		})
	}

	return &storageDescriptor
}

func (gl *GlueSchemaRepository) getS3LocationForTable(tableName string) string {
	bucketPath := fmt.Sprintf("s3://%s", gl.s3bucket)
	var filePath string
	if gl.s3prefix != "" {
		filePath = fmt.Sprintf("%s/", gl.s3prefix)
	}
	filePath += warehouseutils.GetTablePathInObjectStorage(gl.Namespace, tableName)
	return fmt.Sprintf("%s/%s", bucketPath, filePath)
}
