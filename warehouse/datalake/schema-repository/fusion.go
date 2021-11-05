package schemarepository

import (
	"cloud.google.com/go/datafusion/apiv1"
	"context"
	"github.com/aws/aws-sdk-go/service/glue"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var ()

type FusionSchemaRepository struct {
	BQContext context.Context
	client    *datafusion.Client
	bucket    string
	prefix    string
	Namespace string
	Warehouse warehouseutils.WarehouseT
}

func NewFusionSchemaRepository(wh warehouseutils.WarehouseT) (*FusionSchemaRepository, error) {
	gl := FusionSchemaRepository{
		bucket:    warehouseutils.GetConfigValue(AWSBucketNameConfig, wh),
		prefix:    warehouseutils.GetConfigValue(AWSS3Prefix, wh),
		Warehouse: wh,
		Namespace: wh.Namespace,
	}

	glueClient, err := getFusionClient(wh)
	if err != nil {
		return nil, err
	}
	gl.client = glueClient

	return &gl, nil
}

func (gl *FusionSchemaRepository) FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error) {
	var schema = warehouseutils.SchemaT{}
	var err error
	return schema, err
}

func (gl *FusionSchemaRepository) CreateSchema() (err error) {
	return
}

func (gl *FusionSchemaRepository) CreateTable(tableName string, columnMap map[string]string) (err error) {
	return
}

func (gl *FusionSchemaRepository) AddColumn(tableName string, columnName string, columnType string) (err error) {
	return
}

func (gl *FusionSchemaRepository) AlterColumn(tableName string, columnName string, columnType string) (err error) {
	return gl.AddColumn(tableName, columnName, columnType)
}

func getFusionClient(wh warehouseutils.WarehouseT) (*datafusion.Client, error) {
	return nil, nil
}

func (gl *FusionSchemaRepository) getStorageDescriptor(tableName string, columnMap map[string]string) *glue.StorageDescriptor {
	return nil
}

func (gl *FusionSchemaRepository) getS3LocationForTable(tableName string) string {
	return ""
}
