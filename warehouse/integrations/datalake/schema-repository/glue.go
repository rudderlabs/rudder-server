package schemarepository

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/awsutil"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	configv2 "github.com/aws/aws-sdk-go-v2/config"
	gluev2 "github.com/aws/aws-sdk-go-v2/service/glue"
	gluev1 "github.com/aws/aws-sdk-go/service/glue"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type GlueSchemaRepository struct {
	GlueClient GlueClient
	s3bucket   string
	s3prefix   string
	Warehouse  model.Warehouse
	Namespace  string
	conf       *config.Config
	logger     logger.Logger
}

func NewGlueSchemaRepository(glueClient GlueClient, conf *config.Config, log logger.Logger, wh model.Warehouse) *GlueSchemaRepository {
	return &GlueSchemaRepository{
		GlueClient: glueClient,
		s3bucket:   wh.GetStringDestinationConfig(conf, model.AWSBucketNameSetting),
		s3prefix:   wh.GetStringDestinationConfig(conf, model.AWSPrefixSetting),
		Warehouse:  wh,
		Namespace:  wh.Namespace,
		conf:       conf,
		logger:     log.Child("schema-repository"),
	}
}

func (gl *GlueSchemaRepository) FetchSchema(ctx context.Context, warehouse model.Warehouse) (model.Schema, error) {
	schema := model.Schema{}
	tables, err := gl.GlueClient.GetTables(ctx, warehouse.Namespace)
	if err != nil {
		return schema, err
	}
	for _, table := range tables {
		tableName := table.Name
		if _, ok := schema[tableName]; !ok {
			schema[tableName] = model.TableSchema{}
		}
		for _, col := range table.Columns {
			if _, ok := dataTypesMapToRudder[col.Type]; ok {
				schema[tableName][col.Name] = dataTypesMapToRudder[col.Type]
			} else {
				warehouseutils.WHCounterStat(stats.Default, warehouseutils.RudderMissingDatatype, &warehouse, warehouseutils.Tag{Name: "datatype", Value: col.Type}).Count(1)
			}
		}
	}
	return schema, nil
}

func (gl *GlueSchemaRepository) CreateSchema(ctx context.Context) error {
	return gl.GlueClient.CreateDatabase(ctx, gl.Namespace)
}

func (gl *GlueSchemaRepository) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) error {
	columns := make([]Column, 0, len(columnMap))
	for colName, colType := range columnMap {
		columns = append(columns, Column{Name: colName, Type: colType})
	}
	table := Table{Name: tableName, Columns: columns}
	return gl.GlueClient.CreateTable(ctx, gl.Namespace, table)
}

func (gl *GlueSchemaRepository) updateTable(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	schema, err := gl.FetchSchema(ctx, gl.Warehouse)
	if err != nil {
		return err
	}
	tableSchema, ok := schema[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in schema", tableName)
	}
	for _, columnInfo := range columnsInfo {
		tableSchema[columnInfo.Name] = columnInfo.Type
	}
	columns := make([]Column, 0, len(tableSchema))
	for colName, colType := range tableSchema {
		columns = append(columns, Column{Name: colName, Type: colType})
	}
	table := Table{Name: tableName, Columns: columns}
	return gl.GlueClient.UpdateTable(ctx, gl.Namespace, table)
}

func (gl *GlueSchemaRepository) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return gl.updateTable(ctx, tableName, columnsInfo)
}

func (gl *GlueSchemaRepository) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, gl.updateTable(ctx, tableName, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}})
}

func NewGlueClientForWarehouse(wh model.Warehouse, useV2 bool) (GlueClient, error) {
	if useV2 {
		cfg, err := configv2.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}
		client := gluev2.NewFromConfig(cfg)
		return &GlueClientV2{client: client}, nil
	} else {
		sessionConfig, err := awsutil.CreateSession(nil) // Pass the correct config if needed
		if err != nil {
			return nil, err
		}
		return &GlueClientV1{client: gluev1.New(sessionConfig)}, nil
	}
}

func (gl *GlueSchemaRepository) DeleteDatabase(ctx context.Context, databaseName string) error {
	return gl.GlueClient.DeleteDatabase(ctx, databaseName)
}

func (gl *GlueSchemaRepository) RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error {
	return gl.GlueClient.RefreshPartitions(ctx, tableName, loadFiles)
}
