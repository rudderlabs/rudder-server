package schemarepository

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	awsutils "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	utils "github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type GlueSchemaRepositoryV2 struct {
	GlueClient *glue.Client
	conf       *config.Config
	logger     logger.Logger
	s3bucket   string
	s3prefix   string
	Warehouse  model.Warehouse
	Namespace  string
}

func NewGlueSchemaRepositoryV2(conf *config.Config, logger logger.Logger, wh model.Warehouse) (*GlueSchemaRepositoryV2, error) {
	sessionConfig, err := utils.NewSessionConfigForDestinationV2(&wh.Destination, "glue")
	if err != nil {
		return nil, err
	}
	awsConfig, err := awsutils.CreateAWSConfig(context.Background(), sessionConfig)
	if err != nil {
		return nil, err
	}
	glueClient := glue.New(glue.Options{
		Credentials: awsConfig.Credentials,
		Region:      awsConfig.Region,
	})

	return &GlueSchemaRepositoryV2{
		GlueClient: glueClient,
		conf:       conf,
		logger:     logger,
		s3bucket:   wh.GetStringDestinationConfig(conf, model.AWSBucketNameSetting),
		s3prefix:   wh.GetStringDestinationConfig(conf, model.AWSPrefixSetting),
		Warehouse:  wh,
		Namespace:  wh.Namespace,
	}, nil
}

func (g *GlueSchemaRepositoryV2) CreateSchema(ctx context.Context) (err error) {
	return nil
}

func (g *GlueSchemaRepositoryV2) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	return nil
}

func (g *GlueSchemaRepositoryV2) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return nil
}

func (g *GlueSchemaRepositoryV2) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, nil
}

func (g *GlueSchemaRepositoryV2) RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error {
	return nil
}

func (g *GlueSchemaRepositoryV2) FetchSchema(ctx context.Context, warehouse model.Warehouse) (model.Schema, error) {
	return model.Schema{}, nil
}
