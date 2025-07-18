package schemarepository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"

	awsutils "github.com/rudderlabs/rudder-go-kit/awsutil_v2"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	utils "github.com/rudderlabs/rudder-server/utils/awsutils"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// glue specific config
var (
	glueSerdeName             = "ParquetHiveSerDe"
	glueSerdeSerializationLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	glueParquetInputFormat    = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	glueParquetOutputFormat   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
)

var (
	partitionFolderRegex = regexp.MustCompile(`.*/(?P<name>.*)=(?P<value>.*)$`)
	partitionWindowRegex = regexp.MustCompile(`^(?P<name>.*)=(?P<value>.*)$`)
)

type GlueSchemaRepository struct {
	GlueClient *glue.Client
	conf       *config.Config
	logger     logger.Logger
	s3bucket   string
	s3prefix   string
	Warehouse  model.Warehouse
	Namespace  string
}

func NewGlueSchemaRepository(conf *config.Config, logger logger.Logger, wh model.Warehouse) (*GlueSchemaRepository, error) {
	sessionConfig, err := utils.NewSimpleSessionConfigForDestinationV2(&wh.Destination, "glue")
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

	return &GlueSchemaRepository{
		GlueClient: glueClient,
		conf:       conf,
		logger:     logger,
		s3bucket:   wh.GetStringDestinationConfig(conf, model.AWSBucketNameSetting),
		s3prefix:   wh.GetStringDestinationConfig(conf, model.AWSPrefixSetting),
		Warehouse:  wh,
		Namespace:  wh.Namespace,
	}, nil
}

func (g *GlueSchemaRepository) CreateSchema(ctx context.Context) (err error) {
	_, err = g.GlueClient.CreateDatabase(ctx, &glue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{
			Name: &g.Namespace,
		},
	})
	var alreadyExistsException *types.AlreadyExistsException
	if errors.As(err, &alreadyExistsException) {
		g.logger.Infof("Skipping database creation : database %s already exists", g.Namespace)
		err = nil
	}
	return
}

func (g *GlueSchemaRepository) CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error) {
	partitionKeys, err := g.partitionColumns()
	if err != nil {
		return fmt.Errorf("partition keys: %w", err)
	}

	tableInput := &types.TableInput{
		Name:          aws.String(tableName),
		PartitionKeys: partitionKeys,
	}

	// create table request
	input := glue.CreateTableInput{
		DatabaseName: aws.String(g.Namespace),
		TableInput:   tableInput,
	}

	// add storage descriptor to create table request
	input.TableInput.StorageDescriptor = g.getStorageDescriptor(tableName, columnMap)

	_, err = g.GlueClient.CreateTable(ctx, &input)
	if err != nil {
		var alreadyExistsException *types.AlreadyExistsException
		ok := errors.As(err, &alreadyExistsException)
		if ok {
			err = nil
		}
	}
	return
}

func (g *GlueSchemaRepository) AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return g.updateTable(ctx, tableName, columnsInfo)
}

func (g *GlueSchemaRepository) AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, g.updateTable(ctx, tableName, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}})
}

func (g *GlueSchemaRepository) RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error {
	g.logger.Infof("Refreshing partitions for table: %s", tableName)

	// Skip if time window layout is not defined
	if layout := g.Warehouse.GetStringDestinationConfig(g.conf, model.TimeWindowLayoutSetting); layout == "" {
		return nil
	}

	var (
		locationsToPartition = make(map[string]*types.PartitionInput)
		locationFolder       string
		err                  error
		partitionInputs      []types.PartitionInput
		partitionGroups      map[string]string
	)

	for _, loadFile := range loadFiles {
		if locationFolder, err = url.QueryUnescape(warehouseutils.GetS3LocationFolder(loadFile.Location)); err != nil {
			return fmt.Errorf("unesscape location folder: %w", err)
		}

		// Skip if we are already going to process this locationFolder
		if _, ok := locationsToPartition[locationFolder]; ok {
			continue
		}

		if partitionGroups, err = warehouseutils.CaptureRegexGroup(partitionFolderRegex, locationFolder); err != nil {
			g.logger.Warnf("Skipping refresh partitions for table %s with location %s: %v", tableName, locationFolder, err)
			continue
		}

		locationsToPartition[locationFolder] = &types.PartitionInput{
			StorageDescriptor: &types.StorageDescriptor{
				Location: aws.String(locationFolder),
				SerdeInfo: &types.SerDeInfo{
					Name:                 aws.String(glueSerdeName),
					SerializationLibrary: aws.String(glueSerdeSerializationLib),
				},
				InputFormat:  aws.String(glueParquetInputFormat),
				OutputFormat: aws.String(glueParquetOutputFormat),
			},
			Values: []string{partitionGroups["value"]},
		}
	}

	// Check for existing partitions. We do not want to generate unnecessary (for already existing
	// partitions) changes in Glue tables (since the number of versions of a Glue table
	// is limited)
	for location, partition := range locationsToPartition {
		_, err := g.GlueClient.GetPartition(ctx, &glue.GetPartitionInput{
			DatabaseName:    aws.String(g.Namespace),
			PartitionValues: partition.Values,
			TableName:       aws.String(tableName),
		})

		if err != nil {
			var entityNotFoundException *types.EntityNotFoundException
			if !errors.As(err, &entityNotFoundException) {
				return fmt.Errorf("get partition: %w", err)
			}

			partitionInputs = append(partitionInputs, *locationsToPartition[location])
		} else {
			g.logger.Debugf("Partition %s already exists in table %s", location, tableName)
		}
	}
	if len(partitionInputs) == 0 {
		return nil
	}

	// Updating table partitions with empty columns to create partition keys if not created
	if err = g.updateTable(ctx, tableName, []warehouseutils.ColumnInfo{}); err != nil {
		return fmt.Errorf("update table: %w", err)
	}

	g.logger.Debugf("Refreshing %d partitions", len(partitionInputs))

	if _, err = g.GlueClient.BatchCreatePartition(ctx, &glue.BatchCreatePartitionInput{
		DatabaseName:       aws.String(g.Namespace),
		PartitionInputList: partitionInputs,
		TableName:          aws.String(tableName),
	}); err != nil {
		return fmt.Errorf("batch create partitions: %w", err)
	}

	return nil
}

func (g *GlueSchemaRepository) FetchSchema(ctx context.Context, warehouse model.Warehouse) (model.Schema, error) {
	schema := model.Schema{}
	var err error

	var getTablesOutput *glue.GetTablesOutput
	var getTablesInput *glue.GetTablesInput
	for {
		getTablesInput = &glue.GetTablesInput{DatabaseName: &warehouse.Namespace}

		if getTablesOutput != nil && getTablesOutput.NextToken != nil {
			// add nextToken to the request if there are multiple list segments
			getTablesInput.NextToken = getTablesOutput.NextToken
		}

		getTablesOutput, err = g.GlueClient.GetTables(ctx, getTablesInput)
		if err != nil {
			var entityNotFoundException *types.EntityNotFoundException
			if errors.As(err, &entityNotFoundException) {
				g.logger.Debugf("FetchSchema: database %s not found in glue. returning empty schema", warehouse.Namespace)
				err = nil
			}
			return schema, err
		}

		for _, table := range getTablesOutput.TableList {
			if table.Name != nil && table.StorageDescriptor != nil && table.StorageDescriptor.Columns != nil {
				tableName := *table.Name
				if _, ok := schema[tableName]; !ok {
					schema[tableName] = model.TableSchema{}
				}

				for _, col := range table.StorageDescriptor.Columns {
					if _, ok := dataTypesMapToRudder[*col.Type]; ok {
						schema[tableName][*col.Name] = dataTypesMapToRudder[*col.Type]
					} else {
						warehouseutils.WHCounterStat(stats.Default, warehouseutils.RudderMissingDatatype, &warehouse, warehouseutils.Tag{Name: "datatype", Value: *col.Type}).Count(1)
					}
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

func (g *GlueSchemaRepository) updateTable(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) error {
	glueInput := &glue.UpdateTableInput{
		DatabaseName: aws.String(g.Namespace),
		TableInput: &types.TableInput{
			Name: aws.String(tableName),
		},
	}

	schema, err := g.FetchSchema(ctx, g.Warehouse)
	if err != nil {
		return err
	}

	// get table schema
	tableSchema, ok := schema[tableName]
	if !ok {
		return fmt.Errorf("table %s not found in schema", tableName)
	}

	// add new columns to table schema
	for _, columnInfo := range columnsInfo {
		tableSchema[columnInfo.Name] = columnInfo.Type
	}

	partitionKeys, err := g.partitionColumns()
	if err != nil {
		return fmt.Errorf("partition keys: %w", err)
	}

	// add storage descriptor to update table request
	glueInput.TableInput.StorageDescriptor = g.getStorageDescriptor(tableName, tableSchema)
	glueInput.TableInput.PartitionKeys = partitionKeys

	_, err = g.GlueClient.UpdateTable(ctx, glueInput)
	return err
}

func (g *GlueSchemaRepository) partitionColumns() (columns []types.Column, err error) {
	var (
		layout          string
		partitionGroups map[string]string
	)

	if layout = g.Warehouse.GetStringDestinationConfig(g.conf, model.TimeWindowLayoutSetting); layout == "" {
		return
	}

	if partitionGroups, err = warehouseutils.CaptureRegexGroup(partitionWindowRegex, layout); err != nil {
		return columns, fmt.Errorf("capture partition window regex: %w", err)
	}

	columns = append(columns, types.Column{Name: aws.String(partitionGroups["name"]), Type: aws.String("date")})
	return
}

func (g *GlueSchemaRepository) getStorageDescriptor(tableName string, columnMap model.TableSchema) *types.StorageDescriptor {
	storageDescriptor := types.StorageDescriptor{
		Columns:  []types.Column{},
		Location: aws.String(g.getS3LocationForTable(tableName)),
		SerdeInfo: &types.SerDeInfo{
			Name:                 aws.String(glueSerdeName),
			SerializationLibrary: aws.String(glueSerdeSerializationLib),
		},
		InputFormat:  aws.String(glueParquetInputFormat),
		OutputFormat: aws.String(glueParquetOutputFormat),
	}

	// add columns to storage descriptor
	for colName, colType := range columnMap {
		storageDescriptor.Columns = append(storageDescriptor.Columns, types.Column{
			Name: aws.String(colName),
			Type: aws.String(dataTypesMap[colType]),
		})
	}

	return &storageDescriptor
}

func (g *GlueSchemaRepository) getS3LocationForTable(tableName string) string {
	bucketPath := fmt.Sprintf("s3://%s", g.s3bucket)
	var filePath string
	if g.s3prefix != "" {
		filePath = fmt.Sprintf("%s/", g.s3prefix)
	}
	filePath += warehouseutils.GetTablePathInObjectStorage(g.Namespace, tableName)
	return fmt.Sprintf("%s/%s", bucketPath, filePath)
}
