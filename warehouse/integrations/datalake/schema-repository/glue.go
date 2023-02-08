package schemarepository

import (
	"fmt"
	"net/url"
	"regexp"

	"github.com/rudderlabs/rudder-server/utils/logger"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/rudderlabs/rudder-server/utils/awsutils"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var UseGlueConfig = "useGlue"

// glue specific config
var (
	glueSerdeName             = "ParquetHiveSerDe"
	glueSerdeSerializationLib = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	glueParquetInputFormat    = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	glueParquetOutputFormat   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
)

var (
	PartitionFolderRegex = regexp.MustCompile(`.*/(?P<name>.*)=(?P<value>.*)$`)
	PartitionWindowRegex = regexp.MustCompile(`^(?P<name>.*)=(?P<value>.*)$`)
)

type GlueSchemaRepository struct {
	GlueClient *glue.Glue
	s3bucket   string
	s3prefix   string
	Warehouse  warehouseutils.Warehouse
	Namespace  string
	Logger     logger.Logger
}

func NewGlueSchemaRepository(wh warehouseutils.Warehouse) (*GlueSchemaRepository, error) {
	gl := GlueSchemaRepository{
		s3bucket:  warehouseutils.GetConfigValue(warehouseutils.AWSBucketNameConfig, wh),
		s3prefix:  warehouseutils.GetConfigValue(warehouseutils.AWSS3Prefix, wh),
		Warehouse: wh,
		Namespace: wh.Namespace,
		Logger:    pkgLogger,
	}

	glueClient, err := getGlueClient(wh)
	if err != nil {
		return nil, err
	}
	gl.GlueClient = glueClient

	return &gl, nil
}

func (gl *GlueSchemaRepository) FetchSchema(warehouse warehouseutils.Warehouse) (warehouseutils.SchemaT, warehouseutils.SchemaT, error) {
	schema := warehouseutils.SchemaT{}
	unrecognizedSchema := warehouseutils.SchemaT{}
	var err error

	var getTablesOutput *glue.GetTablesOutput
	var getTablesInput *glue.GetTablesInput
	for {
		getTablesInput = &glue.GetTablesInput{DatabaseName: &warehouse.Namespace}

		if getTablesOutput != nil && getTablesOutput.NextToken != nil {
			// add nextToken to the request if there are multiple list segments
			getTablesInput.NextToken = getTablesOutput.NextToken
		}

		getTablesOutput, err = gl.GlueClient.GetTables(getTablesInput)
		if err != nil {
			if _, ok := err.(*glue.EntityNotFoundException); ok {
				gl.Logger.Debugf("FetchSchema: database %s not found in glue. returning empty schema", warehouse.Namespace)
				err = nil
			}
			return schema, unrecognizedSchema, err
		}

		for _, table := range getTablesOutput.TableList {
			if table.Name != nil && table.StorageDescriptor != nil && table.StorageDescriptor.Columns != nil {
				tableName := *table.Name
				if _, ok := schema[tableName]; !ok {
					schema[tableName] = map[string]string{}
				}

				for _, col := range table.StorageDescriptor.Columns {
					if _, ok := dataTypesMapToRudder[*col.Type]; ok {
						schema[tableName][*col.Name] = dataTypesMapToRudder[*col.Type]
					} else {
						if _, ok := unrecognizedSchema[tableName]; !ok {
							unrecognizedSchema[tableName] = make(map[string]string)
						}
						unrecognizedSchema[tableName][*col.Name] = warehouseutils.MISSING_DATATYPE

						warehouseutils.WHCounterStat(warehouseutils.RUDDER_MISSING_DATATYPE, &warehouse, warehouseutils.Tag{Name: "datatype", Value: *col.Type}).Count(1)
					}
				}
			}
		}

		if getTablesOutput.NextToken == nil {
			// break out of the loop if there are no more list segments
			break
		}
	}

	return schema, unrecognizedSchema, err
}

func (gl *GlueSchemaRepository) CreateSchema() (err error) {
	_, err = gl.GlueClient.CreateDatabase(&glue.CreateDatabaseInput{
		DatabaseInput: &glue.DatabaseInput{
			Name: &gl.Namespace,
		},
	})
	if _, ok := err.(*glue.AlreadyExistsException); ok {
		gl.Logger.Infof("Skipping database creation : database %s already exists", gl.Namespace)
		err = nil
	}
	return
}

func (gl *GlueSchemaRepository) CreateTable(tableName string, columnMap map[string]string) (err error) {
	partitionKeys, err := gl.partitionColumns()
	if err != nil {
		return fmt.Errorf("partition keys: %w", err)
	}

	tableInput := &glue.TableInput{
		Name:          aws.String(tableName),
		PartitionKeys: partitionKeys,
	}

	// create table request
	input := glue.CreateTableInput{
		DatabaseName: aws.String(gl.Namespace),
		TableInput:   tableInput,
	}

	// add storage descriptor to create table request
	input.TableInput.StorageDescriptor = gl.getStorageDescriptor(tableName, columnMap)

	_, err = gl.GlueClient.CreateTable(&input)
	if err != nil {
		_, ok := err.(*glue.AlreadyExistsException)
		if ok {
			err = nil
		}
	}
	return
}

func (gl *GlueSchemaRepository) updateTable(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	updateTableInput := glue.UpdateTableInput{
		DatabaseName: aws.String(gl.Namespace),
		TableInput: &glue.TableInput{
			Name: aws.String(tableName),
		},
	}

	// fetch schema from glue
	schema, _, err := gl.FetchSchema(gl.Warehouse)
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

	partitionKeys, err := gl.partitionColumns()
	if err != nil {
		return fmt.Errorf("partition keys: %w", err)
	}

	// add storage descriptor to update table request
	updateTableInput.TableInput.StorageDescriptor = gl.getStorageDescriptor(tableName, tableSchema)
	updateTableInput.TableInput.PartitionKeys = partitionKeys

	// update table
	_, err = gl.GlueClient.UpdateTable(&updateTableInput)
	return
}

func (gl *GlueSchemaRepository) AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error) {
	return gl.updateTable(tableName, columnsInfo)
}

func (gl *GlueSchemaRepository) AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error) {
	return model.AlterTableResponse{}, gl.updateTable(tableName, []warehouseutils.ColumnInfo{{Name: columnName, Type: columnType}})
}

func getGlueClient(wh warehouseutils.Warehouse) (*glue.Glue, error) {
	sessionConfig, err := awsutils.NewSimpleSessionConfigForDestination(&wh.Destination, glue.ServiceID)
	if err != nil {
		return nil, err
	}
	awsSession, err := awsutils.CreateSession(sessionConfig)
	if err != nil {
		return nil, err
	}
	return glue.New(awsSession), nil
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

// RefreshPartitions takes a tableName and a list of loadFiles and refreshes all the
// partitions that are modified by the path in those loadFiles. It returns any error
// reported by Glue
func (gl *GlueSchemaRepository) RefreshPartitions(tableName string, loadFiles []warehouseutils.LoadFileT) error {
	gl.Logger.Infof("Refreshing partitions for table: %s", tableName)

	// Skip if time window layout is not defined
	if layout := warehouseutils.GetConfigValue("timeWindowLayout", gl.Warehouse); layout == "" {
		return nil
	}

	var (
		locationsToPartition = make(map[string]*glue.PartitionInput)
		locationFolder       string
		err                  error
		partitionInputs      []*glue.PartitionInput
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

		if partitionGroups, err = warehouseutils.CaptureRegexGroup(PartitionFolderRegex, locationFolder); err != nil {
			gl.Logger.Warnf("Skipping refresh partitions for table %s with location %s: %v", tableName, locationFolder, err)
			continue
		}

		locationsToPartition[locationFolder] = &glue.PartitionInput{
			StorageDescriptor: &glue.StorageDescriptor{
				Location: aws.String(locationFolder),
				SerdeInfo: &glue.SerDeInfo{
					Name:                 aws.String(glueSerdeName),
					SerializationLibrary: aws.String(glueSerdeSerializationLib),
				},
				InputFormat:  aws.String(glueParquetInputFormat),
				OutputFormat: aws.String(glueParquetOutputFormat),
			},
			Values: []*string{aws.String(partitionGroups["value"])},
		}
	}

	// Check for existing partitions. We do not want to generate unnecessary (for already existing
	// partitions) changes in Glue tables (since the number of versions of a Glue table
	// is limited)
	for location, partition := range locationsToPartition {
		_, err := gl.GlueClient.GetPartition(&glue.GetPartitionInput{
			DatabaseName:    aws.String(gl.Namespace),
			PartitionValues: partition.Values,
			TableName:       aws.String(tableName),
		})

		if err != nil {
			if _, ok := err.(*glue.EntityNotFoundException); !ok {
				return fmt.Errorf("get partition: %w", err)
			}

			partitionInputs = append(partitionInputs, locationsToPartition[location])
		} else {
			gl.Logger.Debugf("Partition %s already exists in table %s", location, tableName)
		}
	}
	if len(partitionInputs) == 0 {
		return nil
	}

	// Updating table partitions with empty columns to create partition keys if not created
	if err = gl.updateTable(tableName, []warehouseutils.ColumnInfo{}); err != nil {
		return fmt.Errorf("update table: %w", err)
	}

	gl.Logger.Debugf("Refreshing %d partitions", len(partitionInputs))

	if _, err = gl.GlueClient.BatchCreatePartition(&glue.BatchCreatePartitionInput{
		DatabaseName:       aws.String(gl.Namespace),
		PartitionInputList: partitionInputs,
		TableName:          aws.String(tableName),
	}); err != nil {
		return fmt.Errorf("batch create partitions: %w", err)
	}

	return nil
}

func (gl *GlueSchemaRepository) partitionColumns() (columns []*glue.Column, err error) {
	var (
		layout          string
		partitionGroups map[string]string
	)

	if layout = warehouseutils.GetConfigValue("timeWindowLayout", gl.Warehouse); layout == "" {
		return
	}

	if partitionGroups, err = warehouseutils.CaptureRegexGroup(PartitionWindowRegex, layout); err != nil {
		return columns, fmt.Errorf("capture partition window regex: %w", err)
	}

	columns = append(columns, &glue.Column{Name: aws.String(partitionGroups["name"]), Type: aws.String("date")})
	return
}
