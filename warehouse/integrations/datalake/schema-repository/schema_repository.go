package schemarepository

import (
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("datalake").Child("schema-repository")
}

var (
	pkgLogger    logger.Logger
	dataTypesMap = map[string]string{
		"boolean":  "boolean",
		"int":      "bigint",
		"bigint":   "bigint",
		"float":    "double",
		"string":   "varchar(65535)",
		"datetime": "timestamp",
	}
	dataTypesMapToRudder = map[string]string{
		"boolean": "boolean",

		"bigint": "int",
		"int":    "int",

		"double":  "float",
		"float":   "float",
		"decimal": "float",

		"varchar(512)":   "string",
		"varchar(65535)": "string",
		"string":         "string",

		"timestamp": "datetime",
	}
)

type SchemaRepository interface {
	FetchSchema(warehouse model.Warehouse) (model.Schema, model.Schema, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap model.TableSchema) (err error)
	AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error)
	RefreshPartitions(tableName string, loadFiles []warehouseutils.LoadFile) error
}

func UseGlue(w *model.Warehouse) bool {
	glueConfig := warehouseutils.GetConfigValueBoolString(UseGlueConfig, *w)
	hasAWSRegion := misc.HasAWSRegionInConfig(w.Destination.Config)
	return glueConfig == "true" && hasAWSRegion
}

func NewSchemaRepository(wh model.Warehouse, uploader warehouseutils.Uploader) (SchemaRepository, error) {
	if UseGlue(&wh) {
		return NewGlueSchemaRepository(wh)
	}
	return NewLocalSchemaRepository(wh, uploader)
}

// LoadFileBatching batches load files for refresh partitions
func LoadFileBatching(files []warehouseutils.LoadFile, batchSize int) [][]warehouseutils.LoadFile {
	fileBatches := make([][]warehouseutils.LoadFile, 0, len(files)/batchSize+1)

	for len(files) > 0 {
		cut := batchSize
		if len(files) < cut {
			cut = len(files)
		}

		fileBatches = append(fileBatches, files[0:cut])
		files = files[cut:]
	}

	return fileBatches
}
