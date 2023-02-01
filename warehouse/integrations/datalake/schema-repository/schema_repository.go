package schemarepository

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("datalake").Child("schema-repository")
}

const MAX_CHARACTER_LIMIT = 65535

var (
	pkgLogger    logger.Logger
	VARCHAR_TYPE = fmt.Sprintf("varchar(%d)", MAX_CHARACTER_LIMIT)
	dataTypesMap = map[string]string{
		"boolean":  "boolean",
		"int":      "bigint",
		"bigint":   "bigint",
		"float":    "double",
		"string":   VARCHAR_TYPE,
		"datetime": "timestamp",
	}
	dataTypesMapToRudder = map[string]string{
		"boolean":      "boolean",
		"bigint":       "int",
		"double":       "float",
		"varchar(512)": "string",
		VARCHAR_TYPE:   "string",
		"timestamp":    "datetime",
		"string":       "string",
	}
)

type SchemaRepository interface {
	FetchSchema(warehouse warehouseutils.Warehouse) (warehouseutils.SchemaT, warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumns(tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(tableName, columnName, columnType string) (model.AlterTableResponse, error)
	RefreshPartitions(tableName string, loadFiles []warehouseutils.LoadFileT) error
}

func UseGlue(w *warehouseutils.Warehouse) bool {
	glueConfig := warehouseutils.GetConfigValueBoolString(UseGlueConfig, *w)
	hasAWSRegion := misc.HasAWSRegionInConfig(w.Destination.Config)
	return glueConfig == "true" && hasAWSRegion
}

func NewSchemaRepository(wh warehouseutils.Warehouse, uploader warehouseutils.UploaderI) (SchemaRepository, error) {
	if UseGlue(&wh) {
		return NewGlueSchemaRepository(wh)
	}
	return NewLocalSchemaRepository(wh, uploader)
}

// LoadFileBatching batches load files for refresh partitions
func LoadFileBatching(files []warehouseutils.LoadFileT, batchSize int) [][]warehouseutils.LoadFileT {
	fileBatches := make([][]warehouseutils.LoadFileT, 0, len(files)/batchSize+1)

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
