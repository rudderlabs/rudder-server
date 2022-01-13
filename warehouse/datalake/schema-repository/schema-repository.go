package schemarepository

import (
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func init() {
	pkgLogger = logger.NewLogger().Child("warehouse").Child("datalake").Child("schema-repository")
}

const MAX_CHARACTER_LIMIT = 65535

var (
	pkgLogger    logger.LoggerI
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
	FetchSchema(warehouse warehouseutils.WarehouseT) (warehouseutils.SchemaT, error)
	CreateSchema() (err error)
	CreateTable(tableName string, columnMap map[string]string) (err error)
	AddColumn(tableName string, columnName string, columnType string) (err error)
	AlterColumn(tableName string, columnName string, columnType string) (err error)
	RefreshPartitions(tableName string, loadFiles []warehouseutils.LoadFileT) error
}

func NewSchemaRepository(wh warehouseutils.WarehouseT, uploader warehouseutils.UploaderI) (SchemaRepository, error) {
	if warehouseutils.GetConfigValueBoolString(UseGlueConfig, wh) == "true" && misc.HasAWSRegionInConfig(wh.Destination.Config) {
		return NewGlueSchemaRepository(wh)
	}
	return NewLocalSchemaRepository(wh, uploader)
}
