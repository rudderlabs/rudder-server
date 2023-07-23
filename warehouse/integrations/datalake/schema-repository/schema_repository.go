package schemarepository

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const maxCharacterLimit = 65535

var (
	varcharType  = fmt.Sprintf("varchar(%d)", maxCharacterLimit)
	dataTypesMap = map[string]string{
		"boolean":  "boolean",
		"int":      "bigint",
		"bigint":   "bigint",
		"float":    "double",
		"string":   varcharType,
		"datetime": "timestamp",
	}
	dataTypesMapToRudder = map[string]string{
		"boolean":      "boolean",
		"bigint":       "int",
		"double":       "float",
		"varchar(512)": "string",
		varcharType:    "string",
		"timestamp":    "datetime",
		"string":       "string",
	}
)

type SchemaRepository interface {
	FetchSchema(ctx context.Context, warehouse model.Warehouse) (model.Schema, model.Schema, error)
	CreateSchema(ctx context.Context) (err error)
	CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error)
	AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error)
	RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error
}

func UseGlue(w *model.Warehouse) bool {
	glueConfig := warehouseutils.GetConfigValueBoolString(useGlueConfig, *w)
	hasAWSRegion := misc.HasAWSRegionInConfig(w.Destination.Config)
	return glueConfig == "true" && hasAWSRegion
}

func NewSchemaRepository(wh model.Warehouse, uploader warehouseutils.Uploader, logger logger.Logger) (SchemaRepository, error) {
	if UseGlue(&wh) {
		return NewGlueSchemaRepository(wh, logger)
	}
	return NewLocalSchemaRepository(wh, uploader)
}
