package schemarepository

import (
	"context"
	"fmt"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/samber/lo"
)

const MAX_CHARACTER_LIMIT = 65535

var (
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
	FetchSchema(ctx context.Context, warehouse model.Warehouse) (model.Schema, model.Schema, error)
	CreateSchema(ctx context.Context) (err error)
	CreateTable(ctx context.Context, tableName string, columnMap model.TableSchema) (err error)
	AddColumns(ctx context.Context, tableName string, columnsInfo []warehouseutils.ColumnInfo) (err error)
	AlterColumn(ctx context.Context, tableName, columnName, columnType string) (model.AlterTableResponse, error)
	RefreshPartitions(ctx context.Context, tableName string, loadFiles []warehouseutils.LoadFile) error
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
	return lo.Chunk(files, batchSize)
}
