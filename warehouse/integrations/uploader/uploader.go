package uploader

import (
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Uploader interface {
	GetSchemaInWarehouse() model.Schema
	GetLocalSchema() (model.Schema, error)
	UpdateLocalSchema(schema model.Schema) error
	GetTableSchemaInWarehouse(tableName string) model.TableSchema
	GetTableSchemaInUpload(tableName string) model.TableSchema
	GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile
	GetSampleLoadFileLocation(tableName string) (string, error)
	GetSingleLoadFile(tableName string) (warehouseutils.LoadFile, error)
	ShouldOnDedupUseNewRecord() bool
	UseRudderStorage() bool
	GetLoadFileGenStartTIme() time.Time
	GetLoadFileType() string
	GetFirstLastEvent() (time.Time, time.Time)
}
