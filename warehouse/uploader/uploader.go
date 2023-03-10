package uploader

import (
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Uploader interface {
	GetSchemaInWarehouse() warehouseutils.Schema
	GetLocalSchema() (warehouseutils.Schema, error)
	UpdateLocalSchema(schema warehouseutils.Schema) error
	GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchema
	GetTableSchemaInUpload(tableName string) warehouseutils.TableSchema
	GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile
	GetSampleLoadFileLocation(tableName string) (string, error)
	GetSingleLoadFile(tableName string) (warehouseutils.LoadFile, error)
	ShouldOnDedupUseNewRecord() bool
	UseRudderStorage() bool
	GetLoadFileGenStartTIme() time.Time
	GetLoadFileType() string
	GetFirstLastEvent() (time.Time, time.Time)
}
