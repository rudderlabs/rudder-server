package uploader

import (
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Uploader interface {
	GetSchemaInWarehouse() warehouseutils.SchemaT
	GetLocalSchema() warehouseutils.SchemaT
	UpdateLocalSchema(schema warehouseutils.SchemaT) error
	GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT
	GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT
	GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT
	GetSampleLoadFileLocation(tableName string) (string, error)
	GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error)
	ShouldOnDedupUseNewRecord() bool
	UseRudderStorage() bool
	GetLoadFileGenStartTIme() time.Time
	GetLoadFileType() string
	GetFirstLastEvent() (time.Time, time.Time)
}
