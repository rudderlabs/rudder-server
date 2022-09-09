package jobs

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WhAsyncJob struct {
}

func (job *WhAsyncJob) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *WhAsyncJob) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *WhAsyncJob) UpdateLocalSchema(schema warehouseutils.SchemaT) error {
	return nil
}

func (job *WhAsyncJob) GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *WhAsyncJob) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *WhAsyncJob) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (job *WhAsyncJob) GetSampleLoadFileLocation(tableName string) (string, error) {
	return "", nil
}

func (job *WhAsyncJob) GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (job *WhAsyncJob) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (job *WhAsyncJob) UseRudderStorage() bool {
	return false
}

func (job *WhAsyncJob) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (job *WhAsyncJob) GetLoadFileType() string {
	return ""
}

func (job *WhAsyncJob) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Now(), time.Now()
}
