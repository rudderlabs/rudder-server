package jobs

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WhAsyncJob struct{}

func (*WhAsyncJob) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*WhAsyncJob) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*WhAsyncJob) UpdateLocalSchema(warehouseutils.SchemaT) error {
	return nil
}

func (*WhAsyncJob) GetTableSchemaInWarehouse(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*WhAsyncJob) GetTableSchemaInUpload(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*WhAsyncJob) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (*WhAsyncJob) GetSampleLoadFileLocation(string) (string, error) {
	return "", nil
}

func (*WhAsyncJob) GetSingleLoadFile(string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (*WhAsyncJob) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (*WhAsyncJob) UseRudderStorage() bool {
	return false
}

func (*WhAsyncJob) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (*WhAsyncJob) GetLoadFileType() string {
	return ""
}

func (*WhAsyncJob) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Now(), time.Now()
}
