package jobs

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WhAsyncJob struct{}

func (*WhAsyncJob) GetSchemaInWarehouse() warehouseutils.Schema {
	return warehouseutils.Schema{}
}

func (*WhAsyncJob) GetLocalSchema() warehouseutils.Schema {
	return warehouseutils.Schema{}
}

func (*WhAsyncJob) UpdateLocalSchema(warehouseutils.Schema) error {
	return nil
}

func (*WhAsyncJob) GetTableSchemaInWarehouse(string) warehouseutils.TableSchema {
	return warehouseutils.TableSchema{}
}

func (*WhAsyncJob) GetTableSchemaInUpload(string) warehouseutils.TableSchema {
	return warehouseutils.TableSchema{}
}

func (*WhAsyncJob) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFile {
	return []warehouseutils.LoadFile{}
}

func (*WhAsyncJob) GetSampleLoadFileLocation(string) (string, error) {
	return "", nil
}

func (*WhAsyncJob) GetSingleLoadFile(string) (warehouseutils.LoadFile, error) {
	return warehouseutils.LoadFile{}, nil
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
