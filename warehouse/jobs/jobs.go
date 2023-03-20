package jobs

import (
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WhAsyncJob struct{}

func (*WhAsyncJob) GetSchemaInWarehouse() model.Schema {
	return model.Schema{}
}

func (*WhAsyncJob) GetLocalSchema() (model.Schema, error) {
	return model.Schema{}, nil
}

func (*WhAsyncJob) UpdateLocalSchema(model.Schema) error {
	return nil
}

func (*WhAsyncJob) GetTableSchemaInWarehouse(string) model.TableSchema {
	return model.TableSchema{}
}

func (*WhAsyncJob) GetTableSchemaInUpload(string) model.TableSchema {
	return model.TableSchema{}
}

func (*WhAsyncJob) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptions) []warehouseutils.LoadFile {
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
