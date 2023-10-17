package jobs

import (
	"context"
	"time"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type WhAsyncJob struct{}

func (*WhAsyncJob) IsWarehouseSchemaEmpty() bool { return true }

func (*WhAsyncJob) GetLocalSchema(context.Context) (model.Schema, error) {
	return model.Schema{}, nil
}

func (*WhAsyncJob) UpdateLocalSchema(context.Context, model.Schema) error {
	return nil
}

func (*WhAsyncJob) GetTableSchemaInWarehouse(string) model.TableSchema {
	return model.TableSchema{}
}

func (*WhAsyncJob) GetTableSchemaInUpload(string) model.TableSchema {
	return model.TableSchema{}
}

func (*WhAsyncJob) GetLoadFilesMetadata(context.Context, warehouseutils.GetLoadFilesOptions) ([]warehouseutils.LoadFile, error) {
	return []warehouseutils.LoadFile{}, nil
}

func (*WhAsyncJob) GetSampleLoadFileLocation(context.Context, string) (string, error) {
	return "", nil
}

func (*WhAsyncJob) GetSingleLoadFile(context.Context, string) (warehouseutils.LoadFile, error) {
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

func (*WhAsyncJob) CanAppend() bool { return false }
