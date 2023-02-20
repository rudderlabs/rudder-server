package uploader

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type NOOP struct{}

func (*NOOP) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*NOOP) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (*NOOP) UpdateLocalSchema(warehouseutils.SchemaT) error {
	return nil
}

func (*NOOP) GetTableSchemaInWarehouse(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*NOOP) GetTableSchemaInUpload(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (*NOOP) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (*NOOP) GetSampleLoadFileLocation(string) (string, error) {
	return "", nil
}

func (*NOOP) GetSingleLoadFile(string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (*NOOP) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (*NOOP) UseRudderStorage() bool {
	return false
}

func (*NOOP) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (*NOOP) GetLoadFileType() string {
	return ""
}

func (*NOOP) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}
