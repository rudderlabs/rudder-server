package uploader

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type NOOP struct{}

func (*NOOP) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.Schema{}
}

func (*NOOP) GetLocalSchema() warehouseutils.Schema {
	return warehouseutils.Schema{}
}

func (*NOOP) UpdateLocalSchema(warehouseutils.Schema) error {
	return nil
}

func (*NOOP) GetTableSchemaInWarehouse(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchema{}
}

func (*NOOP) GetTableSchemaInUpload(string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchema{}
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
