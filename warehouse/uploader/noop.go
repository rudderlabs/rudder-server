package uploader

import (
	"time"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type Noop struct{}

func (*Noop) GetSchemaInWarehouse() warehouseutils.Schema {
	return warehouseutils.Schema{}
}

func (*Noop) GetLocalSchema() warehouseutils.Schema {
	return warehouseutils.Schema{}
}

func (*Noop) UpdateLocalSchema(warehouseutils.Schema) error {
	return nil
}

func (*Noop) GetTableSchemaInWarehouse(string) warehouseutils.TableSchema {
	return warehouseutils.TableSchema{}
}

func (*Noop) GetTableSchemaInUpload(string) warehouseutils.TableSchema {
	return warehouseutils.TableSchema{}
}

func (*Noop) GetLoadFilesMetadata(warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (*Noop) GetSampleLoadFileLocation(string) (string, error) {
	return "", nil
}

func (*Noop) GetSingleLoadFile(string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (*Noop) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (*Noop) UseRudderStorage() bool {
	return false
}

func (*Noop) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (*Noop) GetLoadFileType() string {
	return ""
}

func (*Noop) GetFirstLastEvent() (time.Time, time.Time) {
	return time.Time{}, time.Time{}
}
