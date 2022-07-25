package dedup

import (
	"time"

	backendconfig "github.com/rudderlabs/rudder-server/config/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type DedupRequest struct {
	Destination backendconfig.DestinationT `json:"destination"`
}

type DedupJob struct {
	DedupR *DedupRequest
}

type DedupHandleT struct {
	infoRequest *DedupRequest
	warehouse   warehouseutils.WarehouseT
	manager     manager.WarehouseOperations
}

func (job *DedupJob) GetSchemaInWarehouse() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *DedupJob) GetLocalSchema() warehouseutils.SchemaT {
	return warehouseutils.SchemaT{}
}

func (job *DedupJob) UpdateLocalSchema(schema warehouseutils.SchemaT) error {
	return nil
}

func (job *DedupJob) GetTableSchemaInWarehouse(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *DedupJob) GetTableSchemaInUpload(tableName string) warehouseutils.TableSchemaT {
	return warehouseutils.TableSchemaT{}
}

func (job *DedupJob) GetLoadFilesMetadata(options warehouseutils.GetLoadFilesOptionsT) []warehouseutils.LoadFileT {
	return []warehouseutils.LoadFileT{}
}

func (job *DedupJob) GetSampleLoadFileLocation(tableName string) (string, error) {
	return "", nil
}

func (job *DedupJob) GetSingleLoadFile(tableName string) (warehouseutils.LoadFileT, error) {
	return warehouseutils.LoadFileT{}, nil
}

func (job *DedupJob) ShouldOnDedupUseNewRecord() bool {
	return false
}

func (job *DedupJob) UseRudderStorage() bool {
	return false
}

func (job *DedupJob) GetLoadFileGenStartTIme() time.Time {
	return time.Time{}
}

func (job *DedupJob) GetLoadFileType() string {
	return ""
}
