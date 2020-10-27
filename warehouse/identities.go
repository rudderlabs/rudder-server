package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/logger"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/manager"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	shouldPopulateHistoricIdentities            bool
	populatingHistoricIdentitiesProgressMap     map[string]bool
	populatingHistoricIdentitiesProgressMapLock sync.RWMutex
	populatedHistoricIdentitiesMap              map[string]bool
	populatedHistoricIdentitiesMapLock          sync.RWMutex
)

func init() {
	shouldPopulateHistoricIdentities = config.GetBool("Warehouse.populateHistoricIdentities", false)
	populatingHistoricIdentitiesProgressMap = map[string]bool{}
	populatedHistoricIdentitiesMap = map[string]bool{}
}

func uniqueWarehouseNamespaceString(warehouse warehouseutils.WarehouseT) string {
	return fmt.Sprintf(`namespace:%s:destination:%s`, warehouse.Namespace, warehouse.Destination.ID)
}

func isDestHistoricIdentitiesPopulated(warehouse warehouseutils.WarehouseT) bool {
	populatedHistoricIdentitiesMapLock.RLock()
	if populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatedHistoricIdentitiesMapLock.RUnlock()
		return true
	}
	populatedHistoricIdentitiesMapLock.RUnlock()
	return false
}

func setDestHistoricIndetitiesPopulated(warehouse warehouseutils.WarehouseT) {
	populatedHistoricIdentitiesMapLock.Lock()
	populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] = true
	populatedHistoricIdentitiesMapLock.Unlock()
}

func setDestHistoricIdentitiesPopulateInProgress(warehouse warehouseutils.WarehouseT, starting bool) {
	populatingHistoricIdentitiesProgressMapLock.Lock()
	if starting {
		populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] = true
	} else {
		delete(populatingHistoricIdentitiesProgressMap, uniqueWarehouseNamespaceString(warehouse))
	}
	populatingHistoricIdentitiesProgressMapLock.Unlock()
}

func isDestHistoricIdentitiesPopulateInProgress(warehouse warehouseutils.WarehouseT) bool {
	populatingHistoricIdentitiesProgressMapLock.RLock()
	if populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatingHistoricIdentitiesProgressMapLock.RUnlock()
		return true
	}
	populatingHistoricIdentitiesProgressMapLock.RUnlock()
	return false
}

func (wh *HandleT) getPendingPopulateIdentitiesLoad(warehouse warehouseutils.WarehouseT) (upload UploadT, found bool) {
	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, source_id, destination_id, destination_type, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.destination_type='%[4]s' AND %[1]s.status != '%[5]s' AND %[1]s.status != '%[6]s') ORDER BY id asc`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, wh.poulateHistoricIdentitiesDestType(), ExportedData, Aborted)

	var schema json.RawMessage
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.SourceID, &upload.DestinationID, &upload.DestinationType, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(err)
	}
	found = true
	upload.Schema = warehouseutils.JSONSchemaToMap(schema)
	return
}

func (wh *HandleT) poulateHistoricIdentitiesDestType() string {
	return wh.destType + "_IDENTITY_PRE_LOAD"
}

func (wh *HandleT) hasLocalIdentityData(warehouse warehouseutils.WarehouseT) bool {
	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s`, warehouseutils.IdentityMergeRulesTableName(warehouse))
	var count int
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&count)
	if err != nil {
		// TOOD: Handle this
		panic(err)
	}
	return count > 0
}

func (wh *HandleT) hasWarehouseData(warehouse warehouseutils.WarehouseT) (bool, error) {
	whManager, err := manager.New(wh.destType)
	if err != nil {
		panic(err)
	}

	empty, err := whManager.IsEmpty(warehouse)
	if err != nil {
		return false, err
	}
	return !empty, nil
}

func (wh *HandleT) setupIdentityTables(warehouse warehouseutils.WarehouseT) {
	var name sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT to_regclass('%s')`, warehouseutils.IdentityMappingsTableName(warehouse))
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&name)
	if err != nil {
		panic(err)
	}
	if len(name.String) > 0 {
		return
	}
	// create tables

	sqlStatement = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			merge_property_1_type VARCHAR(64) NOT NULL,
			merge_property_1_value TEXT NOT NULL,
			merge_property_2_type VARCHAR(64),
			merge_property_2_value TEXT,
			created_at TIMESTAMP NOT NULL DEFAULT NOW());
		`, warehouseutils.IdentityMergeRulesTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS merge_properties_index_%[1]s ON %[1]s (merge_property_1_type, merge_property_1_value, merge_property_2_type, merge_property_2_value)`, warehouseutils.IdentityMergeRulesTableName(warehouse))

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id BIGSERIAL PRIMARY KEY,
			merge_property_type VARCHAR(64) NOT NULL,
			merge_property_value TEXT NOT NULL,
			rudder_id VARCHAR(64) NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT NOW());
		`, warehouseutils.IdentityMappingsTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`
		ALTER TABLE %s
			ADD CONSTRAINT %s UNIQUE (merge_property_type, merge_property_value);
		`, warehouseutils.IdentityMappingsTableName(warehouse), warehouseutils.IdentityMappingsUniqueMappingConstraintName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS rudder_id_index_%[1]s ON %[1]s (rudder_id)`, warehouseutils.IdentityMappingsTableName(warehouse))

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}

	sqlStatement = fmt.Sprintf(`CREATE INDEX IF NOT EXISTS merge_property_index_%[1]s ON %[1]s (merge_property_type, merge_property_value)`, warehouseutils.IdentityMappingsTableName(warehouse))

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(err)
	}
}

func (wh *HandleT) initPrePopulateDestIndetitiesUpload(warehouse warehouseutils.WarehouseT) UploadT {
	schema := make(map[string]map[string]string)
	// TODO: DRY this code
	identityRules := map[string]string{
		warehouseutils.ToProviderCase(wh.destType, "merge_property_1_type"):  "string",
		warehouseutils.ToProviderCase(wh.destType, "merge_property_1_value"): "string",
		warehouseutils.ToProviderCase(wh.destType, "merge_property_2_type"):  "string",
		warehouseutils.ToProviderCase(wh.destType, "merge_property_2_value"): "string",
	}
	schema[warehouseutils.ToProviderCase(wh.destType, warehouseutils.IdentityMergeRulesTable)] = identityRules

	// add rudder_identity_mappings table
	identityMappings := map[string]string{
		warehouseutils.ToProviderCase(wh.destType, "merge_property_type"):  "string",
		warehouseutils.ToProviderCase(wh.destType, "merge_property_value"): "string",
		warehouseutils.ToProviderCase(wh.destType, "rudder_id"):            "string",
		warehouseutils.ToProviderCase(wh.destType, "updated_at"):           "datetime",
	}
	schema[warehouseutils.ToProviderCase(wh.destType, warehouseutils.IdentityMappingsTable)] = identityMappings

	marshalledSchema, err := json.Marshal(schema)
	if err != nil {
		panic(err)
	}

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, status, schema, error, created_at, updated_at, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id)	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13) RETURNING id`, warehouseutils.WarehouseUploadsTable)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	now := timeutil.Now()
	row := stmt.QueryRow(warehouse.Source.ID, warehouse.Namespace, warehouse.Destination.ID, wh.poulateHistoricIdentitiesDestType(), Waiting, marshalledSchema, "{}", now, now, 0, 0, 0, 0)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := UploadT{
		ID:              uploadID,
		Namespace:       warehouse.Namespace,
		SourceID:        warehouse.Source.ID,
		DestinationID:   warehouse.Destination.ID,
		DestinationType: wh.poulateHistoricIdentitiesDestType(),
		Status:          Waiting,
		Schema:          schema,
	}

	return upload
}

func (wh *HandleT) setFailedStat(warehouse warehouseutils.WarehouseT, err error) {
	if err != nil {
		warehouseutils.DestStat(stats.CountType, "upload_failed", warehouse.Identifier).Count(1)
	}
}

func (wh *HandleT) populateHistoricIdentities(warehouse warehouseutils.WarehouseT) {
	if isDestHistoricIdentitiesPopulated(warehouse) || isDestHistoricIdentitiesPopulateInProgress(warehouse) {
		return
	}

	setDestInProgress(warehouse, true)
	setDestHistoricIdentitiesPopulateInProgress(warehouse, true)
	rruntime.Go(func() {
		var err error
		defer setDestInProgress(warehouse, false)
		defer setDestHistoricIdentitiesPopulateInProgress(warehouse, false)
		defer setDestHistoricIndetitiesPopulated(warehouse)
		defer wh.setFailedStat(warehouse, err)

		// check for pending loads (populateHistoricIdentites)
		var hasPendingLoad bool
		var upload UploadT
		upload, hasPendingLoad = wh.getPendingPopulateIdentitiesLoad(warehouse)

		if hasPendingLoad {
			logger.Infof("[WH]: Found pending load (populateHistoricIdentites) for %s:%s", wh.destType, warehouse.Destination.ID)
		} else {
			if wh.hasLocalIdentityData(warehouse) {
				logger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentites) for %s:%s as data exists locally", wh.destType, warehouse.Destination.ID)
				return
			}
			var hasData bool
			hasData, err = wh.hasWarehouseData(warehouse)
			if err != nil {
				logger.Errorf(`[WH]: Error checking for data in %s:%s:%s`, wh.destType, warehouse.Destination.ID, warehouse.Destination.Name)
				return
			}
			if !hasData {
				logger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentites) for %s:%s as warehouse does not have any data", wh.destType, warehouse.Destination.ID)
				return
			}
			logger.Infof("[WH]: Did not find local identity tables..")
			logger.Infof("[WH]: Generating identity tables based on data in warehouse %s:%s", wh.destType, warehouse.Destination.ID)
			upload = wh.initPrePopulateDestIndetitiesUpload(warehouse)
		}

		whManager, err := manager.New(wh.destType)
		if err != nil {
			panic(err)
		}

		job := UploadJobT{
			upload:     &upload,
			warehouse:  warehouse,
			whManager:  whManager,
			dbHandle:   wh.dbHandle,
			pgNotifier: &wh.notifier,
		}

		tableUploadsCreated, err := areTableUploadsCreated(job.upload.ID)
		if err != nil {
			panic(err)
		}
		if !tableUploadsCreated {
			err := job.initTableUploads()
			if err != nil {
				// TODO: Handle error / Retry
				logger.Error("[WH]: Error creating records in wh_table_uploads", err)
			}
		}

		err = whManager.Setup(job.warehouse, &job)
		if err != nil {
			job.setUploadError(err, Aborted)
			return
		}
		defer whManager.Cleanup()

		var schemaInWarehouse warehouseutils.SchemaT
		schemaInWarehouse, err = whManager.FetchSchema(job.warehouse)
		if err != nil {
			logger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
			job.setUploadError(err, Aborted)
			return
		}

		job.setUploadStatus(UpdatedRemoteSchema)
		diff := getSchemaDiff(schemaInWarehouse, job.upload.Schema)
		err = whManager.MigrateSchema(diff)
		if err != nil {
			job.setUploadError(err, Aborted)
			return
		}
		job.setUploadStatus(UpdatedRemoteSchema)

		job.setUploadStatus(getInProgressState(ExportedData))
		loadErrors, err := job.loadIdentityTables(true)
		if len(loadErrors) > 0 {
			job.setUploadError(warehouseutils.ConcatErrors(loadErrors), Aborted)
		}
		job.setUploadStatus(ExportedData)
		return
	})
}
