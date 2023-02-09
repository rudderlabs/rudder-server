package warehouse

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/config"
	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/services/stats"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	shouldPopulateHistoricIdentities            bool
	populatingHistoricIdentitiesProgressMap     map[string]bool
	populatingHistoricIdentitiesProgressMapLock sync.RWMutex
	populatedHistoricIdentitiesMap              map[string]bool
	populatedHistoricIdentitiesMapLock          sync.RWMutex
)

func Init2() {
	config.RegisterBoolConfigVariable(false, &shouldPopulateHistoricIdentities, false, "Warehouse.populateHistoricIdentities")
	populatingHistoricIdentitiesProgressMap = map[string]bool{}
	populatedHistoricIdentitiesMap = map[string]bool{}
}

func uniqueWarehouseNamespaceString(warehouse warehouseutils.Warehouse) string {
	return fmt.Sprintf(`namespace:%s:destination:%s`, warehouse.Namespace, warehouse.Destination.ID)
}

func isDestHistoricIdentitiesPopulated(warehouse warehouseutils.Warehouse) bool {
	populatedHistoricIdentitiesMapLock.RLock()
	if populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatedHistoricIdentitiesMapLock.RUnlock()
		return true
	}
	populatedHistoricIdentitiesMapLock.RUnlock()
	return false
}

func setDestHistoricIdentitiesPopulated(warehouse warehouseutils.Warehouse) {
	populatedHistoricIdentitiesMapLock.Lock()
	populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] = true
	populatedHistoricIdentitiesMapLock.Unlock()
}

func setDestHistoricIdentitiesPopulateInProgress(warehouse warehouseutils.Warehouse, starting bool) {
	populatingHistoricIdentitiesProgressMapLock.Lock()
	if starting {
		populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] = true
	} else {
		delete(populatingHistoricIdentitiesProgressMap, uniqueWarehouseNamespaceString(warehouse))
	}
	populatingHistoricIdentitiesProgressMapLock.Unlock()
}

func isDestHistoricIdentitiesPopulateInProgress(warehouse warehouseutils.Warehouse) bool {
	populatingHistoricIdentitiesProgressMapLock.RLock()
	if populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatingHistoricIdentitiesProgressMapLock.RUnlock()
		return true
	}
	populatingHistoricIdentitiesProgressMapLock.RUnlock()
	return false
}

func (wh *HandleT) getPendingPopulateIdentitiesLoad(warehouse warehouseutils.Warehouse) (upload model.Upload, found bool) {
	sqlStatement := fmt.Sprintf(`
		SELECT
			id,
			status,
			schema,
			namespace,
			workspace_id,
			source_id,
			destination_id,
			destination_type,
			start_staging_file_id,
			end_staging_file_id,
			start_load_file_id,
			end_load_file_id,
			error
		FROM %[1]s UT
		WHERE (
			UT.source_id='%[2]s' AND
			UT.destination_id='%[3]s' AND
			UT.destination_type='%[4]s' AND
			UT.status != '%[5]s' AND
			UT.status != '%[6]s'
		)
		ORDER BY id asc
	`,
		warehouseutils.WarehouseUploadsTable,
		warehouse.Source.ID,
		warehouse.Destination.ID,
		wh.populateHistoricIdentitiesDestType(),
		model.ExportedData,
		model.Aborted,
	)

	var schema json.RawMessage
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(
		&upload.ID,
		&upload.Status,
		&schema,
		&upload.Namespace,
		&upload.WorkspaceID,
		&upload.SourceID,
		&upload.DestinationID,
		&upload.DestinationType,
		&upload.StagingFileStartID,
		&upload.StagingFileEndID,
		&upload.LoadFileStartID,
		&upload.LoadFileEndID,
		&upload.Error,
	)
	if err == sql.ErrNoRows {
		return
	}
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	found = true
	upload.UploadSchema = warehouseutils.JSONSchemaToMap(schema)

	// TODO: remove this once the migration is complete
	if upload.WorkspaceID == "" {
		var ok bool
		wh.workspaceBySourceIDsLock.RLock()
		upload.WorkspaceID, ok = wh.workspaceBySourceIDs[upload.SourceID]
		wh.workspaceBySourceIDsLock.RUnlock()

		if !ok {
			pkgLogger.Warnf("Workspace not found for source id: %q", upload.SourceID)
		}

	}

	return
}

func (wh *HandleT) populateHistoricIdentitiesDestType() string {
	return wh.destType + "_IDENTITY_PRE_LOAD"
}

func (wh *HandleT) hasLocalIdentityData(warehouse warehouseutils.Warehouse) (exists bool) {
	sqlStatement := fmt.Sprintf(`
		SELECT
		  EXISTS (
			SELECT
			  1
			FROM
			  %s
		  );
`,
		warehouseutils.IdentityMergeRulesTableName(warehouse),
	)
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&exists)
	if err != nil {
		// TODO: Handle this
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	return
}

func (wh *HandleT) hasWarehouseData(warehouse warehouseutils.Warehouse) (bool, error) {
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

func (wh *HandleT) setupIdentityTables(warehouse warehouseutils.Warehouse) {
	var name sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT to_regclass('%s')`, warehouseutils.IdentityMappingsTableName(warehouse))
	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&name)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
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
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS merge_properties_index_%[1]s ON %[1]s (
		  merge_property_1_type, merge_property_1_value,
		  merge_property_2_type, merge_property_2_value
		);
`,
		warehouseutils.IdentityMergeRulesTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		  id BIGSERIAL PRIMARY KEY,
		  merge_property_type VARCHAR(64) NOT NULL,
		  merge_property_value TEXT NOT NULL,
		  rudder_id VARCHAR(64) NOT NULL,
		  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
		);
		`,
		warehouseutils.IdentityMappingsTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		ALTER TABLE
		  %s
		ADD
		  CONSTRAINT %s UNIQUE (
			merge_property_type, merge_property_value
		  );
		`,
		warehouseutils.IdentityMappingsTableName(warehouse),
		warehouseutils.IdentityMappingsUniqueMappingConstraintName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS rudder_id_index_%[1]s ON %[1]s (rudder_id);
`,
		warehouseutils.IdentityMappingsTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS merge_property_index_%[1]s ON %[1]s (
		  merge_property_type, merge_property_value
		);
`,
		warehouseutils.IdentityMappingsTableName(warehouse),
	)

	_, err = wh.dbHandle.Exec(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
}

func (wh *HandleT) initPrePopulateDestIdentitiesUpload(warehouse warehouseutils.Warehouse) model.Upload {
	schema := make(warehouseutils.SchemaT)
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

	sqlStatement := fmt.Sprintf(`INSERT INTO %s (
		source_id, namespace, workspace_id, destination_id,
		destination_type, status, schema, error, metadata,
		created_at, updated_at,
		start_staging_file_id, end_staging_file_id,
		start_load_file_id, end_load_file_id)
	VALUES
		($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13, $14, $15)
	RETURNING id
	`, warehouseutils.WarehouseUploadsTable)
	stmt, err := wh.dbHandle.Prepare(sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed to prepare with Error : %w", sqlStatement, err))
	}
	defer stmt.Close()

	now := timeutil.Now()
	row := stmt.QueryRow(
		warehouse.Source.ID,
		warehouse.Namespace,
		warehouse.WorkspaceID,
		warehouse.Destination.ID,
		wh.populateHistoricIdentitiesDestType(),
		model.Waiting,
		marshalledSchema,
		"{}",
		"{}",
		now,
		now,
		0,
		0,
		0,
		0,
	)

	var uploadID int64
	err = row.Scan(&uploadID)
	if err != nil {
		panic(err)
	}

	upload := model.Upload{
		ID:              uploadID,
		Namespace:       warehouse.Namespace,
		WorkspaceID:     warehouse.WorkspaceID,
		SourceID:        warehouse.Source.ID,
		DestinationID:   warehouse.Destination.ID,
		DestinationType: wh.populateHistoricIdentitiesDestType(),
		Status:          model.Waiting,
		UploadSchema:    schema,
	}
	return upload
}

func (*HandleT) setFailedStat(warehouse warehouseutils.Warehouse, err error) {
	if err != nil {
		warehouseutils.DestStat(stats.CountType, "failed_uploads", warehouse.Identifier).Count(1)
	}
}

func (wh *HandleT) populateHistoricIdentities(warehouse warehouseutils.Warehouse) {
	if isDestHistoricIdentitiesPopulated(warehouse) || isDestHistoricIdentitiesPopulateInProgress(warehouse) {
		return
	}

	wh.setDestInProgress(warehouse, 0)
	setDestHistoricIdentitiesPopulateInProgress(warehouse, true)
	rruntime.GoForWarehouse(func() {
		var err error
		defer wh.removeDestInProgress(warehouse, 0)
		defer setDestHistoricIdentitiesPopulateInProgress(warehouse, false)
		defer setDestHistoricIdentitiesPopulated(warehouse)
		defer wh.setFailedStat(warehouse, err)

		// check for pending loads (populateHistoricIdentities)
		var (
			hasPendingLoad bool
			upload         model.Upload
		)

		upload, hasPendingLoad = wh.getPendingPopulateIdentitiesLoad(warehouse)

		if hasPendingLoad {
			pkgLogger.Infof("[WH]: Found pending load (populateHistoricIdentities) for %s:%s", wh.destType, warehouse.Destination.ID)
		} else {
			if wh.hasLocalIdentityData(warehouse) {
				pkgLogger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentities) for %s:%s as data exists locally", wh.destType, warehouse.Destination.ID)
				return
			}
			var hasData bool
			hasData, err = wh.hasWarehouseData(warehouse)
			if err != nil {
				pkgLogger.Errorf(`[WH]: Error checking for data in %s:%s:%s, err: %s`, wh.destType, warehouse.Destination.ID, warehouse.Destination.Name, err.Error())
				return
			}
			if !hasData {
				pkgLogger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentities) for %s:%s as warehouse does not have any data", wh.destType, warehouse.Destination.ID)
				return
			}
			pkgLogger.Infof("[WH]: Did not find local identity tables..")
			pkgLogger.Infof("[WH]: Generating identity tables based on data in warehouse %s:%s", wh.destType, warehouse.Destination.ID)
			upload = wh.initPrePopulateDestIdentitiesUpload(warehouse)
		}

		whManager, err := manager.New(wh.destType)
		if err != nil {
			panic(err)
		}

		job := wh.uploadJobFactory.NewUploadJob(&model.UploadJob{
			Upload:    upload,
			Warehouse: warehouse,
		}, whManager)

		tableUploadsCreated := areTableUploadsCreated(job.upload.ID)
		if !tableUploadsCreated {
			err := job.initTableUploads()
			if err != nil {
				// TODO: Handle error / Retry
				pkgLogger.Error("[WH]: Error creating records in wh_table_uploads", err)
			}
		}

		err = whManager.Setup(job.warehouse, job)
		if err != nil {
			job.setUploadError(err, model.Aborted)
			return
		}
		defer whManager.Cleanup()

		schemaHandle := SchemaHandleT{
			warehouse:    job.warehouse,
			stagingFiles: job.stagingFiles,
			dbHandle:     job.dbHandle,
		}
		job.schemaHandle = &schemaHandle

		job.schemaHandle.schemaInWarehouse, job.schemaHandle.unrecognizedSchemaInWarehouse, err = whManager.FetchSchema(job.warehouse)
		if err != nil {
			pkgLogger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
			job.setUploadError(err, model.Aborted)
			return
		}

		job.setUploadStatus(UploadStatusOpts{Status: getInProgressState(model.ExportedData)})
		loadErrors, err := job.loadIdentityTables(true)
		if err != nil {
			pkgLogger.Errorf(`[WH]: Identity table upload errors: %v`, err)
		}
		if len(loadErrors) > 0 {
			job.setUploadError(misc.ConcatErrors(loadErrors), model.Aborted)
			return
		}
		job.setUploadStatus(UploadStatusOpts{Status: model.ExportedData})
	})
}
