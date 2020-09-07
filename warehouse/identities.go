package warehouse

import (
	"database/sql"
	"fmt"

	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// func isDestPreLoaded(warehouse warehouseutils.WarehouseT) bool {
// 	preLoadedIdentitiesMapLock.RLock()
// 	if preLoadedIdentitiesMap[connectionString(warehouse)] {
// 		preLoadedIdentitiesMapLock.RUnlock()
// 		return true
// 	}
// 	preLoadedIdentitiesMapLock.RUnlock()
// 	return false
// }

// func setDestPreLoaded(warehouse warehouseutils.WarehouseT) {
// 	preLoadedIdentitiesMapLock.Lock()
// 	preLoadedIdentitiesMap[connectionString(warehouse)] = true
// 	preLoadedIdentitiesMapLock.Unlock()
// }

// func (wh *HandleT) getPendingPreLoad(warehouse warehouseutils.WarehouseT) (upload warehouseutils.UploadT, found bool) {
// 	sqlStatement := fmt.Sprintf(`SELECT id, status, schema, namespace, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id, error FROM %[1]s WHERE (%[1]s.source_id='%[2]s' AND %[1]s.destination_id='%[3]s' AND %[1]s.destination_type='%[4]s') ORDER BY id asc`, warehouseutils.WarehouseUploadsTable, warehouse.Source.ID, warehouse.Destination.ID, wh.preLoadDestType())

// 	var schema json.RawMessage
// 	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&upload.ID, &upload.Status, &schema, &upload.Namespace, &upload.StartStagingFileID, &upload.EndStagingFileID, &upload.StartLoadFileID, &upload.EndLoadFileID, &upload.Error)
// 	if err == sql.ErrNoRows {
// 		return
// 	}
// 	if err != nil {
// 		panic(err)
// 	}
// 	found = true
// 	upload.Schema = warehouseutils.JSONSchemaToMap(schema)
// 	return
// }

// func (wh *HandleT) preLoadDestType() string {
// 	return wh.destType + "_IDENTITY_PRE_LOAD"
// }

// func (wh *HandleT) hasLocalIdentityData(warehouse warehouseutils.WarehouseT) bool {
// 	sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s`, warehouseutils.IdentityMergeRulesTableName(warehouse))
// 	var count int
// 	err := wh.dbHandle.QueryRow(sqlStatement).Scan(&count)
// 	if err != nil {
// 		// TOOD: Handle this
// 		panic(err)
// 	}
// 	return count > 0
// }

// func (wh *HandleT) hasWarehouseData(warehouse warehouseutils.WarehouseT) (bool, error) {
// 	whManager, err := manager.New(wh.destType)
// 	if err != nil {
// 		panic(err)
// 	}

// 	empty, err := whManager.IsEmpty(warehouse)
// 	if err != nil {
// 		return false, err
// 	}
// 	return !empty, nil

// 	// TODO: Change logic to check if warehouse has data in tables
// 	// sqlStatement := fmt.Sprintf(`SELECT count(*) FROM %s WHERE destination_id='%s' AND status='%s'`, warehouseutils.WarehouseUploadsTable, warehouse.Destination.ID, warehouseutils.ExportedDataState)
// 	// var count int
// 	// err := wh.dbHandle.QueryRow(sqlStatement).Scan(&count)
// 	// if err != nil {
// 	// 	// TOOD: Handle this
// 	// 	panic(err)
// 	// }
// 	// return count > 0
// }

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
}

// func (wh *HandleT) initPreLoadUpload(warehouse warehouseutils.WarehouseT) warehouseutils.UploadT {
// 	schema := make(map[string]map[string]string)
// 	// TODO: DRY this code
// 	identityRules := map[string]string{
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_1_type"):  "string",
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_1_value"): "string",
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_2_type"):  "string",
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_2_value"): "string",
// 	}
// 	schema[warehouseutils.ToProviderCase(wh.destType, warehouseutils.IdentityMergeRulesTable)] = identityRules

// 	// add rudder_identity_mappings table
// 	identityMappings := map[string]string{
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_type"):  "string",
// 		warehouseutils.ToProviderCase(wh.destType, "merge_property_value"): "string",
// 		warehouseutils.ToProviderCase(wh.destType, "rudder_id"):            "string",
// 		warehouseutils.ToProviderCase(wh.destType, "updated_at"):           "datetime",
// 	}
// 	schema[warehouseutils.ToProviderCase(wh.destType, warehouseutils.IdentityMappingsTable)] = identityMappings

// 	marshalledSchema, err := json.Marshal(schema)
// 	if err != nil {
// 		panic(err)
// 	}

// 	sqlStatement := fmt.Sprintf(`INSERT INTO %s (source_id, namespace, destination_id, destination_type, status, schema, error, created_at, updated_at, start_staging_file_id, end_staging_file_id, start_load_file_id, end_load_file_id)	VALUES ($1, $2, $3, $4, $5, $6 ,$7, $8, $9, $10, $11, $12, $13) RETURNING id`, warehouseutils.WarehouseUploadsTable)
// 	stmt, err := wh.dbHandle.Prepare(sqlStatement)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer stmt.Close()

// 	now := timeutil.Now()
// 	row := stmt.QueryRow(warehouse.Source.ID, warehouse.Namespace, warehouse.Destination.ID, wh.preLoadDestType(), warehouseutils.WaitingState, marshalledSchema, "{}", now, now, 0, 0, 0, 0)

// 	var uploadID int64
// 	err = row.Scan(&uploadID)
// 	if err != nil {
// 		panic(err)
// 	}

// 	upload := warehouseutils.UploadT{
// 		ID:              uploadID,
// 		Namespace:       warehouse.Namespace,
// 		SourceID:        warehouse.Source.ID,
// 		DestinationID:   warehouse.Destination.ID,
// 		DestinationType: wh.preLoadDestType(),
// 		Status:          warehouseutils.WaitingState,
// 		Schema:          schema,
// 	}

// 	err = wh.initTableUploads(upload, upload.Schema)
// 	if err != nil {
// 		panic(err)
// 	}

// 	return upload
// }

// func (wh *HandleT) preLoadIdentityTables(warehouse warehouseutils.WarehouseT) (upload warehouseutils.UploadT, err error) {

// 	// check for pending preLoads
// 	var found bool
// 	if upload, found = wh.getPendingPreLoad(warehouse); !found {
// 		upload = wh.initPreLoadUpload(warehouse)
// 	}

// 	whManager, err := manager.New(wh.destType)
// 	if err != nil {
// 		panic(err)
// 	}

// 	err = whManager.Process(warehouseutils.ConfigT{
// 		DbHandle:  wh.dbHandle,
// 		Upload:    upload,
// 		Warehouse: warehouse,
// 		Stage:     warehouseutils.PreLoadingIdentities,
// 	})

// 	return
// }
