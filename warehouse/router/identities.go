package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	"github.com/rudderlabs/rudder-server/warehouse/internal/model"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
	warehouseutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	populatingHistoricIdentitiesProgressMap     map[string]bool
	populatingHistoricIdentitiesProgressMapLock sync.RWMutex
	populatedHistoricIdentitiesMap              map[string]bool
	populatedHistoricIdentitiesMapLock          sync.RWMutex
)

func init() {
	populatingHistoricIdentitiesProgressMap = map[string]bool{}
	populatedHistoricIdentitiesMap = map[string]bool{}
}

func uniqueWarehouseNamespaceString(warehouse model.Warehouse) string {
	return fmt.Sprintf(`namespace:%s:destination:%s`, warehouse.Namespace, warehouse.Destination.ID)
}

func isDestHistoricIdentitiesPopulated(warehouse model.Warehouse) bool {
	populatedHistoricIdentitiesMapLock.RLock()
	if populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatedHistoricIdentitiesMapLock.RUnlock()
		return true
	}
	populatedHistoricIdentitiesMapLock.RUnlock()
	return false
}

func setDestHistoricIdentitiesPopulated(warehouse model.Warehouse) {
	populatedHistoricIdentitiesMapLock.Lock()
	populatedHistoricIdentitiesMap[uniqueWarehouseNamespaceString(warehouse)] = true
	populatedHistoricIdentitiesMapLock.Unlock()
}

func setDestHistoricIdentitiesPopulateInProgress(warehouse model.Warehouse, starting bool) {
	populatingHistoricIdentitiesProgressMapLock.Lock()
	if starting {
		populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] = true
	} else {
		delete(populatingHistoricIdentitiesProgressMap, uniqueWarehouseNamespaceString(warehouse))
	}
	populatingHistoricIdentitiesProgressMapLock.Unlock()
}

func isDestHistoricIdentitiesPopulateInProgress(warehouse model.Warehouse) bool {
	populatingHistoricIdentitiesProgressMapLock.RLock()
	if populatingHistoricIdentitiesProgressMap[uniqueWarehouseNamespaceString(warehouse)] {
		populatingHistoricIdentitiesProgressMapLock.RUnlock()
		return true
	}
	populatingHistoricIdentitiesProgressMapLock.RUnlock()
	return false
}

func (r *Router) getPendingPopulateIdentitiesLoad(warehouse model.Warehouse) (upload model.Upload, found bool) {
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
		r.populateHistoricIdentitiesDestType(),
		model.ExportedData,
		model.Aborted,
	)

	var schema json.RawMessage
	err := r.db.QueryRow(sqlStatement).Scan(
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
	if errors.Is(err, sql.ErrNoRows) {
		return
	}
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	found = true
	upload.UploadSchema = warehouseutils.JSONSchemaToMap(schema)
	return
}

func (r *Router) populateHistoricIdentitiesDestType() string {
	return r.destType + "_IDENTITY_PRE_LOAD"
}

func (r *Router) hasLocalIdentityData(warehouse model.Warehouse) (exists bool) {
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
	err := r.db.QueryRow(sqlStatement).Scan(&exists)
	if err != nil {
		// TODO: Handle this
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
	return
}

func (r *Router) hasWarehouseData(ctx context.Context, warehouse model.Warehouse) (bool, error) {
	whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
	if err != nil {
		panic(err)
	}

	empty, err := whManager.IsEmpty(ctx, warehouse)
	if err != nil {
		return false, err
	}
	return !empty, nil
}

func (r *Router) setupIdentityTables(ctx context.Context, warehouse model.Warehouse) {
	var name sql.NullString
	sqlStatement := fmt.Sprintf(`SELECT to_regclass('%s')`, warehouseutils.IdentityMappingsTableName(warehouse))
	err := r.db.QueryRow(sqlStatement).Scan(&name)
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

	_, err = r.db.ExecContext(ctx, sqlStatement)
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

	_, err = r.db.ExecContext(ctx, sqlStatement)
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

	_, err = r.db.ExecContext(ctx, sqlStatement)
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

	_, err = r.db.ExecContext(ctx, sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}

	sqlStatement = fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS rudder_id_index_%[1]s ON %[1]s (rudder_id);
`,
		warehouseutils.IdentityMappingsTableName(warehouse),
	)

	_, err = r.db.ExecContext(ctx, sqlStatement)
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

	_, err = r.db.ExecContext(ctx, sqlStatement)
	if err != nil {
		panic(fmt.Errorf("Query: %s\nfailed with Error : %w", sqlStatement, err))
	}
}

func (r *Router) initPrePopulateDestIdentitiesUpload(warehouse model.Warehouse) model.Upload {
	schema := make(model.Schema)
	// TODO: DRY this code
	identityRules := model.TableSchema{
		warehouseutils.ToProviderCase(r.destType, "merge_property_1_type"):  "string",
		warehouseutils.ToProviderCase(r.destType, "merge_property_1_value"): "string",
		warehouseutils.ToProviderCase(r.destType, "merge_property_2_type"):  "string",
		warehouseutils.ToProviderCase(r.destType, "merge_property_2_value"): "string",
	}
	schema[warehouseutils.ToProviderCase(r.destType, warehouseutils.IdentityMergeRulesTable)] = identityRules

	// add rudder_identity_mappings table
	identityMappings := model.TableSchema{
		warehouseutils.ToProviderCase(r.destType, "merge_property_type"):  "string",
		warehouseutils.ToProviderCase(r.destType, "merge_property_value"): "string",
		warehouseutils.ToProviderCase(r.destType, "rudder_id"):            "string",
		warehouseutils.ToProviderCase(r.destType, "updated_at"):           "datetime",
	}
	schema[warehouseutils.ToProviderCase(r.destType, warehouseutils.IdentityMappingsTable)] = identityMappings

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

	now := r.now()
	row := r.db.QueryRow(
		sqlStatement,
		warehouse.Source.ID,
		warehouse.Namespace,
		warehouse.WorkspaceID,
		warehouse.Destination.ID,
		r.populateHistoricIdentitiesDestType(),
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
		DestinationType: r.populateHistoricIdentitiesDestType(),
		Status:          model.Waiting,
		UploadSchema:    schema,
	}
	return upload
}

func (*Router) setFailedStat(warehouse model.Warehouse, err error) {
	if err == nil {
		return
	}
	warehouseutils.DestStat(stats.CountType, "failed_uploads", warehouse.Identifier).Count(1)
}

func (r *Router) populateHistoricIdentities(ctx context.Context, warehouse model.Warehouse) {
	if isDestHistoricIdentitiesPopulated(warehouse) || isDestHistoricIdentitiesPopulateInProgress(warehouse) {
		return
	}

	r.setDestInProgress(warehouse, 0)
	setDestHistoricIdentitiesPopulateInProgress(warehouse, true)
	rruntime.GoForWarehouse(func() {
		var err error
		defer r.removeDestInProgress(warehouse, 0)
		defer setDestHistoricIdentitiesPopulateInProgress(warehouse, false)
		defer setDestHistoricIdentitiesPopulated(warehouse)
		defer r.setFailedStat(warehouse, err)

		// check for pending loads (populateHistoricIdentities)
		var (
			hasPendingLoad bool
			upload         model.Upload
		)

		upload, hasPendingLoad = r.getPendingPopulateIdentitiesLoad(warehouse)

		if hasPendingLoad {
			r.logger.Infof("[WH]: Found pending load (populateHistoricIdentities) for %s:%s", r.destType, warehouse.Destination.ID)
		} else {
			if r.hasLocalIdentityData(warehouse) {
				r.logger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentities) for %s:%s as data exists locally", r.destType, warehouse.Destination.ID)
				return
			}
			var hasData bool
			hasData, err = r.hasWarehouseData(ctx, warehouse)
			if err != nil {
				r.logger.Errorf(`[WH]: Error checking for data in %s:%s:%s, err: %s`, r.destType, warehouse.Destination.ID, warehouse.Destination.Name, err.Error())
				return
			}
			if !hasData {
				r.logger.Infof("[WH]: Skipping identity tables load (populateHistoricIdentities) for %s:%s as warehouse does not have any data", r.destType, warehouse.Destination.ID)
				return
			}
			r.logger.Infof("[WH]: Did not find local identity tables..")
			r.logger.Infof("[WH]: Generating identity tables based on data in warehouse %s:%s", r.destType, warehouse.Destination.ID)
			upload = r.initPrePopulateDestIdentitiesUpload(warehouse)
		}

		whManager, err := manager.New(r.destType, r.conf, r.logger, r.statsFactory)
		if err != nil {
			panic(err)
		}

		job := r.uploadJobFactory.NewUploadJob(ctx, &model.UploadJob{
			Upload:    upload,
			Warehouse: warehouse,
		},
			whManager,
		)

		tableUploadsCreated, tableUploadsErr := job.tableUploadsRepo.ExistsForUploadID(ctx, job.upload.ID)
		if tableUploadsErr != nil {
			r.logger.Warnw("table uploads exists",
				logfield.UploadJobID, job.upload.ID,
				logfield.SourceID, job.upload.SourceID,
				logfield.DestinationID, job.upload.DestinationID,
				logfield.DestinationType, job.upload.DestinationType,
				logfield.WorkspaceID, job.upload.WorkspaceID,
				logfield.Error, tableUploadsErr.Error(),
			)
			return
		}
		if !tableUploadsCreated {
			err := job.createTableUploads()
			if err != nil {
				// TODO: Handle error / Retry
				r.logger.Error("[WH]: Error creating records in wh_table_uploads", err)
			}
		}

		whManager.SetConnectionTimeout(warehouseutils.GetConnectionTimeout(
			r.destType, warehouse.Destination.ID,
		))
		err = whManager.Setup(ctx, job.warehouse, job)
		if err != nil {
			_, _ = job.setUploadError(err, model.Aborted)
			return
		}
		defer whManager.Cleanup(ctx)

		err = job.schemaHandle.FetchSchemaFromWarehouse(ctx, whManager)
		if err != nil {
			r.logger.Errorf(`[WH]: Failed fetching schema from warehouse: %v`, err)
			_, _ = job.setUploadError(err, model.Aborted)
			return
		}

		_ = job.setUploadStatus(UploadStatusOpts{Status: inProgressState(model.ExportedData)})
		loadErrors, err := job.loadIdentityTables(true)
		if err != nil {
			r.logger.Errorf(`[WH]: Identity table upload errors: %v`, err)
		}
		if len(loadErrors) > 0 {
			_, _ = job.setUploadError(misc.ConcatErrors(loadErrors), model.Aborted)
			return
		}
		_ = job.setUploadStatus(UploadStatusOpts{Status: model.ExportedData})
	})
}
