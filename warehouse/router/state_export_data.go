package router

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/services/alerta"
	"github.com/rudderlabs/rudder-server/warehouse/identity"
	integrationsconfig "github.com/rudderlabs/rudder-server/warehouse/integrations/config"
	schemarepository "github.com/rudderlabs/rudder-server/warehouse/integrations/datalake/schema-repository"
	"github.com/rudderlabs/rudder-server/warehouse/internal/repo"
	"github.com/rudderlabs/rudder-server/warehouse/internal/service/loadfiles/downloader"
	"github.com/rudderlabs/rudder-server/warehouse/logfield"

	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"

	"github.com/rudderlabs/rudder-server/rruntime"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/internal/model"
)

func (job *UploadJob) exportData() error {
	_, currentSucceededTables, err := job.TablesToSkip()
	if err != nil {
		return fmt.Errorf("tables to skip: %w", err)
	}

	var (
		loadErrors        []error
		loadErrorLock     sync.Mutex
		loadFilesTableMap map[tableNameT]bool
	)

	loadFilesTableMap, err = job.getLoadFilesTableMap()
	if err != nil {
		return fmt.Errorf("unable to get load files table map: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(3)

	userTables := []string{job.identifiesTableName(), job.usersTableName()}
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	rruntime.GoForWarehouse(func() {
		defer wg.Done()

		var succeededUserTableCount int
		for _, userTable := range userTables {
			if _, ok := currentSucceededTables[userTable]; ok {
				succeededUserTableCount++
			}
		}
		if succeededUserTableCount >= len(userTables) {
			return
		}
		err := job.exportUserTables(loadFilesTableMap)
		if err != nil {
			loadErrorLock.Lock()
			loadErrors = append(loadErrors, err)
			loadErrorLock.Unlock()
		}
	})

	rruntime.GoForWarehouse(func() {
		defer wg.Done()

		var succeededIdentityTableCount int
		for _, identityTable := range identityTables {
			if _, ok := currentSucceededTables[identityTable]; ok {
				succeededIdentityTableCount++
			}
		}
		if succeededIdentityTableCount >= len(identityTables) {
			return
		}
		err := job.exportIdentities()
		if err != nil {
			loadErrorLock.Lock()
			loadErrors = append(loadErrors, err)
			loadErrorLock.Unlock()
		}
	})

	rruntime.GoForWarehouse(func() {
		defer wg.Done()

		specialTables := make([]string, 0, len(userTables)+len(identityTables))
		specialTables = append(specialTables, userTables...)
		specialTables = append(specialTables, identityTables...)

		err := job.exportRegularTables(specialTables, loadFilesTableMap)
		if err != nil {
			loadErrorLock.Lock()
			loadErrors = append(loadErrors, err)
			loadErrorLock.Unlock()
		}
	})

	wg.Wait()

	if err := job.RefreshPartitions(job.upload.LoadFileStartID, job.upload.LoadFileEndID); err != nil {
		loadErrors = append(loadErrors, fmt.Errorf("refresh partitions: %w", err))
	}

	if len(loadErrors) > 0 {
		return misc.ConcatErrors(loadErrors)
	}
	job.generateUploadSuccessMetrics()

	return nil
}

func (job *UploadJob) identifiesTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentifiesTable)
}

func (job *UploadJob) usersTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.UsersTable)
}

func (job *UploadJob) identityMergeRulesTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMergeRulesTable)
}

func (job *UploadJob) identityMappingsTableName() string {
	return whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMappingsTable)
}

func (job *UploadJob) TablesToSkip() (map[string]model.PendingTableUpload, map[string]model.PendingTableUpload, error) {
	job.pendingTableUploadsOnce.Do(func() {
		job.pendingTableUploads, job.pendingTableUploadsError = job.pendingTableUploadsRepo.PendingTableUploads(
			job.ctx,
			job.upload.Namespace,
			job.upload.ID,
			job.upload.DestinationID,
		)
	})

	if job.pendingTableUploadsError != nil {
		return nil, nil, fmt.Errorf("pending table uploads: %w", job.pendingTableUploadsError)
	}

	var (
		previouslyFailedTableMap   = make(map[string]model.PendingTableUpload)
		currentlySucceededTableMap = make(map[string]model.PendingTableUpload)
	)

	for _, pendingTableUpload := range job.pendingTableUploads {
		if pendingTableUpload.UploadID < job.upload.ID && pendingTableUpload.Status == model.TableUploadExportingFailed {
			previouslyFailedTableMap[pendingTableUpload.TableName] = pendingTableUpload
		}
		if pendingTableUpload.UploadID == job.upload.ID && pendingTableUpload.Status == model.TableUploadExported { // Current upload and table upload succeeded
			currentlySucceededTableMap[pendingTableUpload.TableName] = pendingTableUpload
		}
	}
	return previouslyFailedTableMap, currentlySucceededTableMap, nil
}

func (job *UploadJob) getLoadFilesTableMap() (loadFilesMap map[tableNameT]bool, err error) {
	tableName, err := job.loadFilesRepo.DistinctTableName(
		job.ctx,
		job.warehouse.Source.ID,
		job.warehouse.Destination.ID,
		job.upload.LoadFileStartID,
		job.upload.LoadFileEndID,
	)
	if err != nil {
		return nil, fmt.Errorf("getting load files table name: %w", err)
	}

	tablesMap := lo.SliceToMap(tableName, func(tName string) (tableNameT, bool) {
		return tableNameT(tName), true
	})
	return tablesMap, nil
}

func (job *UploadJob) exportUserTables(loadFilesTableMap map[tableNameT]bool) (err error) {
	uploadSchema := job.upload.UploadSchema
	if _, ok := uploadSchema[job.identifiesTableName()]; ok {
		defer job.stats.userTablesLoadTime.RecordDuration()()
		var loadErrors []error
		loadErrors, err = job.loadUserTables(loadFilesTableMap)
		if err != nil {
			return
		}

		if len(loadErrors) > 0 {
			err = misc.ConcatErrors(loadErrors)
			return
		}
	}
	return
}

func (job *UploadJob) loadUserTables(loadFilesTableMap map[tableNameT]bool) ([]error, error) {
	var hasLoadFiles bool
	userTables := []string{job.identifiesTableName(), job.usersTableName()}

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{}, fmt.Errorf("tables to skip: %w", err)
	}

	for _, tName := range userTables {
		if prevJobStatus, ok := previouslyFailedTables[tName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tName, prevJobStatus.UploadID, prevJobStatus.Error)
			return []error{skipError}, nil
		}
	}
	for _, tName := range userTables {
		if _, ok := currentJobSucceededTables[tName]; ok {
			continue
		}
		hasLoadFiles = loadFilesTableMap[tableNameT(tName)]
		if hasLoadFiles {
			// There is at least one table to load
			break
		}
	}

	if !hasLoadFiles {
		return []error{}, nil
	}

	// Load all user tables
	status := model.TableUploadExecuting
	lastExecTime := job.now()
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	alteredIdentitySchema, err := job.updateSchema(job.identifiesTableName())
	if err != nil {
		status := model.TableUploadUpdatingSchemaFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.identifiesTableName(), repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return job.processLoadTableResponse(map[string]error{job.identifiesTableName(): err})
	}
	var alteredUserSchema bool
	if _, ok := job.upload.UploadSchema[job.usersTableName()]; ok {
		status := model.TableUploadExecuting
		lastExecTime := job.now()
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		alteredUserSchema, err = job.updateSchema(job.usersTableName())
		if err != nil {
			status = model.TableUploadUpdatingSchemaFailed
			errorsString := misc.QuoteLiteral(err.Error())
			_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, job.usersTableName(), repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
			return job.processLoadTableResponse(map[string]error{job.usersTableName(): err})
		}
	}

	// Skip loading user tables if identifies table schema is not present
	if identifiesSchema := job.GetTableSchemaInUpload(job.identifiesTableName()); len(identifiesSchema) == 0 {
		return []error{}, nil
	}

	errorMap := job.whManager.LoadUserTables(job.ctx)

	if alteredIdentitySchema || alteredUserSchema {
		job.logger.Infof("loadUserTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx)
	}
	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) updateSchema(tName string) (alteredSchema bool, err error) {
	tableSchemaDiff, err := job.schemaHandle.TableSchemaDiff(job.ctx, tName, job.GetTableSchemaInUpload(tName))
	if err != nil {
		return false, fmt.Errorf("table schema diff: %w", err)
	}
	if tableSchemaDiff.Exists {
		err = job.UpdateTableSchema(tName, tableSchemaDiff)
		if err != nil {
			return
		}

		err = job.schemaHandle.UpdateWarehouseTableSchema(job.ctx, tName, tableSchemaDiff.UpdatedSchema)
		if err != nil {
			return false, fmt.Errorf("update warehouse table schema: %w", err)
		}
		alteredSchema = true
	}
	return
}

func (job *UploadJob) UpdateTableSchema(tName string, tableSchemaDiff whutils.TableSchemaDiff) (err error) {
	job.logger.Infof(`[WH]: Starting schema update for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	if tableSchemaDiff.TableToBeCreated {
		err = job.whManager.CreateTable(job.ctx, tName, tableSchemaDiff.ColumnMap)
		if err != nil {
			job.logger.Errorf("Error creating table %s on namespace: %s, error: %v", tName, job.warehouse.Namespace, err)
			return err
		}
		job.stats.tablesAdded.Increment()
		return nil
	}

	if err = job.addColumnsToWarehouse(job.ctx, tName, tableSchemaDiff.ColumnMap); err != nil {
		return fmt.Errorf("adding columns to warehouse: %w", err)
	}

	if err = job.alterColumnsToWarehouse(job.ctx, tName, tableSchemaDiff.AlteredColumnMap); err != nil {
		return fmt.Errorf("altering columns to warehouse: %w", err)
	}

	return nil
}

func (job *UploadJob) alterColumnsToWarehouse(ctx context.Context, tName string, columnsMap model.TableSchema) error {
	if job.config.disableAlter {
		job.logger.Debugw("skipping alter columns to warehouse",
			logfield.TableName, tName,
			"columns", columnsMap,
		)
		return nil
	}
	var responseToAlerta []model.AlterTableResponse
	var errs []error

	for columnName, columnType := range columnsMap {
		res, err := job.whManager.AlterColumn(ctx, tName, columnName, columnType)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if res.IsDependent {
			responseToAlerta = append(responseToAlerta, res)
			continue
		}

		job.logger.Infof(`
			[WH]: Altered column %s of type %s in table %s in namespace %s of destination %s:%s
		`,
			columnName,
			columnType,
			tName,
			job.warehouse.Namespace,
			job.warehouse.Type,
			job.warehouse.Destination.ID,
		)
	}

	if len(responseToAlerta) > 0 {
		queries := make([]string, len(responseToAlerta))
		for i, res := range responseToAlerta {
			queries[i] = res.Query
		}

		query := strings.Join(queries, "\n")
		job.logger.Infof("altering dependent columns: %s", query)

		err := job.alertSender.SendAlert(ctx, "warehouse-column-changes",
			alerta.SendAlertOpts{
				Severity:    alerta.SeverityCritical,
				Priority:    alerta.PriorityP1,
				Environment: alerta.PROXYMODE,
				Tags: alerta.Tags{
					"destID":      job.upload.DestinationID,
					"destType":    job.upload.DestinationType,
					"workspaceID": job.upload.WorkspaceID,
					"query":       query,
				},
			},
		)
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return misc.ConcatErrors(errs)
	}

	return nil
}

func (job *UploadJob) addColumnsToWarehouse(ctx context.Context, tName string, columnsMap model.TableSchema) (err error) {
	job.logger.Infof(`[WH]: Adding columns for table %s in namespace %s of destination %s:%s`, tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var columnsToAdd []whutils.ColumnInfo
	for columnName, columnType := range columnsMap {
		columnsToAdd = append(columnsToAdd, whutils.ColumnInfo{Name: columnName, Type: columnType})
	}

	chunks := lo.Chunk(columnsToAdd, job.config.columnsBatchSize)
	for _, chunk := range chunks {
		err = job.whManager.AddColumns(ctx, tName, chunk)
		if err != nil {
			err = fmt.Errorf("failed to add columns for table %s in namespace %s of destination %s:%s with error: %w", tName, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID, err)
			break
		}

		job.stats.columnsAdded.Count(len(chunk))
	}
	return err
}

func (job *UploadJob) processLoadTableResponse(errorMap map[string]error) (errors []error, tableUploadErr error) {
	for tName, loadErr := range errorMap {
		// TODO: set last_exec_time
		if loadErr != nil {
			errors = append(errors, loadErr)
			errorsString := misc.QuoteLiteral(loadErr.Error())
			status := model.TableUploadExportingFailed
			tableUploadErr = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
				Error:  &errorsString,
			})
		} else {
			status := model.TableUploadExported
			tableUploadErr = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
				Status: &status,
			})
			if tableUploadErr == nil {
				// Since load is successful, we assume all events in load files are uploaded
				tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
				if queryErr == nil {
					job.recordTableLoad(tName, tableUpload.TotalEvents)
				}
			}
		}

		if tableUploadErr != nil {
			break
		}

	}
	return errors, tableUploadErr
}

func (job *UploadJob) exportIdentities() (err error) {
	// Load Identities if enabled
	uploadSchema := job.upload.UploadSchema
	if whutils.IDResolutionEnabled() && slices.Contains(whutils.IdentityEnabledWarehouses, job.warehouse.Type) {
		if _, ok := uploadSchema[job.identityMergeRulesTableName()]; ok {
			defer job.stats.identityTablesLoadTime.RecordDuration()()

			var loadErrors []error
			loadErrors, err = job.loadIdentityTables(false)
			if err != nil {
				return
			}

			if len(loadErrors) > 0 {
				err = misc.ConcatErrors(loadErrors)
				return
			}
		}
	}
	return
}

func (job *UploadJob) loadIdentityTables(populateHistoricIdentities bool) (loadErrors []error, tableUploadErr error) {
	job.logger.Infof(`[WH]: Starting load for identity tables in namespace %s of destination %s:%s`, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)
	identityTables := []string{job.identityMergeRulesTableName(), job.identityMappingsTableName()}

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{}, fmt.Errorf("tables to skip: %w", err)
	}

	for _, tableName := range identityTables {
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tableName, prevJobStatus.UploadID, prevJobStatus.Error)
			return []error{skipError}, nil
		}
	}

	errorMap := make(map[string]error)
	// var generated bool
	if generated, _ := job.areIdentityTablesLoadFilesGenerated(job.ctx); !generated {
		if err := job.resolveIdentities(populateHistoricIdentities); err != nil {
			job.logger.Errorf(` ID Resolution operation failed: %v`, err)
			errorMap[job.identityMergeRulesTableName()] = err
			return job.processLoadTableResponse(errorMap)
		}
	}

	var alteredSchema bool
	for _, tableName := range identityTables {
		if _, loaded := currentJobSucceededTables[tableName]; loaded {
			continue
		}

		errorMap[tableName] = nil

		tableSchemaDiff, err := job.schemaHandle.TableSchemaDiff(job.ctx, tableName, job.GetTableSchemaInUpload(tableName))
		if err != nil {
			return nil, fmt.Errorf("table schema diff: %w", err)
		}
		if tableSchemaDiff.Exists {
			err := job.UpdateTableSchema(tableName, tableSchemaDiff)
			if err != nil {
				status := model.TableUploadUpdatingSchemaFailed
				errorsString := misc.QuoteLiteral(err.Error())
				_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
					Error:  &errorsString,
				})
				errorMap := map[string]error{tableName: err}
				return job.processLoadTableResponse(errorMap)
			}
			err = job.schemaHandle.UpdateWarehouseTableSchema(job.ctx, tableName, tableSchemaDiff.UpdatedSchema)
			if err != nil {
				return nil, fmt.Errorf("update warehouse table schema: %w", err)
			}

			status := model.TableUploadUpdatedSchema
			_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
				Status: &status,
			})
			alteredSchema = true
		}

		status := model.TableUploadExecuting
		lastExecTime := job.now()
		err = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
			Status:       &status,
			LastExecTime: &lastExecTime,
		})
		if err != nil {
			errorMap[tableName] = err
			break
		}

		switch tableName {
		case job.identityMergeRulesTableName():
			err = job.whManager.LoadIdentityMergeRulesTable(job.ctx)
		case job.identityMappingsTableName():
			err = job.whManager.LoadIdentityMappingsTable(job.ctx)
		}

		if err != nil {
			errorMap[tableName] = err
			break
		}
	}

	if alteredSchema {
		job.logger.Infof("loadIdentityTables: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx) // TODO check error
	}

	return job.processLoadTableResponse(errorMap)
}

func (job *UploadJob) areIdentityTablesLoadFilesGenerated(ctx context.Context) (bool, error) {
	var (
		mergeRulesTable = whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMergeRulesTable)
		mappingsTable   = whutils.ToProviderCase(job.warehouse.Type, whutils.IdentityMappingsTable)
		tu              model.TableUpload
		err             error
	)

	if tu, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, mergeRulesTable); err != nil {
		return false, fmt.Errorf("table upload not found for merge rules table: %w", err)
	}
	if tu.Location == "" {
		return false, fmt.Errorf("merge rules location not found: %w", err)
	}
	if tu, err = job.tableUploadsRepo.GetByUploadIDAndTableName(ctx, job.upload.ID, mappingsTable); err != nil {
		return false, fmt.Errorf("table upload not found for mappings table: %w", err)
	}
	if tu.Location == "" {
		return false, fmt.Errorf("mappings location not found: %w", err)
	}
	return true, nil
}

func (job *UploadJob) resolveIdentities(populateHistoricIdentities bool) (err error) {
	idr := identity.New(
		job.warehouse,
		job.db,
		job,
		job.upload.ID,
		job.whManager,
		downloader.NewDownloader(&job.warehouse, job, 8),
		job.encodingFactory,
	)
	if populateHistoricIdentities {
		return idr.ResolveHistoricIdentities(job.ctx)
	}
	return idr.Resolve(job.ctx)
}

func (job *UploadJob) exportRegularTables(specialTables []string, loadFilesTableMap map[tableNameT]bool) (err error) {
	//[]string{job.identifiesTableName(), job.usersTableName(), job.identityMergeRulesTableName(), job.identityMappingsTableName()}
	// Export all other tables
	defer job.stats.otherTablesLoadTime.RecordDuration()()

	loadErrors := job.loadAllTablesExcept(specialTables, loadFilesTableMap)

	if len(loadErrors) > 0 {
		err = misc.ConcatErrors(loadErrors)
		return
	}

	return
}

func (job *UploadJob) loadAllTablesExcept(skipLoadForTables []string, loadFilesTableMap map[tableNameT]bool) []error {
	maxParallelLoadsMap := integrationsconfig.MaxParallelLoadsMap(job.conf)

	uploadSchema := job.upload.UploadSchema
	var parallelLoads int
	var ok bool
	if parallelLoads, ok = maxParallelLoadsMap[job.warehouse.Type]; !ok {
		parallelLoads = 1
	}

	if k, ok := job.config.maxParallelLoadsWorkspaceIDs[strings.ToLower(job.warehouse.WorkspaceID)]; ok {
		if load, ok := k.(float64); ok {
			parallelLoads = int(load)
		}
	}

	job.logger.Infof(`[WH]: Running %d parallel loads in namespace %s of destination %s:%s`, parallelLoads, job.warehouse.Namespace, job.warehouse.Type, job.warehouse.Destination.ID)

	var loadErrors []error
	var loadErrorLock sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(uploadSchema))

	var alteredSchemaInAtLeastOneTable atomic.Bool
	concurrencyGuard := make(chan struct{}, parallelLoads)

	var (
		err                       error
		previouslyFailedTables    map[string]model.PendingTableUpload
		currentJobSucceededTables map[string]model.PendingTableUpload
	)
	if previouslyFailedTables, currentJobSucceededTables, err = job.TablesToSkip(); err != nil {
		return []error{fmt.Errorf("tables to skip: %w", err)}
	}

	for tableName := range uploadSchema {
		if slices.Contains(skipLoadForTables, tableName) {
			wg.Done()
			continue
		}
		if _, ok := currentJobSucceededTables[tableName]; ok {
			wg.Done()
			continue
		}
		if prevJobStatus, ok := previouslyFailedTables[tableName]; ok {
			skipError := fmt.Errorf("skipping table %s because it previously failed to load in an earlier job: %d with error: %s", tableName, prevJobStatus.UploadID, prevJobStatus.Error)
			loadErrors = append(loadErrors, skipError)
			wg.Done()
			continue
		}
		hasLoadFiles := loadFilesTableMap[tableNameT(tableName)]
		if !hasLoadFiles {
			if slices.Contains(alwaysMarkExported, strings.ToLower(tableName)) {
				status := model.TableUploadExported
				_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tableName, repo.TableUploadSetOptions{
					Status: &status,
				})
			}
			wg.Done()
			continue
		}
		tableName := tableName
		concurrencyGuard <- struct{}{}
		rruntime.GoForWarehouse(func() {
			alteredSchema, err := job.loadTable(tableName)
			if alteredSchema {
				alteredSchemaInAtLeastOneTable.Store(true)
			}
			if err != nil {
				loadErrorLock.Lock()
				loadErrors = append(loadErrors, err)
				loadErrorLock.Unlock()
			}

			<-concurrencyGuard
			wg.Done()
		})
	}
	wg.Wait()

	if alteredSchemaInAtLeastOneTable.Load() {
		job.logger.Infof("loadAllTablesExcept: schema changed - updating local schema for %s", job.warehouse.Identifier)
		_ = job.schemaHandle.UpdateLocalSchemaWithWarehouse(job.ctx) // TODO check error
	}

	return loadErrors
}

func (job *UploadJob) loadTable(tName string) (bool, error) {
	alteredSchema, err := job.updateSchema(tName)
	if err != nil {
		status := model.TableUploadUpdatingSchemaFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return alteredSchema, fmt.Errorf("update schema: %w", err)
	}

	job.logger.Infow("starting load for table", logfield.TableName, tName)

	status := model.TableUploadExecuting
	lastExecTime := job.now()
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
		Status:       &status,
		LastExecTime: &lastExecTime,
	})

	loadTableStat, err := job.whManager.LoadTable(job.ctx, tName)
	if err != nil {
		status := model.TableUploadExportingFailed
		errorsString := misc.QuoteLiteral(err.Error())
		_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
			Status: &status,
			Error:  &errorsString,
		})
		return alteredSchema, fmt.Errorf("load table: %w", err)
	}
	if loadTableStat.RowsUpdated > 0 {
		job.statsFactory.NewTaggedStat("dedup_rows", stats.CountType, stats.Tags{
			"sourceID":       job.warehouse.Source.ID,
			"sourceType":     job.warehouse.Source.SourceDefinition.Name,
			"sourceCategory": job.warehouse.Source.SourceDefinition.Category,
			"destID":         job.warehouse.Destination.ID,
			"destType":       job.warehouse.Destination.DestinationDefinition.Name,
			"workspaceId":    job.warehouse.WorkspaceID,
			"tableName":      whutils.TableNameForStats(tName),
		}).Count(int(loadTableStat.RowsUpdated))
	}

	tableUpload, errEventCount := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
	if errEventCount != nil {
		return alteredSchema, fmt.Errorf("get table upload: %w", errEventCount)
	}

	tags := []whutils.Tag{
		{Name: "tableName", Value: whutils.TableNameForStats(tName)},
		{Name: "sourceCategory", Value: job.warehouse.Source.SourceDefinition.Category},
	}
	job.gaugeStat(`post_load_table_rows_estimate`, tags...).Gauge(int(tableUpload.TotalEvents))
	job.gaugeStat(`post_load_table_rows`, tags...).Gauge(int(loadTableStat.RowsInserted))

	status = model.TableUploadExported
	_ = job.tableUploadsRepo.Set(job.ctx, job.upload.ID, tName, repo.TableUploadSetOptions{
		Status: &status,
	})
	tableUpload, queryErr := job.tableUploadsRepo.GetByUploadIDAndTableName(job.ctx, job.upload.ID, tName)
	if queryErr == nil {
		job.recordTableLoad(tName, tableUpload.TotalEvents)
	}

	job.columnCountStat(tName)

	return alteredSchema, nil
}

// columnCountStat sent the column count for a table to statsd
// skip sending for S3_DATALAKE, GCS_DATALAKE, AZURE_DATALAKE
func (job *UploadJob) columnCountStat(tableName string) {
	var (
		columnCountLimit int
		ok               bool
	)

	switch job.warehouse.Type {
	case whutils.S3Datalake, whutils.GCSDatalake, whutils.AzureDatalake:
		return
	}

	columnCountLimitMap := integrationsconfig.ColumnCountLimitMap(job.conf)

	if columnCountLimit, ok = columnCountLimitMap[job.warehouse.Type]; !ok {
		return
	}

	tags := []whutils.Tag{
		{Name: "tableName", Value: whutils.TableNameForStats(tableName)},
	}
	currentColumnsCount, err := job.schemaHandle.GetColumnsCountInWarehouseSchema(job.ctx, tableName)
	if err != nil {
		job.logger.Warnn("Getting column count in warehouse schema",
			logger.NewStringField(logfield.TableName, tableName),
			obskit.Error(err),
		)
		return
	}

	job.gaugeStat(`warehouse_load_table_column_count`, tags...).Gauge(currentColumnsCount)
	job.gaugeStat(`warehouse_load_table_column_limit`, tags...).Gauge(columnCountLimit)
}

func (job *UploadJob) RefreshPartitions(loadFileStartID, loadFileEndID int64) error {
	if !slices.Contains(whutils.TimeWindowDestinations, job.upload.DestinationType) {
		return nil
	}

	var (
		repository schemarepository.SchemaRepository
		err        error
	)

	if repository, err = schemarepository.NewSchemaRepository(job.conf, job.logger, job.warehouse, job); err != nil {
		return fmt.Errorf("create schema repository: %w", err)
	}

	// Refresh partitions if exists
	for tableName := range job.upload.UploadSchema {
		loadFiles, err := job.GetLoadFilesMetadata(job.ctx, whutils.GetLoadFilesOptions{
			Table:   tableName,
			StartID: loadFileStartID,
			EndID:   loadFileEndID,
		})
		if err != nil {
			return fmt.Errorf("get load files metadata: %w", err)
		}
		batches := lo.Chunk(loadFiles, job.config.refreshPartitionBatchSize)
		for _, batch := range batches {
			if err = repository.RefreshPartitions(job.ctx, tableName, batch); err != nil {
				return fmt.Errorf("refresh partitions: %w", err)
			}
		}
	}
	return nil
}
