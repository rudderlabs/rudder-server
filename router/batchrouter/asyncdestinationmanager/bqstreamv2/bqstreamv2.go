package bqstreamv2

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// defaultChunkSizeBytes bounds a single AppendRows request. The Storage Write
// API rejects requests over 10MB, and an oversized chunk would fail on every
// retry, so stay well under the limit (row sizes are accounted as conservative
// upper bounds, leaving additional headroom).
const defaultChunkSizeBytes int64 = 8 * bytesize.MB

var (
	usersTableName    = whutils.ToProviderCase(whutils.BQStreamV2, whutils.UsersTable)
	discardsTableName = whutils.ToProviderCase(whutils.BQStreamV2, whutils.DiscardsTable)

	discardsTableSchema = lo.MapEntries(whutils.DiscardsSchema, func(columnName, columnType string) (string, string) {
		return whutils.ToProviderCase(whutils.BQStreamV2, columnName), columnType
	})
)

// NewManager creates the BQSTREAM_V2 async destination manager, which
// streams events into BigQuery through the Storage Write API.
func NewManager(
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
) *Manager {
	m := &Manager{
		appConfig: conf,
		logger: log.Child("bqstreamv2").Withn(
			obskit.WorkspaceID(destination.WorkspaceID),
			obskit.DestinationID(destination.ID),
			obskit.DestinationType(destination.DestinationDefinition.Name),
			logger.NewStringField("id", uuid.New().String()),
		),
		statsFactory:  statsFactory,
		destination:   destination,
		streamWriters: make(map[string]tableStreamWriter),
		streamWriterFactory: &streamWriterFactoryImpl{
			maxInflightRequests: conf.GetIntVar(1000, 1, "BQStreamV2.maxInflightRequests"),
			maxInflightBytes:    conf.GetInt64Var(100*bytesize.MB, bytesize.B, "BQStreamV2.maxInflightBytes"),
		},
		now: timeutil.Now,
	}

	m.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "BQStreamV2.maxBufferCapacity")
	m.config.tableWorkers = conf.GetReloadableIntVar(25, 1, "BQStreamV2.tableWorkers")
	m.config.maxChunkBytes = conf.GetReloadableInt64Var(defaultChunkSizeBytes, bytesize.B, "BQStreamV2.maxChunkBytes")
	m.config.schemaCacheTTL = conf.GetReloadableDurationVar(5, time.Minute, "BQStreamV2.schemaCacheTTL")
	m.schemaCache = NewTableSchemaCache(m.config.schemaCacheTTL.Load())

	tags := stats.Tags{
		"module":        "batch_router",
		"workspaceId":   destination.WorkspaceID,
		"destType":      destination.DestinationDefinition.Name,
		"destinationId": destination.ID,
	}
	m.stats.jobs.succeeded = statsFactory.NewTaggedStat("bqstream_v2_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "succeeded",
	}))
	m.stats.jobs.failed = statsFactory.NewTaggedStat("bqstream_v2_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "failed",
	}))
	m.stats.jobs.aborted = statsFactory.NewTaggedStat("bqstream_v2_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "aborted",
	}))
	m.stats.discards = statsFactory.NewTaggedStat("bqstream_v2_discards", stats.CountType, tags)
	m.stats.duplicateEventsInBatch = statsFactory.NewTaggedStat("bqstream_v2_duplicate_events", stats.CountType, lo.Assign(tags, stats.Tags{
		"reason": "batch",
	}))

	m.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
		return m.createIntegrationManager(ctx, cfg)
	}

	return m
}

// Transform wraps the job's transformed payload with its job ID for the async
// file.
func (m *Manager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

// Upload reads the async file, groups and chunks events per table, reconciles
// the warehouse schema (create dataset/tables, add columns), and streams the
// rows table by table with bounded concurrency, classifying failures into
// retryable vs aborted per table.
func (m *Manager) Upload(_ context.Context, asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.logger.Infon("Uploading data to BQStream V2 destination")

	ctx := context.Background()

	var cfg destConfig
	if err := cfg.Decode(asyncDest.Destination.Config); err != nil {
		m.logger.Warnn("Failed to decode destination config",
			obskit.Error(err),
		)
		return m.abortJobs(asyncDest, fmt.Errorf("failed to decode destination config: %w", err).Error())
	}

	events, err := m.eventsFromFile(asyncDest.FileName, asyncDest.Count)
	if err != nil {
		m.logger.Warnn("Failed to read events from file",
			obskit.Error(err),
		)
		return m.abortJobs(asyncDest, fmt.Errorf("failed to read events from file: %w", err).Error())
	}
	m.logger.Infon("Read events from file",
		logger.NewIntField("events", int64(len(events))),
		logger.NewIntField("size", int64(asyncDest.Size)),
	)

	groupedAndChunkedEvents := m.groupAndChunkEvents(events)
	eventsTables := lo.Keys(groupedAndChunkedEvents)

	if err := m.refreshSchemaCacheIfNeeded(ctx, cfg, groupedAndChunkedEvents); err != nil {
		m.logger.Warnn("Failed to refresh schema cache", obskit.Error(err))

		return m.failOrAbortJobs(asyncDest, err)
	}

	// The discards table is shared by all table workers, so it is
	// created/migrated once upfront instead of racing on BigQuery's
	// etag-guarded metadata updates from concurrent workers.
	if err := m.createTableAndAddColumnsIfNeeded(ctx, cfg, discardsTableName, discardsTableSchema); err != nil {
		m.logger.Warnn("Failed to create discards table and add columns", obskit.Error(err))

		return m.failOrAbortJobs(asyncDest, fmt.Errorf("failed to create discards table and add columns: %w", err))
	}

	tableWorkers := max(1, m.config.tableWorkers.Load())

	tableErrgroup, ctx := errgroup.WithContext(ctx)
	tableErrgroup.SetLimit(tableWorkers)

	var (
		succeeded, failed, aborted []int64
		failedReason, abortReason  string
		statusMu                   sync.Mutex
	)

	for _, tableName := range eventsTables {
		tableBatches := groupedAndChunkedEvents[tableName]
		tableErrgroup.Go(func() error {
			result := m.processTable(ctx, cfg, tableName, tableBatches)
			statusMu.Lock()
			defer statusMu.Unlock()

			succeeded = append(succeeded, result.succeededJobIDs...)
			if result.err != nil {
				m.logger.Warnn("Failed to process table",
					logger.NewStringField("namespace", cfg.Namespace),
					logger.NewStringField("table", tableName),
					obskit.Error(result.err),
				)
				if shouldAbort(result.err) {
					aborted = append(aborted, result.failedJobIDs...)
					abortReason = result.err.Error()
				} else {
					failed = append(failed, result.failedJobIDs...)
					failedReason = result.err.Error()
				}
			}
			return nil
		})
	}

	if err := tableErrgroup.Wait(); err != nil {
		m.logger.Warnn("Failed to process tables", obskit.Error(err))
		return m.failedJobs(asyncDest, fmt.Errorf("failed to process tables: %w", err).Error())
	}
	m.logger.Infon("Completed uploading data to BQStream V2 destination")

	m.stats.jobs.succeeded.Count(len(succeeded))
	m.stats.jobs.failed.Count(len(failed))
	m.stats.jobs.aborted.Count(len(aborted))

	return common.AsyncUploadOutput{
		SucceededJobIDs: succeeded,
		FailedJobIDs:    failed,
		FailedCount:     len(failed),
		FailedReason:    failedReason,
		AbortJobIDs:     aborted,
		AbortCount:      len(aborted),
		AbortReason:     abortReason,
		DestinationID:   asyncDest.Destination.ID,
	}
}

// refreshSchemaCacheIfNeeded fetches the warehouse schema when any of the
// upload's tables is missing from the cache, creating the namespace if it
// doesn't exist yet and refreshing the cached entries.
func (m *Manager) refreshSchemaCacheIfNeeded(ctx context.Context, cfg destConfig, groupedAndChunkedEvents map[string][]tableEvents) error {
	if !m.shouldFetchSchema(groupedAndChunkedEvents) {
		return nil
	}

	schema, err := m.fetchSchemaFromWarehouse(ctx, cfg, append(lo.Keys(groupedAndChunkedEvents), discardsTableName))
	if err != nil {
		return fmt.Errorf("failed to fetch schema: %w", err)
	}

	if len(schema) == 0 {
		m.logger.Infon("No schema found in warehouse")

		if err := m.createSchemaInWarehouse(ctx, cfg); err != nil && !checkAndIgnoreAlreadyExistError(err) {
			return fmt.Errorf("failed to create schema: %w", err)
		}
		return nil
	}

	for tableName, schema := range schema {
		// Only invalidate (and close) the stream writer when the table's
		// schema actually changed; TTL refreshes must not tear down healthy
		// streams.
		if cached, ok := m.schemaCache.Peek(tableName); !ok || !maps.Equal(cached, schema) {
			m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		}
		m.schemaCache.Set(tableName, schema, m.now())
	}
	return nil
}

// failOrAbortJobs classifies the error as terminal vs retryable and marks all
// of the upload's jobs accordingly.
func (m *Manager) failOrAbortJobs(asyncDest *common.AsyncDestinationStruct, err error) common.AsyncUploadOutput {
	if shouldAbort(err) {
		return m.abortJobs(asyncDest, err.Error())
	}
	return m.failedJobs(asyncDest, err.Error())
}

func (m *Manager) abortJobs(asyncDest *common.AsyncDestinationStruct, abortReason string) common.AsyncUploadOutput {
	m.stats.jobs.aborted.Count(len(asyncDest.ImportingJobIDs))

	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDest.ImportingJobIDs,
		AbortCount:    len(asyncDest.ImportingJobIDs),
		AbortReason:   abortReason,
		DestinationID: asyncDest.Destination.ID,
	}
}

func (m *Manager) failedJobs(asyncDest *common.AsyncDestinationStruct, failedReason string) common.AsyncUploadOutput {
	m.stats.jobs.failed.Count(len(asyncDest.ImportingJobIDs))

	return common.AsyncUploadOutput{
		FailedJobIDs:  asyncDest.ImportingJobIDs,
		FailedCount:   len(asyncDest.ImportingJobIDs),
		FailedReason:  failedReason,
		DestinationID: asyncDest.Destination.ID,
	}
}

func (m *Manager) createIntegrationManager(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   cfg.Namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}

	bigQueryManager, err := manager.New(whutils.BQStreamV2, m.appConfig, m.logger, m.statsFactory)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery manager: %w", err)
	}
	err = bigQueryManager.Setup(ctx, modelWarehouse, whutils.NewNoOpUploader())
	if err != nil {
		return nil, fmt.Errorf("setting up bigquery manager: %w", err)
	}
	return bigQueryManager, nil
}

// fetchSchemaFromWarehouse fetches the namespace schema and filters it down
// to the given tables.
func (m *Manager) fetchSchemaFromWarehouse(ctx context.Context, cfg destConfig, tableNames []string) (whutils.ModelSchema, error) {
	m.logger.Infon("Fetching schema from warehouse")

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer bigQueryManager.Cleanup(ctx)

	warehouseSchema, err := bigQueryManager.FetchSchema(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching schema from warehouse: %w", err)
	}

	tableNamesSet := lo.SliceToMap(tableNames, func(tableName string) (string, struct{}) {
		return whutils.ToProviderCase(whutils.BQ, tableName), struct{}{}
	})

	filteredWarehouseSchema := lo.PickBy(warehouseSchema, func(tableName string, schema whutils.ModelTableSchema) bool {
		_, ok := tableNamesSet[tableName]
		return ok
	})

	return filteredWarehouseSchema, nil
}

func (m *Manager) createSchemaInWarehouse(ctx context.Context, cfg destConfig) error {
	m.logger.Infon("Creating schema in warehouse",
		logger.NewStringField("namespace", cfg.Namespace),
	)

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer bigQueryManager.Cleanup(ctx)

	if err := bigQueryManager.CreateSchema(ctx); err != nil {
		return fmt.Errorf("creating schema in warehouse: %w", err)
	}
	return nil
}

// processTable streams one table's chunks: it reconciles the table schema,
// converts or discards values that don't match it (discards go to
// rudder_discards), and appends the encoded rows through the table's cached
// stream writer. Outcomes are reported per chunk: jobs of acknowledged chunks
// succeed even when other chunks of the same table fail.
func (m *Manager) processTable(ctx context.Context, cfg destConfig, tableName string, tableEventsList []tableEvents) tableProcessResult {
	m.logger.Infon("Processing table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	eventsSchema := tableEventsList[0].eventsSchema
	if err := m.createTableAndAddColumnsIfNeeded(ctx, cfg, tableName, eventsSchema); err != nil {
		return tableProcessResult{
			failedJobIDs: jobIDsFromTableEvents(tableEventsList),
			err:          fmt.Errorf("failed to create table and add columns: %w", err),
		}
	}

	warehouseEventsSchema, ok := m.schemaCache.Get(tableName, m.now())
	if !ok {
		return tableProcessResult{
			failedJobIDs: jobIDsFromTableEvents(tableEventsList),
			err:          fmt.Errorf("no warehouse schema found for table %s", tableName),
		}
	}

	formattedTS := m.now().Format(misc.RFC3339Milli)

	var discardedRecords []discardEvent
	for _, tableEvents := range tableEventsList {
		for _, event := range tableEvents.events {
			discardedRecords = append(discardedRecords, getDiscardedRecordsFromEvent(m.logger, event, warehouseEventsSchema, tableName, formattedTS)...)
		}
	}

	if len(discardedRecords) > 0 {
		m.logger.Infon("Inserting discarded records into discards table",
			logger.NewStringField("namespace", cfg.Namespace),
			logger.NewStringField("table", discardsTableName),
			logger.NewIntField("discardedRecords", int64(len(discardedRecords))),
		)

		// Resolved lazily so tables without discards never depend on the
		// discards cache entry (a concurrent worker's failure may have
		// invalidated it). The canonical schema is a safe fallback: Upload
		// guarantees the table contains at least these columns.
		warehouseDiscardsSchema, ok := m.schemaCache.Get(discardsTableName, m.now())
		if !ok {
			warehouseDiscardsSchema = discardsTableSchema
		}

		if err := m.appendToStream(ctx, cfg, discardsTableName, warehouseDiscardsSchema, convertDiscardedEventsToRows(discardedRecords)); err != nil {
			return tableProcessResult{
				failedJobIDs: jobIDsFromTableEvents(tableEventsList),
				err:          fmt.Errorf("failed to stream discarded rows: %w", err),
			}
		}

		m.stats.discards.Count(len(discardedRecords))
	}

	result := m.streamEventBatches(ctx, cfg, tableName, warehouseEventsSchema, tableEventsList)
	if result.err != nil {
		return result
	}

	if tableName != usersTableName {
		duplicateCount := lo.SumBy(tableEventsList, func(tableEvents tableEvents) int {
			return checkForDuplicateIDsInEvents(tableEvents.events)
		})
		if duplicateCount > 0 {
			m.logger.Infon("Duplicate ids found in the events", logger.NewIntField("duplicateEvents", int64(duplicateCount)), logger.NewStringField("reason", "batch"))
			m.stats.duplicateEventsInBatch.Count(duplicateCount)
		}
	}

	m.logger.Infon("Processed table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	return result
}

// streamEventBatches appends each chunk through the table's cached stream
// writer and reports outcomes per chunk: an acknowledged append is already
// durable in BigQuery, so only the failed chunks' jobs are retried instead of
// re-appending (and duplicating) the chunks that landed. Appends are pipelined
// (fire all, then wait), and the writer and schema cache are evicted on any
// failure so a retry rebuilds them against the current table schema.
func (m *Manager) streamEventBatches(ctx context.Context, cfg destConfig, tableName string, schema whutils.ModelTableSchema, tableEventsList []tableEvents) tableProcessResult {
	writer, descriptor, err := m.writerForTable(ctx, cfg, tableName, schema)
	if err != nil {
		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		return tableProcessResult{
			failedJobIDs: jobIDsFromTableEvents(tableEventsList),
			err:          fmt.Errorf("failed to stream events rows: creating stream writer: %w", err),
		}
	}

	type inFlightAppend struct {
		jobIDs       []int64
		appendResult AppendResult
	}

	var result tableProcessResult
	inFlight := make([]inFlightAppend, 0, len(tableEventsList))
	for _, tableEvents := range tableEventsList {
		rows := lo.Map(tableEvents.events, func(event *event, _ int) Row {
			return event.Message.Data
		})

		encodedRows, err := encodeRows(rows, descriptor, schema)
		if err != nil {
			result.failedJobIDs = append(result.failedJobIDs, tableEvents.jobIDs...)
			result.err = errors.Join(result.err, fmt.Errorf("encoding rows: %w", err))
			continue
		}

		appendResult, err := writer.AppendRows(ctx, encodedRows)
		if err != nil {
			result.failedJobIDs = append(result.failedJobIDs, tableEvents.jobIDs...)
			result.err = errors.Join(result.err, fmt.Errorf("appending rows: %w", err))
			continue
		}
		inFlight = append(inFlight, inFlightAppend{jobIDs: tableEvents.jobIDs, appendResult: appendResult})
	}
	for _, pending := range inFlight {
		if _, err := pending.appendResult.GetResult(ctx); err != nil {
			result.failedJobIDs = append(result.failedJobIDs, pending.jobIDs...)
			result.err = errors.Join(result.err, fmt.Errorf("getting append result: %w", err))
			continue
		}
		result.succeededJobIDs = append(result.succeededJobIDs, pending.jobIDs...)
	}

	// Any chunk failure may indicate a stale stream (e.g. schema drift), so
	// evict the writer and schema cache for the retry to rebuild them.
	if result.err != nil {
		result.err = fmt.Errorf("failed to stream events rows: %w", result.err)
		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
	}
	return result
}

// appendToStream encodes the row batches and appends them through the table's
// cached stream writer, pipelining the appends (fire all, then wait, so N
// batches cost ~one round trip) and evicting the writer and schema cache on
// any failure so a retry rebuilds them against the current table schema.
func (m *Manager) appendToStream(ctx context.Context, cfg destConfig, tableName string, schema whutils.ModelTableSchema, rowBatches ...[]Row) (err error) {
	defer func() {
		if err != nil {
			m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		}
	}()

	writer, descriptor, err := m.writerForTable(ctx, cfg, tableName, schema)
	if err != nil {
		return fmt.Errorf("creating stream writer: %w", err)
	}

	appendResults := make([]AppendResult, 0, len(rowBatches))
	for _, rows := range rowBatches {
		encodedRows, err := encodeRows(rows, descriptor, schema)
		if err != nil {
			return fmt.Errorf("encoding rows: %w", err)
		}

		appendResult, err := writer.AppendRows(ctx, encodedRows)
		if err != nil {
			return fmt.Errorf("appending rows: %w", err)
		}
		appendResults = append(appendResults, appendResult)
	}
	for _, appendResult := range appendResults {
		if _, err := appendResult.GetResult(ctx); err != nil {
			return fmt.Errorf("getting append result: %w", err)
		}
	}
	return nil
}

// createTableAndAddColumnsIfNeeded creates the table when it is not in the
// schema cache, or adds any event columns missing from the cached warehouse
// schema, keeping the cache (and the dependent stream writer) in sync.
func (m *Manager) createTableAndAddColumnsIfNeeded(ctx context.Context, cfg destConfig, tableName string, eventsSchema whutils.ModelTableSchema) error {
	warehouseSchema, ok := m.schemaCache.Get(tableName, m.now())
	if !ok {
		m.logger.Infon("No table schema found in cache",
			logger.NewStringField("namespace", cfg.Namespace),
			logger.NewStringField("table", tableName),
		)

		err := m.createTableSchema(ctx, cfg, tableName, eventsSchema)
		if err != nil {
			if !checkAndIgnoreAlreadyExistError(err) {
				m.logger.Infon("Table schema already exists", logger.NewStringField("table", tableName))

				return fmt.Errorf("table schema already exists: %w", err)
			}
			return nil
		}

		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		m.schemaCache.Set(tableName, eventsSchema, m.now())
		return nil
	}

	newColumns := findNewColumns(eventsSchema, warehouseSchema)
	if len(newColumns) > 0 {
		err := m.addColumnsToTable(ctx, cfg, tableName, newColumns)
		if err != nil {
			return fmt.Errorf("failed to add columns: %w", err)
		}

		for _, column := range newColumns {
			warehouseSchema[column.Name] = column.Type
		}
		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		m.schemaCache.Set(tableName, warehouseSchema, m.now())
	}
	return nil
}

func (m *Manager) createTableSchema(ctx context.Context, cfg destConfig, tableName string, eventsSchema whutils.ModelTableSchema) error {
	m.logger.Infon("Creating table schema",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer bigQueryManager.Cleanup(ctx)

	if err := bigQueryManager.CreateTable(ctx, tableName, eventsSchema); err != nil {
		return fmt.Errorf("creating table schema: %w", err)
	}
	return nil
}

func (m *Manager) addColumnsToTable(ctx context.Context, cfg destConfig, tableName string, columns []whutils.ColumnInfo) error {
	m.logger.Infon("Adding columns", logger.NewStringField("table", tableName))

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer bigQueryManager.Cleanup(ctx)

	if err := bigQueryManager.AddColumns(ctx, tableName, columns); err != nil {
		return fmt.Errorf("adding columns: %w", err)
	}
	return nil
}

// writerForTable returns the table's cached stream writer and proto
// descriptor, creating both from the given schema on a miss. They share one
// lifecycle: created together, evicted (and closed) together on schema change,
// so encoded rows always line up with the stream. Writers outlive the upload,
// hence context.WithoutCancel.
func (m *Manager) writerForTable(ctx context.Context, cfg destConfig, table string, schema whutils.ModelTableSchema) (StreamWriter, protoreflect.MessageDescriptor, error) {
	m.streamWritersMu.Lock()
	defer m.streamWritersMu.Unlock()

	if w, ok := m.streamWriters[streamWriterKey(cfg, table)]; ok {
		return w.writer, w.descriptor, nil
	}

	m.logger.Infon("Creating writer for table", logger.NewStringField("table", table))

	descriptor, err := m.descriptorForSchema(schema)
	if err != nil {
		return nil, nil, fmt.Errorf("creating descriptor for table: %w", err)
	}

	w, err := m.streamWriterFactory.NewStreamWriter(context.WithoutCancel(ctx), cfg, table, schema)
	if err != nil {
		return nil, nil, fmt.Errorf("creating writer for table: %w", err)
	}

	m.logger.Infon("Writer created for table", logger.NewStringField("table", table))

	m.streamWriters[streamWriterKey(cfg, table)] = tableStreamWriter{writer: w, descriptor: descriptor}

	return w, descriptor, nil
}

// invalidateTableCacheAndStreamWriter evicts the table's schema cache entry
// and closes its stream writer; writers are not bound to any upload context,
// so eviction is the only place they get released.
func (m *Manager) invalidateTableCacheAndStreamWriter(cfg destConfig, tableName string) {
	m.logger.Infon("Invalidating table cache and stream writer",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	m.schemaCache.Invalidate(tableName)

	m.streamWritersMu.Lock()
	defer m.streamWritersMu.Unlock()

	if w, ok := m.streamWriters[streamWriterKey(cfg, tableName)]; ok {
		if err := w.writer.Close(); err != nil {
			m.logger.Warnn("Failed to close stream writer",
				logger.NewStringField("table", tableName),
				obskit.Error(err),
			)
		}
		delete(m.streamWriters, streamWriterKey(cfg, tableName))
	}
}
