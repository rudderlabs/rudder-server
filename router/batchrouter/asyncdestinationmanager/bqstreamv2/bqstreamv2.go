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
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// defaultChunkSizeBytes bounds a single AppendRows request. The Storage Write
// API rejects requests over 10MB, and an oversized chunk would fail on every
// retry, so stay well under the limit (row sizes are accounted as conservative
// upper bounds, leaving additional headroom).
const defaultChunkSizeBytes int64 = 8 * bytesize.MB

// errWriterClosed is reported (once per batch) when an append is attempted on a
// stream writer a concurrent worker has already evicted/closed; the caller
// fails those batches so a retry rebuilds the writer.
var errWriterClosed = errors.New("stream writer already closed")

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
) common.AsyncDestinationManager {
	return common.SimpleAsyncDestinationManager{UploaderAndTransformer: newManager(conf, log, statsFactory, destination)}
}

func newManager(
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
		streamWriters: make(map[string]*tableStreamWriter),
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

	// Deliberately detached from the caller's context: an in-flight upload must
	// run to completion so per-chunk success/failure accounting stays accurate
	// and acknowledged appends are not re-sent on retry. Stream writers also
	// outlive a single Upload (see writerForTable's context.WithoutCancel), so
	// they must not be torn down by a cancelled request context. The trade-off
	// is that a graceful shutdown will not cancel an upload already in progress.
	ctx := context.Background()

	var cfg destConfig
	if err := cfg.Decode(asyncDest.Destination.Config); err != nil {
		m.logger.Warnn("Failed to decode destination config",
			obskit.Error(err),
		)
		return m.abortJobs(asyncDest, fmt.Errorf("decoding destination config: %w", err).Error())
	}

	// One integration manager is shared by every schema operation in this
	// upload, created lazily on first use and cleaned up once, instead of
	// dialing a client and running a dropDanglingStagingTables query per
	// operation. getIntegrationManager memoises the (manager, error) outcome; an
	// upload whose schemas are fully cached never calls it, so it dials nothing.
	var (
		integrationManager     IntegrationManager
		integrationManagerErr  error
		integrationManagerOnce sync.Once
	)
	integrationManagerCreator := func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
		integrationManagerOnce.Do(func() {
			integrationManager, integrationManagerErr = m.integrationManagerCreator(ctx, cfg)
		})
		return integrationManager, integrationManagerErr
	}
	defer func() {
		if integrationManager != nil {
			integrationManager.Cleanup(ctx)
		}
	}()

	events, err := m.eventsFromFile(asyncDest.FileName, asyncDest.Count)
	if err != nil {
		m.logger.Warnn("Failed to read events from file",
			obskit.Error(err),
		)
		return m.abortJobs(asyncDest, fmt.Errorf("reading events from file: %w", err).Error())
	}
	m.logger.Infon("Read events from file",
		logger.NewIntField("events", int64(len(events))),
		logger.NewIntField("size", int64(asyncDest.Size)),
	)

	groupedAndChunkedEvents := m.groupAndChunkEvents(events)
	eventsTables := lo.Keys(groupedAndChunkedEvents)

	if err := m.refreshSchemaCacheIfNeeded(ctx, cfg, integrationManagerCreator, groupedAndChunkedEvents); err != nil {
		m.logger.Warnn("Failed to refresh schema cache", obskit.Error(err))

		return m.failOrAbortJobs(asyncDest, err)
	}

	// The discards table is shared by all table workers, so it is
	// created/migrated once upfront instead of racing on BigQuery's
	// etag-guarded metadata updates from concurrent workers.
	if err := m.createTableAndAddColumnsIfNeeded(ctx, cfg, integrationManagerCreator, discardsTableName, discardsTableSchema); err != nil {
		m.logger.Warnn("Failed to create discards table and add columns", obskit.Error(err))

		return m.failOrAbortJobs(asyncDest, fmt.Errorf("creating discards table and adding columns: %w", err))
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
			result := m.processTable(ctx, cfg, integrationManagerCreator, tableName, tableBatches)

			statusMu.Lock()
			defer statusMu.Unlock()

			succeeded = append(succeeded, result.succeededJobIDs...)

			failedJobResult := result.failedJobResult
			failedJobIDs := failedJobResult.failedJobIDs
			failedJobError := failedJobResult.failedJobError

			if failedJobError != nil {
				m.logger.Warnn("Failed to process table",
					logger.NewStringField("namespace", cfg.Namespace),
					logger.NewStringField("table", tableName),
					obskit.Error(failedJobError),
				)

				if shouldAbort(failedJobError) {
					aborted = append(aborted, failedJobIDs...)
					abortReason = failedJobError.Error()
				} else {
					failed = append(failed, failedJobIDs...)
					failedReason = failedJobError.Error()
				}
			}
			return nil
		})
	}

	_ = tableErrgroup.Wait()

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
func (m *Manager) refreshSchemaCacheIfNeeded(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, groupedAndChunkedEvents map[string][]tableEvents) error {
	if !m.shouldFetchSchema(groupedAndChunkedEvents) {
		return nil
	}

	schema, err := m.fetchSchemaFromWarehouseForTables(ctx, cfg, integrationManagerCreator, append(lo.Keys(groupedAndChunkedEvents), discardsTableName))
	if err != nil {
		return fmt.Errorf("fetching schema from warehouse for tables: %w", err)
	}

	if len(schema) == 0 {
		m.logger.Infon("No schema found in warehouse")

		if err := m.createSchemaInWarehouse(ctx, cfg, integrationManagerCreator); err != nil && !checkAndIgnoreAlreadyExistError(err) {
			return fmt.Errorf("creating schema in warehouse: %w", err)
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

// abortJobs marks all of the upload's jobs as aborted.
func (m *Manager) abortJobs(asyncDest *common.AsyncDestinationStruct, abortReason string) common.AsyncUploadOutput {
	m.stats.jobs.aborted.Count(len(asyncDest.ImportingJobIDs))

	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDest.ImportingJobIDs,
		AbortCount:    len(asyncDest.ImportingJobIDs),
		AbortReason:   abortReason,
		DestinationID: asyncDest.Destination.ID,
	}
}

// failedJobs marks all of the upload's jobs as failed.
func (m *Manager) failedJobs(asyncDest *common.AsyncDestinationStruct, failedReason string) common.AsyncUploadOutput {
	m.stats.jobs.failed.Count(len(asyncDest.ImportingJobIDs))

	return common.AsyncUploadOutput{
		FailedJobIDs:  asyncDest.ImportingJobIDs,
		FailedCount:   len(asyncDest.ImportingJobIDs),
		FailedReason:  failedReason,
		DestinationID: asyncDest.Destination.ID,
	}
}

// processTable streams one table's chunks: it reconciles the table schema,
// converts or discards values that don't match it (discards go to
// rudder_discards), and appends the encoded rows through the table's cached
// stream writer. Outcomes are reported per chunk: jobs of acknowledged chunks
// succeed even when other chunks of the same table fail.
func (m *Manager) processTable(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, tableName string, tableEventsList []tableEvents) tableProcessResult {
	m.logger.Infon("Processing table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	eventsSchema := tableEventsList[0].eventsSchema
	if err := m.createTableAndAddColumnsIfNeeded(ctx, cfg, integrationManagerCreator, tableName, eventsSchema); err != nil {
		return tableProcessResult{
			failedJobResult: failedJobResult{
				failedJobIDs:   jobIDsFromTableEvents(tableEventsList),
				failedJobError: fmt.Errorf("creating table schema: %w", err),
			},
		}
	}

	warehouseEventsSchema, ok := m.schemaCache.Get(tableName, m.now())
	if !ok {
		return tableProcessResult{
			failedJobResult: failedJobResult{
				failedJobIDs:   jobIDsFromTableEvents(tableEventsList),
				failedJobError: fmt.Errorf("no warehouse schema found for table %s", tableName),
			},
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

		err := m.streamDiscardedEvents(ctx, cfg, discardsTableName, warehouseDiscardsSchema, convertDiscardedEventsToRows(discardedRecords))
		if err != nil {
			return tableProcessResult{
				failedJobResult: failedJobResult{
					failedJobIDs:   jobIDsFromTableEvents(tableEventsList),
					failedJobError: fmt.Errorf("streaming discarded rows: %w", err),
				},
			}
		}

		m.stats.discards.Count(len(discardedRecords))
	}

	streamResult := m.streamEventBatches(ctx, cfg, tableName, warehouseEventsSchema, tableEventsList)

	m.logger.Infon("Processed table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	if tableName != usersTableName {
		duplicateCount := lo.SumBy(tableEventsList, func(tableEvents tableEvents) int {
			return checkForDuplicateIDsInEvents(tableEvents.events)
		})
		if duplicateCount > 0 {
			m.logger.Infon("Duplicate ids found in the events", logger.NewIntField("duplicateEvents", int64(duplicateCount)), logger.NewStringField("reason", "batch"))
			m.stats.duplicateEventsInBatch.Count(duplicateCount)
		}
	}

	response := tableProcessResult{
		succeededJobIDs: streamResult.succeededIDs,
		failedJobResult: failedJobResult{
			failedJobIDs: streamResult.failedIDs,
		},
	}
	if streamResult.failedError != nil {
		response.failedJobResult.failedJobError = streamResult.failedError
	}
	return response
}

// streamEventBatches appends each chunk through the table's cached stream
// writer and reports outcomes per chunk: an acknowledged append is already
// durable in BigQuery, so only the failed chunks' jobs are retried instead of
// re-appending (and duplicating) the chunks that landed. Appends are pipelined
// (fire all, then wait), and the writer and schema cache are evicted on any
// failure so a retry rebuilds them against the current table schema.
func (m *Manager) streamEventBatches(ctx context.Context, cfg destConfig, tableName string, schema whutils.ModelTableSchema, tableEventsList []tableEvents) streamEventBatchesResult {
	tableStreamWriter, err := m.writerForTable(ctx, cfg, tableName, schema)
	if err != nil {
		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		return streamEventBatchesResult{
			failedIDs:   jobIDsFromTableEvents(tableEventsList),
			failedError: fmt.Errorf("streaming events rows: creating stream writer: %w", err),
		}
	}

	var result streamEventBatchesResult
	defer func() {
		if result.failedError != nil {
			m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		}
	}()

	for _, tableEvents := range tableEventsList {
		rows := lo.Map(tableEvents.events, func(event *event, _ int) Row {
			return event.Message.Data
		})

		encodedRows, err := encodeRows(rows, tableStreamWriter.descriptor, schema)
		if err != nil {
			result.failedIDs = append(result.failedIDs, tableEvents.jobIDs...)
			result.failedError = errors.Join(result.failedError, fmt.Errorf("encoding rows: %w", err))
			continue
		}

		if err := tableStreamWriter.streamEncodedRows(ctx, encodedRows); err != nil {
			result.failedIDs = append(result.failedIDs, tableEvents.jobIDs...)
			result.failedError = errors.Join(result.failedError, fmt.Errorf("streaming encoded rows: %w", err))
			continue
		}

		result.succeededIDs = append(result.succeededIDs, tableEvents.jobIDs...)
	}
	return result
}

// streamDiscardedEvents encodes the row batches and appends them through the
// table's cached stream writer, pipelining the appends (fire all, then wait, so
// N batches cost ~one round trip) and evicting the writer and schema cache on
// any failure so a retry rebuilds them against the current table schema.
func (m *Manager) streamDiscardedEvents(ctx context.Context, cfg destConfig, tableName string, schema whutils.ModelTableSchema, rows []Row) (err error) {
	defer func() {
		if err != nil {
			m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		}
	}()

	tableStreamWriter, err := m.writerForTable(ctx, cfg, tableName, schema)
	if err != nil {
		return fmt.Errorf("creating stream writer: %w", err)
	}

	encodedRows, err := encodeRows(rows, tableStreamWriter.descriptor, schema)
	if err != nil {
		return fmt.Errorf("encoding rows: %w", err)
	}

	if err := tableStreamWriter.streamEncodedRows(ctx, encodedRows); err != nil {
		return fmt.Errorf("streaming encoded rows: %w", err)
	}

	return nil
}

// createTableAndAddColumnsIfNeeded creates the table when it is not in the
// schema cache, or adds any event columns missing from the cached warehouse
// schema, keeping the cache (and the dependent stream writer) in sync.
func (m *Manager) createTableAndAddColumnsIfNeeded(ctx context.Context, cfg destConfig, integrationManagerCreator IntegrationManagerCreator, tableName string, eventsSchema whutils.ModelTableSchema) error {
	warehouseSchema, ok := m.schemaCache.Get(tableName, m.now())
	if !ok {
		m.logger.Infon("No table schema found in cache",
			logger.NewStringField("namespace", cfg.Namespace),
			logger.NewStringField("table", tableName),
		)

		err := m.createTableSchema(ctx, cfg, integrationManagerCreator, tableName, eventsSchema)
		if err != nil {
			if !checkAndIgnoreAlreadyExistError(err) {
				m.logger.Infon("Table schema already exists", logger.NewStringField("table", tableName))

				return fmt.Errorf("creating table schema: %w", err)
			}
			return nil
		}

		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		m.schemaCache.Set(tableName, eventsSchema, m.now())
		return nil
	}

	newColumns := findNewColumns(eventsSchema, warehouseSchema)
	if len(newColumns) > 0 {
		err := m.addColumnsToTable(ctx, cfg, integrationManagerCreator, tableName, newColumns)
		if err != nil {
			return fmt.Errorf("adding columns to table %s: %w", tableName, err)
		}

		for _, column := range newColumns {
			warehouseSchema[column.Name] = column.Type
		}
		m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		m.schemaCache.Set(tableName, warehouseSchema, m.now())
	}
	return nil
}

// writerForTable returns the table's cached stream writer handle, creating it
// from the given schema on a miss. The writer and its proto descriptor share
// one lifecycle: created together, evicted (and closed) together on schema
// change, so encoded rows always line up with the stream. Writers outlive the
// upload, hence context.WithoutCancel.
//
// The map mutex is released before dialing the gRPC client / opening the
// managed stream, so concurrent workers for other tables are not serialized
// behind this table's network round-trips. A second lookup after creation
// keeps the winner when two workers race to build the same writer (the shared
// discards stream), discarding the loser's duplicate.
func (m *Manager) writerForTable(ctx context.Context, cfg destConfig, tableName string, schema whutils.ModelTableSchema) (*tableStreamWriter, error) {
	m.streamWritersMu.Lock()
	if handle, ok := m.streamWriters[tableName]; ok {
		m.streamWritersMu.Unlock()
		return handle, nil
	}
	m.streamWritersMu.Unlock()

	m.logger.Infon("Creating writer for table", logger.NewStringField("table", tableName))

	descriptor, err := descriptorForSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("creating descriptor for table: %w", err)
	}

	w, err := m.streamWriterFactory.NewStreamWriter(context.WithoutCancel(ctx), cfg, tableName, schema)
	if err != nil {
		return nil, fmt.Errorf("creating writer for table: %w", err)
	}

	m.logger.Infon("Writer created for table", logger.NewStringField("table", tableName))

	tableStreamWriter := NewTableStreamWriter(w, descriptor)

	m.streamWritersMu.Lock()
	if existing, ok := m.streamWriters[tableName]; ok {
		m.streamWritersMu.Unlock()
		// Another worker won the race while we were dialing; keep theirs and
		// discard the writer we just created.
		if err := w.Close(); err != nil {
			m.logger.Warnn("Failed to close duplicate stream writer",
				logger.NewStringField("table", tableName),
				obskit.Error(err),
			)
		}
		return existing, nil
	}
	m.streamWriters[tableName] = tableStreamWriter
	m.streamWritersMu.Unlock()

	return tableStreamWriter, nil
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
	handle, ok := m.streamWriters[tableName]
	if ok {
		delete(m.streamWriters, tableName)
	}
	m.streamWritersMu.Unlock()

	if !ok {
		return
	}

	// close() takes the writer's exclusive lock, so it drains in-flight appends
	// (e.g. other workers sharing the discards stream) and blocks new ones
	// rather than racing a concurrent AppendRows. It is closed outside the map
	// mutex so that lock is never held across network I/O.
	if err := handle.close(); err != nil {
		m.logger.Warnn("Failed to close stream writer",
			logger.NewStringField("table", tableName),
			obskit.Error(err),
		)
	}
}
