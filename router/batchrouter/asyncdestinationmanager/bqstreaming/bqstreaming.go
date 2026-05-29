package bqstreaming

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	"github.com/rudderlabs/rudder-server/warehouse/slave"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

const defaultChunkSizeBytes int64 = 20 * bytesize.MB

var (
	idColumnName         = whutils.ToProviderCase(whutils.BQStreaming, "id")
	receivedAtColumnName = whutils.ToProviderCase(whutils.BQStreaming, "received_at")
	uuidTSColumnName     = whutils.ToProviderCase(whutils.BQ, "uuid_ts")
	loadedAtColumnName   = whutils.ToProviderCase(whutils.BQ, "loaded_at")
	usersTableName       = whutils.ToProviderCase(whutils.BQStreaming, whutils.UsersTable)
	discardsTableName    = whutils.ToProviderCase(whutils.BQStreaming, whutils.DiscardsTable)

	discardsTableSchema = lo.MapEntries(whutils.DiscardsSchema, func(columnName, columnType string) (string, string) {
		return whutils.ToProviderCase(whutils.BQStreaming, columnName), columnType
	})
	sliceOfAnyType = reflect.TypeFor[[]any]()
)

func NewManager(
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
) *Manager {
	m := &Manager{
		appConfig: conf,
		logger: log.Child("bqstreaming").Withn(
			obskit.WorkspaceID(destination.WorkspaceID),
			obskit.DestinationID(destination.ID),
			obskit.DestinationType(destination.DestinationDefinition.Name),
			logger.NewStringField("id", uuid.New().String()),
		),
		statsFactory:        statsFactory,
		destination:         destination,
		streamWriters:       make(map[string]tableStreamWriter),
		streamWriterFactory: &streamWriterFactoryImpl{},
		now:                 timeutil.Now,
	}

	m.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "BQStreaming.maxBufferCapacity")
	m.config.tableWorkers = conf.GetReloadableIntVar(25, 1, "BQStreaming.tableWorkers")
	m.config.maxChunkBytes = conf.GetReloadableInt64Var(defaultChunkSizeBytes, bytesize.B, "BQStreaming.maxChunkBytes")
	m.config.schemaCacheTTL = conf.GetReloadableDurationVar(5, time.Minute, "BQStreaming.schemaCacheTTL")
	m.schemaCache = NewTableSchemaCache(m.config.schemaCacheTTL.Load())

	tags := stats.Tags{
		"module":        "batch_router",
		"workspaceId":   destination.WorkspaceID,
		"destType":      destination.DestinationDefinition.Name,
		"destinationId": destination.ID,
	}
	m.stats.jobs.succeeded = statsFactory.NewTaggedStat("bq_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "succeeded",
	}))
	m.stats.jobs.failed = statsFactory.NewTaggedStat("bq_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "failed",
	}))
	m.stats.jobs.aborted = statsFactory.NewTaggedStat("bq_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "aborted",
	}))
	m.stats.discards = statsFactory.NewTaggedStat("bq_streaming_discards", stats.CountType, tags)
	m.stats.duplicateEventsInBatch = statsFactory.NewTaggedStat("bq_streaming_duplicate_events", stats.CountType, lo.Assign(tags, stats.Tags{
		"reason": "batch",
	}))

	m.integrationManagerCreator = func(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
		return m.createIntegrationManager(ctx, cfg)
	}

	return m
}

func (m *Manager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m *Manager) Upload(_ context.Context, asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.logger.Infon("Uploading data to BQ streaming destination")

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

	shouldFetchSchema := m.shouldFetchSchema(groupedAndChunkedEvents)
	if shouldFetchSchema {
		schema, err := m.fetchSchemaFromWarehouse(ctx, cfg, append(eventsTables, discardsTableName))
		if err != nil {
			m.logger.Warnn("Failed to fetch schema", obskit.Error(err))

			if shouldAbort(err) {
				return m.abortJobs(asyncDest, fmt.Errorf("failed to fetch schema: %w", err).Error())
			}

			return m.failedJobs(asyncDest, fmt.Errorf("failed to fetch schema: %w", err).Error())
		}

		if len(schema) == 0 {
			m.logger.Infon("No schema found in warehouse")

			err = m.createSchemaInWarehouse(ctx, cfg)
			if err != nil {
				if !checkAndIgnoreAlreadyExistError(err) {
					m.logger.Infon("Schema already exists in warehouse", logger.NewStringField("namespace", cfg.Namespace))

					if shouldAbort(err) {
						return m.abortJobs(asyncDest, fmt.Errorf("failed to create schema: %w", err).Error())
					}

					return m.failedJobs(asyncDest, fmt.Errorf("failed to create schema: %w", err).Error())
				}
			}
		} else {
			for tableName, schema := range schema {
				if cached, ok := m.schemaCache.Peek(tableName); !ok || !maps.Equal(cached, schema) {
					m.invalidateTableCacheAndStreamWriter(cfg, tableName)
				}
				m.schemaCache.Set(tableName, schema, m.now())
			}
		}
	}

	err = m.createTableAndAddColumnsIfNeeded(ctx, cfg, discardsTableName, discardsTableSchema)
	if err != nil {
		m.logger.Warnn("Failed to create discards table and add columns", obskit.Error(err))

		if shouldAbort(err) {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to create discards table and add columns: %w", err).Error())
		}

		return m.failedJobs(asyncDest, fmt.Errorf("failed to create discards table and add columns: %w", err).Error())
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
			err := m.processTable(ctx, cfg, tableName, tableBatches)
			statusMu.Lock()
			defer statusMu.Unlock()

			jobIDs := lo.FlatMap(tableBatches, func(batch tableEvents, _ int) []int64 {
				return batch.jobIDs
			})
			if err != nil {
				if shouldAbort(err) {
					aborted = append(aborted, jobIDs...)
					abortReason = err.Error()
				} else {
					failed = append(failed, jobIDs...)
					failedReason = err.Error()
				}
			} else {
				succeeded = append(succeeded, jobIDs...)
			}
			return nil
		})
	}

	if err := tableErrgroup.Wait(); err != nil {
		m.logger.Warnn("Failed to process tables", obskit.Error(err))
		return m.failedJobs(asyncDest, fmt.Errorf("failed to process tables: %w", err).Error())
	}
	m.logger.Infon("Completed uploading data to BQ streaming destination")

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

func (m *Manager) eventsFromFile(fileName string, eventsCount int) ([]*event, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("opening async file: %w", err)
	}
	defer func() { _ = file.Close() }()

	events := make([]*event, 0, eventsCount)
	formattedTS := m.now().Format(misc.RFC3339Milli)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(m.config.maxBufferCapacity.Load()))

	for scanner.Scan() {
		line := scanner.Bytes()

		var e event
		if err := jsonrs.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("unmarshalling event line: %w", err)
		}

		isUUIDTimestampSet := m.setUUIDTimestamp(&e, formattedTS)
		isLoadedAtTimestampSet := m.setLoadedAtTimestamp(&e, formattedTS)

		e.MessageDataByteSize = int64(len(line))
		if isUUIDTimestampSet {
			e.MessageDataByteSize += int64(len(formattedTS))
		}
		if isLoadedAtTimestampSet {
			e.MessageDataByteSize += int64(len(formattedTS))
		}

		events = append(events, &e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanning async file: %w", err)
	}
	return events, nil
}

func (m *Manager) groupAndChunkEvents(events []*event) map[string][]tableEvents {
	groupedEvents := lo.GroupBy(events, func(e *event) string {
		return e.Message.Metadata.Table
	})
	maxChunkBytes := m.config.maxChunkBytes.Load()
	groupedAndChunkedEvents := make(map[string][]tableEvents, len(groupedEvents))

	for tableName, tableEventsList := range groupedEvents {
		eventsSchema := schemaFromEvents(tableEventsList)

		var currentChunkBytes int64
		var currentChunk []*event

		for _, e := range tableEventsList {
			size := e.MessageDataByteSize
			if currentChunkBytes+size > maxChunkBytes {
				groupedAndChunkedEvents[tableName] = append(groupedAndChunkedEvents[tableName], tableEvents{
					tableName: whutils.ToProviderCase(whutils.BQ, tableName),
					events:    currentChunk,
					jobIDs: lo.Map(currentChunk, func(e *event, _ int) int64 {
						return e.Metadata.JobID
					}),
					eventsSchema: eventsSchema,
				})
				currentChunk = make([]*event, 0)
				currentChunkBytes = 0
			}
			currentChunk = append(currentChunk, e)
			currentChunkBytes += size
		}

		if len(currentChunk) > 0 {
			groupedAndChunkedEvents[tableName] = append(groupedAndChunkedEvents[tableName], tableEvents{
				tableName: whutils.ToProviderCase(whutils.BQ, tableName),
				events:    currentChunk,
				jobIDs: lo.Map(currentChunk, func(e *event, _ int) int64 {
					return e.Metadata.JobID
				}),
				eventsSchema: eventsSchema,
			})
		}
	}
	return groupedAndChunkedEvents
}

func schemaFromEvents(events []*event) whutils.ModelTableSchema {
	columnsMap := make(whutils.ModelTableSchema)
	for _, e := range events {
		for col, typ := range e.Message.Metadata.Columns {
			if _, exists := columnsMap[col]; !exists {
				columnsMap[col] = typ
			}
		}
	}
	return columnsMap
}

func (m *Manager) shouldFetchSchema(groupedAndChunkedEvents map[string][]tableEvents) bool {
	if m.schemaCache.Len() == 0 {
		return true
	}

	for _, tableEventsList := range groupedAndChunkedEvents {
		for _, tableEvents := range tableEventsList {
			if _, ok := m.schemaCache.Get(tableEvents.tableName, m.now()); !ok {
				return true
			}
			continue
		}
	}
	return false
}

func (m *Manager) createIntegrationManager(ctx context.Context, cfg destConfig) (IntegrationManager, error) {
	modelWarehouse := whutils.ModelWarehouse{
		WorkspaceID: m.destination.WorkspaceID,
		Destination: *m.destination,
		Namespace:   cfg.Namespace,
		Type:        m.destination.DestinationDefinition.Name,
		Identifier:  m.destination.WorkspaceID + ":" + m.destination.ID,
	}

	bigQueryManager, err := manager.New(whutils.BQStreaming, m.appConfig, m.logger, m.statsFactory)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery manager: %w", err)
	}
	err = bigQueryManager.Setup(ctx, modelWarehouse, whutils.NewNoOpUploader())
	if err != nil {
		return nil, fmt.Errorf("setting up bigquery manager: %w", err)
	}
	return bigQueryManager, nil
}

func (m *Manager) fetchSchemaFromWarehouse(ctx context.Context, cfg destConfig, tableNames []string) (whutils.ModelSchema, error) {
	m.logger.Infon("Fetching schema from warehouse")

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer func() {
		bigQueryManager.Cleanup(ctx)
	}()

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
	defer func() {
		bigQueryManager.Cleanup(ctx)
	}()

	err = bigQueryManager.CreateSchema(ctx)
	if err != nil {
		return fmt.Errorf("creating schema in warehouse: %w", err)
	}
	return nil
}

func checkAndIgnoreAlreadyExistError(err error) bool {
	var e *googleapi.Error
	if errors.As(err, &e) {
		// 409 is returned when we try to create a table that already exists
		// 400 is returned for all kinds of invalid input - so we need to check the error message too
		if e.Code == 409 || (e.Code == 400 && strings.Contains(e.Message, "already exists in schema")) {
			return true
		}
	}
	return false
}

func (m *Manager) processTable(ctx context.Context, cfg destConfig, tableName string, tableEventsList []tableEvents) error {
	m.logger.Infon("Processing table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	eventsSchema := tableEventsList[0].eventsSchema
	err := m.createTableAndAddColumnsIfNeeded(ctx, cfg, tableName, eventsSchema)
	if err != nil {
		return fmt.Errorf("failed to create table and add columns: %w", err)
	}

	warehouseEventsSchema, ok := m.schemaCache.Get(tableName, m.now())
	if !ok {
		return fmt.Errorf("no warehouse schema found for table %s", tableName)
	}
	warehouseDiscardsSchema, ok := m.schemaCache.Get(discardsTableName, m.now())
	if !ok {
		return fmt.Errorf("no warehouse schema found for discards table %s", discardsTableName)
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

		var discardsErr error
		var discardsSchemaDescriptor protoreflect.MessageDescriptor

		defer func() {
			if discardsErr != nil {
				m.invalidateTableCacheAndStreamWriter(cfg, discardsTableName)
			}
		}()

		var discardsWriter StreamWriter
		discardsWriter, discardsSchemaDescriptor, discardsErr = m.writerForTable(ctx, cfg, discardsTableName, warehouseDiscardsSchema)
		if discardsErr != nil {
			return fmt.Errorf("failed to create discards writer: %w", discardsErr)
		}

		var encodedRows [][]byte
		encodedRows, discardsErr = encodeRows(convertDiscardedEventsToRows(discardedRecords), discardsSchemaDescriptor, warehouseDiscardsSchema)
		if discardsErr != nil {
			return fmt.Errorf("failed to encode discarded rows: %w", discardsErr)
		}

		var discardedRowsRes AppendResult
		discardedRowsRes, discardsErr = discardsWriter.AppendRows(ctx, encodedRows)
		if discardsErr != nil {
			return fmt.Errorf("failed to append discarded rows: %w", discardsErr)
		}

		_, discardsErr = discardedRowsRes.GetResult(ctx)
		if discardsErr != nil {
			return fmt.Errorf("failed to get result of discarded rows: %w", discardsErr)
		}

		m.stats.discards.Count(len(discardedRecords))
	}

	var duplicateCount int
	if tableName != usersTableName {
		for _, tableEvents := range tableEventsList {
			duplicateCount += checkForDuplicateIDsInEvents(tableEvents.events)
		}
	}

	var eventsErr error
	var eventsSchemaDescriptor protoreflect.MessageDescriptor

	defer func() {
		if eventsErr != nil {
			m.invalidateTableCacheAndStreamWriter(cfg, tableName)
		}
	}()

	var eventsWriter StreamWriter
	eventsWriter, eventsSchemaDescriptor, eventsErr = m.writerForTable(ctx, cfg, tableName, warehouseEventsSchema)
	if eventsErr != nil {
		return fmt.Errorf("failed to create events writer: %w", eventsErr)
	}

	appendResults := make([]AppendResult, 0, len(tableEventsList))
	for _, tableEvents := range tableEventsList {
		eventRows := lo.Map(tableEvents.events, func(event *event, _ int) Row {
			return event.Message.Data
		})

		var encodedRows [][]byte
		encodedRows, eventsErr = encodeRows(eventRows, eventsSchemaDescriptor, warehouseEventsSchema)
		if eventsErr != nil {
			return fmt.Errorf("failed to encode events rows: %w", eventsErr)
		}

		var eventsRes AppendResult
		eventsRes, eventsErr = eventsWriter.AppendRows(ctx, encodedRows)
		if eventsErr != nil {
			return fmt.Errorf("failed to append events rows: %w", eventsErr)
		}
		appendResults = append(appendResults, eventsRes)
	}
	for _, eventsRes := range appendResults {
		if _, eventsErr = eventsRes.GetResult(ctx); eventsErr != nil {
			return fmt.Errorf("failed to get result of events rows: %w", eventsErr)
		}
	}

	if duplicateCount > 0 {
		m.logger.Infon("Duplicate ids found in the events", logger.NewIntField("duplicateEvents", int64(duplicateCount)), logger.NewStringField("reason", "batch"))
		m.stats.duplicateEventsInBatch.Count(duplicateCount)
	}

	m.logger.Infon("Processed table",
		logger.NewStringField("namespace", cfg.Namespace),
		logger.NewStringField("table", tableName),
	)

	return nil
}

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
	defer func() {
		bigQueryManager.Cleanup(ctx)
	}()

	err = bigQueryManager.CreateTable(ctx, tableName, eventsSchema)
	if err != nil {
		return fmt.Errorf("creating table schema: %w", err)
	}
	return nil
}

func findNewColumns(eventSchema, warehouseSchema whutils.ModelTableSchema) []whutils.ColumnInfo {
	var newColumns []whutils.ColumnInfo
	for column, dataType := range eventSchema {
		if _, exists := warehouseSchema[column]; !exists {
			newColumns = append(newColumns, whutils.ColumnInfo{
				Name: column,
				Type: dataType,
			})
		}
	}
	return newColumns
}

func (m *Manager) addColumnsToTable(ctx context.Context, cfg destConfig, tableName string, columns []whutils.ColumnInfo) error {
	m.logger.Infon("Adding columns", logger.NewStringField("table", tableName))

	bigQueryManager, err := m.integrationManagerCreator(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating bigquery manager: %w", err)
	}
	defer func() {
		bigQueryManager.Cleanup(ctx)
	}()

	err = bigQueryManager.AddColumns(ctx, tableName, columns)
	if err != nil {
		return fmt.Errorf("adding columns: %w", err)
	}
	return nil
}

func getDiscardedRecordsFromEvent(log logger.Logger, event *event, warehouseSchema whutils.ModelTableSchema, tableName, formattedTS string) (discardedRecords []discardEvent) {
	for columnName, actualType := range event.Message.Metadata.Columns {
		if expectedType, exists := warehouseSchema[columnName]; exists && actualType != expectedType {
			currentValue := event.Message.Data[columnName]
			convertedVal, err := slave.HandleSchemaChange(log, expectedType, actualType, currentValue)
			if err != nil {
				event.Message.Data[columnName] = nil // Discard value if conversion fails

				rowID, idExists := event.Message.Data[idColumnName]
				receivedAt, receivedAtExists := event.Message.Data[receivedAtColumnName]

				if !idExists || !receivedAtExists {
					continue
				}

				discardedRecords = append(discardedRecords, discardEvent{
					tableName:   tableName,
					columnName:  columnName,
					columnValue: currentValue,
					reason:      err.Error(),
					uuidTS:      formattedTS,
					rowID:       rowID,
					receivedAt:  receivedAt,
				})
			} else {
				// Update value if conversion succeeds
				event.Message.Data[columnName] = convertedVal
			}
		}
		if reflect.TypeOf(event.Message.Data[columnName]) == sliceOfAnyType {
			marshalledVal, err := jsonrs.Marshal(event.Message.Data[columnName])
			if err != nil {
				// Discard value if marshalling fails
				event.Message.Data[columnName] = nil
			} else {
				event.Message.Data[columnName] = string(marshalledVal)
			}
		}
	}
	return discardedRecords
}

func checkForDuplicateIDsInEvents(events []*event) (duplicateCount int) {
	ids := lo.FilterMap(events, func(event *event, _ int) (any, bool) {
		id, ok := event.Message.Data[idColumnName]
		if !ok {
			return nil, false
		}
		return id, true
	})

	duplicates := make(map[any]struct{})
	for _, id := range ids {
		if _, ok := duplicates[id]; ok {
			duplicateCount++
			continue
		}
		duplicates[id] = struct{}{}
	}
	return duplicateCount
}

func (m *Manager) descriptorForSchema(schema whutils.ModelTableSchema) (protoreflect.MessageDescriptor, error) {
	tableSchema, err := adapt.BQSchemaToStorageTableSchema(toBigQuerySchema(schema))
	if err != nil {
		return nil, fmt.Errorf("converting schema to storage table schema: %w", err)
	}
	desc, err := adapt.StorageSchemaToProto2Descriptor(tableSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("converting storage schema to proto2 descriptor: %w", err)
	}
	md, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("unexpected descriptor type: %T", desc)
	}
	return md, nil
}

func streamWriterKey(cfg destConfig, tableName string) string {
	return cfg.ProjectID + ":" + cfg.Namespace + ":" + tableName
}

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

func convertDiscardedEventsToRows(discardEvents []discardEvent) []Row {
	return lo.FilterMap(discardEvents, func(event discardEvent, _ int) (Row, bool) {
		return Row{
			"column_name":  event.columnName,
			"column_value": fmt.Sprintf("%v", event.columnValue),
			"reason":       event.reason,
			"received_at":  event.receivedAt,
			"row_id":       fmt.Sprintf("%v", event.rowID),
			"table_name":   event.tableName,
			"uuid_ts":      event.uuidTS,
		}, true
	})
}

func encodeRows(rows []Row, md protoreflect.MessageDescriptor, schema whutils.ModelTableSchema) ([][]byte, error) {
	fields := md.Fields()
	fieldsByName := make(map[string]protoreflect.FieldDescriptor, fields.Len())
	for i := range fields.Len() {
		fd := fields.Get(i)
		fieldsByName[string(fd.Name())] = fd
	}

	message := dynamicpb.NewMessage(md)
	encodedRows := make([][]byte, 0, len(rows))
	for _, row := range rows {
		normalizeRow(row, schema)

		proto.Reset(message)
		for columnName, value := range row {
			if value == nil {
				continue
			}
			fd, ok := fieldsByName[columnName]
			if !ok {
				return nil, fmt.Errorf("encoding row: unknown column %q", columnName)
			}
			fieldValue, err := protoValueFor(fd, value)
			if err != nil {
				return nil, fmt.Errorf("encoding row: column %q: %w", columnName, err)
			}
			message.Set(fd, fieldValue)
		}

		encoded, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("marshalling row: %w", err)
		}
		encodedRows = append(encodedRows, encoded)
	}
	return encodedRows, nil
}

func protoValueFor(fd protoreflect.FieldDescriptor, value any) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		if v, ok := value.(string); ok {
			return protoreflect.ValueOfString(v), nil
		}
	case protoreflect.BoolKind:
		if v, ok := value.(bool); ok {
			return protoreflect.ValueOfBool(v), nil
		}
	case protoreflect.Int64Kind:
		switch v := value.(type) {
		case int64:
			return protoreflect.ValueOfInt64(v), nil
		case int:
			return protoreflect.ValueOfInt64(int64(v)), nil
		case float64:
			if v == math.Trunc(v) {
				return protoreflect.ValueOfInt64(int64(v)), nil
			}
		case json.Number:
			if parsed, err := v.Int64(); err == nil {
				return protoreflect.ValueOfInt64(parsed), nil
			}
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				return protoreflect.ValueOfInt64(parsed), nil
			}
		}
	case protoreflect.DoubleKind:
		switch v := value.(type) {
		case float64:
			return protoreflect.ValueOfFloat64(v), nil
		case int64:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		case int:
			return protoreflect.ValueOfFloat64(float64(v)), nil
		case json.Number:
			if parsed, err := v.Float64(); err == nil {
				return protoreflect.ValueOfFloat64(parsed), nil
			}
		case string:
			if parsed, err := strconv.ParseFloat(v, 64); err == nil {
				return protoreflect.ValueOfFloat64(parsed), nil
			}
		}
	}
	return protoreflect.Value{}, fmt.Errorf("invalid value of type %T for %s field", value, fd.Kind())
}

func normalizeRow(row Row, schema whutils.ModelTableSchema) {
	for col, v := range row {
		if v == nil {
			continue
		}
		switch schema[col] {
		case "datetime":
			s, ok := v.(string)
			if !ok {
				continue
			}
			if ts, err := time.Parse(time.RFC3339Nano, s); err == nil {
				row[col] = ts.UnixMicro()
			}
		}
	}
}

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

func shouldAbort(err error) bool {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.PermissionDenied, codes.Unauthenticated, codes.FailedPrecondition:
		return true
	case codes.DeadlineExceeded, codes.Unavailable, codes.Internal, codes.Aborted, codes.ResourceExhausted, codes.Canceled, codes.Unknown, codes.NotFound:
		return false
	default:
		return false
	}
}
