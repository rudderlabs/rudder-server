package bqstreamv2

import (
	"bufio"
	"fmt"
	"os"
	"reflect"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/warehouse/slave"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var (
	idColumnName         = whutils.ToProviderCase(whutils.BQStreamV2, "id")
	receivedAtColumnName = whutils.ToProviderCase(whutils.BQStreamV2, "received_at")
	uuidTSColumnName     = whutils.ToProviderCase(whutils.BQStreamV2, "uuid_ts")
	loadedAtColumnName   = whutils.ToProviderCase(whutils.BQStreamV2, "loaded_at")

	sliceOfAnyType = reflect.TypeFor[[]any]()
)

// eventsFromFile parses the async file (one event per line), stamps
// uuid_ts/loaded_at, and records each event's approximate size for chunking
// (full line length as a conservative upper bound).
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

		isUUIDTimestampSet := setColumnTimestamp(&e, uuidTSColumnName, formattedTS)
		isLoadedAtTimestampSet := setColumnTimestamp(&e, loadedAtColumnName, formattedTS)

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

// setColumnTimestamp sets the given column timestamp in the event message data and
// reports whether it was actually set.
func setColumnTimestamp(e *event, columnName, formattedTS string) bool {
	if e.Message.Metadata.Columns == nil {
		return false
	}
	if _, ok := e.Message.Metadata.Columns[columnName]; ok {
		e.Message.Data[columnName] = formattedTS
		return true
	}
	return false
}

// groupAndChunkEvents groups events by table and splits each table's events
// into chunks bounded by maxChunkBytes (the per-append payload limit).
func (m *Manager) groupAndChunkEvents(events []*event) map[string][]tableEvents {
	groupedEvents := lo.GroupBy(events, func(e *event) string {
		return e.Message.Metadata.Table
	})
	maxChunkBytes := m.config.maxChunkBytes.Load()
	groupedAndChunkedEvents := make(map[string][]tableEvents, len(groupedEvents))

	for tableName, tableEventsList := range groupedEvents {
		eventsSchema := schemaFromEvents(tableEventsList)
		providerTableName := whutils.ToProviderCase(whutils.BQStreamV2, tableName)

		var currentChunkBytes int64
		var currentChunk []*event
		flush := func() {
			if len(currentChunk) == 0 {
				return
			}
			groupedAndChunkedEvents[providerTableName] = append(groupedAndChunkedEvents[providerTableName], tableEvents{
				tableName: providerTableName,
				events:    currentChunk,
				jobIDs: lo.Map(currentChunk, func(e *event, _ int) int64 {
					return e.Metadata.JobID
				}),
				eventsSchema: eventsSchema,
			})
			currentChunk = nil
			currentChunkBytes = 0
		}

		for _, e := range tableEventsList {
			if currentChunkBytes+e.MessageDataByteSize > maxChunkBytes {
				flush()
			}
			currentChunk = append(currentChunk, e)
			currentChunkBytes += e.MessageDataByteSize
		}
		flush()
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

// shouldFetchSchema reports whether any of the upload's tables is missing
// from (or expired in) the schema cache.
func (m *Manager) shouldFetchSchema(groupedAndChunkedEvents map[string][]tableEvents) bool {
	if m.schemaCache.Len() == 0 {
		return true
	}

	for _, tableEventsList := range groupedAndChunkedEvents {
		// Chunks of a table share the same cache key, so checking the first
		// one suffices.
		if len(tableEventsList) == 0 {
			continue
		}
		if !m.schemaCache.Has(tableEventsList[0].tableName, m.now()) {
			return true
		}
	}
	return false
}

func jobIDsFromTableEvents(tableEventsList []tableEvents) []int64 {
	return lo.FlatMap(tableEventsList, func(batch tableEvents, _ int) []int64 {
		return batch.jobIDs
	})
}

// getDiscardedRecordsFromEvent mutates the event's data in place: values whose
// event type differs from the warehouse column type are converted to the
// warehouse type where possible and nilled out (and reported as discards)
// otherwise; slice values are JSON-stringified for string columns.
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
					log.Warnn("Missing ID or Received At in event",
						logger.NewStringField("tableName", tableName),
						logger.NewStringField("columnName", columnName),
					)
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
