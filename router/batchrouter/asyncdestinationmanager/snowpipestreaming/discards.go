package snowpipestreaming

import (
	"context"
	"fmt"
	"reflect"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/slave"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// sendDiscardEventsToSnowpipe uploads discarded records to the Snowpipe discards table.
// In case of failure, it deletes the channel.
func (m *Manager) sendDiscardEventsToSnowpipe(
	ctx context.Context,
	offset string,
	discardsChannelID string,
	discardInfos []discardInfo,
) (*importInfo, error) {
	tableName := discardsTable()

	insertReq := &model.InsertRequest{
		Rows:   convertDiscardedInfosToRows(discardInfos),
		Offset: offset,
	}

	var destConf destConfig
	err := destConf.Decode(m.destination.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to decode destination config: %w", err)
	}

	info := &uploadInfo{
		tableName:    discardsTable(),
		eventsSchema: discardsSchema(),
	}

	channelID, err := m.insert(ctx, m.destination.ID, &destConf, info, insertReq, discardsChannelID)
	if err != nil {
		return nil, fmt.Errorf("inserting data to discards: %v", err)
	}

	m.stats.discards.Count(len(discardInfos))

	imInfo := &importInfo{
		ChannelID: channelID,
		Offset:    offset,
		Table:     tableName,
		Count:     len(discardInfos),
	}
	return imInfo, nil
}

func discardsTable() string {
	return whutils.ToProviderCase(whutils.SnowpipeStreaming, whutils.DiscardsTable)
}

func discardsSchema() whutils.ModelTableSchema {
	return lo.MapEntries(whutils.DiscardsSchema, func(columnName, columnType string) (string, string) {
		return whutils.ToProviderCase(whutils.SnowpipeStreaming, columnName), columnType
	})
}

// getDiscardedRecordsFromEvent returns the records that were discarded due to schema mismatch
// It also updates the event data with the converted values
// If the conversion fails, the value is discarded
// If the value is a slice, it is marshalled to a string
func getDiscardedRecordsFromEvent(
	log logger.Logger,
	event *event,
	snowpipeSchema whutils.ModelTableSchema,
	tableName string,
	formattedTS string,
) (discardedRecords []discardInfo) {
	sliceType := reflect.TypeFor[[]any]()
	for columnName, actualType := range event.Message.Metadata.Columns {
		if expectedType, exists := snowpipeSchema[columnName]; exists && actualType != expectedType {
			currentValue := event.Message.Data[columnName]
			convertedVal, err := slave.HandleSchemaChange(log, expectedType, actualType, currentValue)
			if err != nil {
				event.Message.Data[columnName] = nil // Discard value if conversion fails

				rowID, idExists := event.Message.Data[whutils.ToProviderCase(whutils.SnowpipeStreaming, "id")]
				receivedAt, receivedAtExists := event.Message.Data[whutils.ToProviderCase(whutils.SnowpipeStreaming, "received_at")]

				if !idExists || !receivedAtExists {
					continue
				}

				discardedRecords = append(discardedRecords, discardInfo{
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
		if reflect.TypeOf(event.Message.Data[columnName]) == sliceType {
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

// convertDiscardedInfosToRows converts discardInfo to model.Row
func convertDiscardedInfosToRows(discardInfos []discardInfo) []model.Row {
	return lo.FilterMap(discardInfos, func(info discardInfo, _ int) (model.Row, bool) {
		return model.Row{
			"column_name":  info.columnName,
			"column_value": fmt.Sprintf("%v", info.columnValue),
			"reason":       info.reason,
			"received_at":  info.receivedAt,
			"row_id":       info.rowID,
			"table_name":   info.tableName,
			"uuid_ts":      info.uuidTS,
		}, true
	})
}
