package snowpipestreaming

import (
	"context"
	"fmt"
	"reflect"

	"github.com/samber/lo"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

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

	log := m.logger.Withn(
		logger.NewStringField("table", tableName),
		logger.NewIntField("discards", int64(len(discardInfos))),
		logger.NewStringField("offset", offset),
	)

	insertReq := &model.InsertRequest{
		Rows:   convertDiscardedInfosToRows(discardInfos),
		Offset: offset,
	}
	insertRes, err := m.api.Insert(ctx, discardsChannelID, insertReq)
	defer func() {
		if err != nil || !insertRes.Success {
			if deleteErr := m.deleteChannel(ctx, tableName, discardsChannelID); deleteErr != nil {
				log.Warnn("Failed to delete channel", obskit.Error(deleteErr))
			}
		}
	}()
	if err != nil {
		return nil, fmt.Errorf("inserting data to discards: %v", err)
	}
	if !insertRes.Success {
		errorMessages := lo.Map(insertRes.Errors, func(ie model.InsertError, _ int) string {
			return ie.Message
		})
		return nil, fmt.Errorf("inserting data %s failed: %v", tableName, errorMessages)
	}

	m.stats.discards.Count(len(discardInfos))

	imInfo := &importInfo{
		ChannelID: discardsChannelID,
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
	return lo.MapEntries(whutils.DiscardsSchema, func(colName, colType string) (string, string) {
		return whutils.ToProviderCase(whutils.SnowpipeStreaming, colName), colType
	})
}

// getDiscardedRecordsFromEvent returns the records that were discarded due to schema mismatch
// It also updates the event data with the converted values
// If the conversion fails, the value is discarded
// If the value is a slice, it is marshalled to a string
func getDiscardedRecordsFromEvent(
	event *event,
	snowPipeSchema whutils.ModelTableSchema,
	tableName string,
	formattedTS string,
) (discardedRecords []discardInfo) {
	sliceType := reflect.TypeOf([]interface{}{})
	for colName, actualType := range event.Message.Metadata.Columns {
		if expectedType, exists := snowPipeSchema[colName]; exists && actualType != expectedType {
			if convertedVal, err := slave.HandleSchemaChange(expectedType, actualType, event.Message.Data[colName]); err != nil {
				// Discard value if conversion fails
				event.Message.Data[colName] = nil
				discardedRecords = append(discardedRecords, discardInfo{
					table:     tableName,
					colName:   colName,
					eventData: event.Message.Data,
					reason:    err.Error(),
					uuidTS:    formattedTS,
				})
			} else {
				// Update value if conversion succeeds
				event.Message.Data[colName] = convertedVal
			}
		}
		if reflect.TypeOf(event.Message.Data[colName]) == sliceType {
			marshalledVal, err := json.Marshal(event.Message.Data[colName])
			if err != nil {
				// Discard value if marshalling fails
				event.Message.Data[colName] = nil
			} else {
				event.Message.Data[colName] = string(marshalledVal)
			}
		}
	}
	return discardedRecords
}

// convertDiscardedInfosToRows converts discardInfo to model.Row
func convertDiscardedInfosToRows(discardInfos []discardInfo) []model.Row {
	return lo.FilterMap(discardInfos, func(info discardInfo, _ int) (model.Row, bool) {
		id, idExists := info.eventData[whutils.ToProviderCase(whutils.SnowpipeStreaming, "id")]
		receivedAt, receivedAtExists := info.eventData[whutils.ToProviderCase(whutils.SnowpipeStreaming, "received_at")]

		if !idExists || !receivedAtExists {
			return nil, false
		}

		return model.Row{
			"column_name":  info.colName,
			"column_value": fmt.Sprintf("%v", info.eventData[info.colName]),
			"reason":       info.reason,
			"received_at":  receivedAt,
			"row_id":       id,
			"table_name":   info.table,
			"uuid_ts":      info.uuidTS,
		}, true
	})
}
