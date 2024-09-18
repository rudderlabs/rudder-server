package snowpipestreaming

import (
	"context"
	"fmt"
	"strconv"

	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/slave"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) loadDiscardsToSnowPipe(
	ctx context.Context,
	asyncDest *common.AsyncDestinationStruct,
	destConf destConfig,
	discardInfos []discardInfo,
) (*uploadInfo, error) {
	tableName, eventSchema := discardsTable(), discardsSchema()

	log := m.logger.Withn(
		logger.NewStringField("table", tableName),
		logger.NewIntField("events", int64(len(discardInfos))),
	)
	log.Infon("Uploading data to table")

	channelResponse, err := m.createChannel(ctx, asyncDest, destConf, tableName, eventSchema)
	if err != nil {
		return nil, fmt.Errorf("creating channel: %v", err)
	}

	columnInfos := findNewColumns(eventSchema, channelResponse.SnowPipeSchema())
	if len(columnInfos) > 0 {
		if err := m.addColumns(ctx, destConf.Namespace, tableName, columnInfos); err != nil {
			return nil, fmt.Errorf("adding columns: %v", err)
		}

		channelResponse, err = m.recreateChannel(ctx, asyncDest, destConf, tableName, eventSchema, channelResponse)
		if err != nil {
			return nil, fmt.Errorf("recreating channel: %v", err)
		}
	}

	offset := strconv.FormatInt(m.now().Unix(), 10)

	insertReq := &model.InsertRequest{
		Rows:   createRowsFromDiscardInfos(discardInfos),
		Offset: offset,
	}
	insertRes, err := m.api.Insert(ctx, channelResponse.ChannelID, insertReq)
	if err != nil {
		if deleteErr := m.deleteChannel(ctx, tableName, channelResponse.ChannelID); deleteErr != nil {
			m.logger.Warnn("Failed to delete channel",
				logger.NewStringField("table", tableName),
				obskit.Error(deleteErr),
			)
		}
		return nil, fmt.Errorf("inserting data: %v", err)
	}
	if !insertRes.Success {
		if deleteErr := m.deleteChannel(ctx, tableName, channelResponse.ChannelID); deleteErr != nil {
			m.logger.Warnn("Failed to delete channel",
				logger.NewStringField("table", tableName),
				obskit.Error(deleteErr),
			)
		}
		return nil, errInsertingDataFailed
	}
	m.logger.Infon("Successfully uploaded data to table",
		logger.NewStringField("table", tableName),
		logger.NewIntField("events", int64(len(discardInfos))),
	)
	m.stats.discardCount.Count(len(discardInfos))

	idOffset := &uploadInfo{
		ChannelID: channelResponse.ChannelID,
		Offset:    offset,
		Table:     tableName,
	}
	return idOffset, nil
}

func discardsTable() string {
	return whutils.ToProviderCase(whutils.SNOWFLAKE, whutils.DiscardsTable)
}

func discardsSchema() whutils.ModelTableSchema {
	return lo.MapEntries(whutils.DiscardsSchema, func(colName, colType string) (string, string) {
		return whutils.ToProviderCase(whutils.SNOWFLAKE, colName), colType
	})
}

func createRowsFromDiscardInfos(discardInfos []discardInfo) []model.Row {
	return lo.FilterMap(discardInfos, func(info discardInfo, _ int) (model.Row, bool) {
		id, idExists := info.eventData[whutils.ToProviderCase(whutils.SNOWFLAKE, "id")]
		receivedAt, receivedAtExists := info.eventData[whutils.ToProviderCase(whutils.SNOWFLAKE, "received_at")]

		if !idExists || !receivedAtExists {
			return nil, false
		}

		return model.Row{
			"column_name":  info.colName,
			"column_value": info.eventData[info.colName],
			"reason":       info.reason,
			"received_at":  receivedAt,
			"row_id":       id,
			"table_name":   info.table,
			"uuid_ts":      info.uuidTS,
		}, true
	})
}

func discardedRecords(
	event event,
	snowPipeSchema whutils.ModelTableSchema,
	tableName string,
	formattedTS string,
) (discardedRecords []discardInfo) {
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
	}
	return discardedRecords
}
