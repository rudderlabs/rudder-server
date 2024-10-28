package snowpipestreaming

import (
	"bufio"
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

// Upload uploads data to the Snowpipe streaming destination.
// It reads events from the file, groups them by table, and sends them to Snowpipe.
// It returns the IDs of the importing and failed jobs.
// In case of failure, it aborts the jobs and returns the aborted job IDs.
func (m *Manager) Upload(asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.logger.Infon("Uploading data to snowpipe streaming destination")

	var destConf destConfig
	err := destConf.Decode(asyncDest.Destination.Config)
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("failed to decode destination config: %w", err).Error())
	}

	events, err := m.eventsFromFile(asyncDest.FileName, asyncDest.Count)
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("failed to read events from file: %w", err).Error())
	}
	m.logger.Infon("Read events from file", logger.NewIntField("events", int64(len(events))))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	discardsChannel, err := m.prepareChannelResponse(ctx, asyncDest.Destination.ID, &destConf, discardsTable(), discardsSchema())
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("failed to prepare discards channel: %w", err).Error())
	}
	m.logger.Infon("Prepared discards channel")

	groupedEvents := lo.GroupBy(events, func(event *event) string {
		return event.Message.Metadata.Table
	})
	uploadInfos := lo.MapToSlice(groupedEvents, func(tableName string, tableEvents []*event) *uploadInfo {
		jobIDs := lo.Map(tableEvents, func(event *event, _ int) int64 {
			return event.Metadata.JobID
		})
		latestJobID := lo.MaxBy(tableEvents, func(a, b *event) bool {
			return a.Metadata.JobID > b.Metadata.JobID
		})
		return &uploadInfo{
			tableName:              tableName,
			events:                 tableEvents,
			jobIDs:                 jobIDs,
			eventsSchema:           schemaFromEvents(tableEvents),
			discardChannelResponse: discardsChannel,
			latestJobID:            latestJobID.Metadata.JobID,
		}
	})
	slices.SortFunc(uploadInfos, func(a, b *uploadInfo) int {
		return int(a.latestJobID - b.latestJobID)
	})

	var (
		importingJobIDs, failedJobIDs   []int64
		importInfos, discardImportInfos []*importInfo
	)
	for _, info := range uploadInfos {
		imInfo, discardImInfo, err := m.sendEventsToSnowpipe(ctx, asyncDest.Destination.ID, &destConf, info)
		if err != nil {
			m.logger.Warnn("Failed to send events to Snowpipe",
				logger.NewStringField("table", info.tableName),
				obskit.Error(err),
			)

			failedJobIDs = append(failedJobIDs, info.jobIDs...)
			continue
		}

		importingJobIDs = append(importingJobIDs, info.jobIDs...)
		importInfos = append(importInfos, imInfo)

		if discardImInfo != nil {
			discardImportInfos = append(discardImportInfos, discardImInfo)
		}
	}
	if len(discardImportInfos) > 0 {
		importInfos = append(importInfos, discardImportInfos[len(discardImportInfos)-1])
	}

	var importParameters stdjson.RawMessage
	if len(importInfos) > 0 {
		importIDBytes, err := json.Marshal(importInfos)
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to marshal import id: %w", err).Error())
		}

		importParameters, err = json.Marshal(common.ImportParameters{
			ImportId: string(importIDBytes),
		})
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to marshal import parameters: %w", err).Error())
		}
	}
	m.logger.Infon("Uploaded data to snowpipe streaming destination")

	m.stats.jobs.importing.Count(len(importingJobIDs))
	m.stats.jobs.failed.Count(len(failedJobIDs))

	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		ImportingCount:      len(importingJobIDs),
		ImportingParameters: importParameters,
		FailedJobIDs:        failedJobIDs,
		FailedCount:         len(failedJobIDs),
		DestinationID:       asyncDest.Destination.ID,
	}
}

func (m *Manager) eventsFromFile(fileName string, eventsCount int) ([]*event, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	events := make([]*event, 0, eventsCount)

	formattedTS := m.now().Format(misc.RFC3339Milli)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(m.config.maxBufferCapacity.Load()))

	for scanner.Scan() {
		var e event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %w", err)
		}
		e.setUUIDTimestamp(formattedTS)

		events = append(events, &e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file: %w", err)
	}
	return events, nil
}

// sendEventsToSnowpipe sends events to Snowpipe for the given table.
// It creates a channel for the table, inserts the events into the channel, and sends the discard events to the discards table.
// It returns the import info for the table and the discard import info if any.
// In case of failure, it deletes the channel.
func (m *Manager) sendEventsToSnowpipe(
	ctx context.Context,
	destinationID string,
	destConf *destConfig,
	info *uploadInfo,
) (*importInfo, *importInfo, error) {
	offset := strconv.FormatInt(info.latestJobID, 10)

	log := m.logger.Withn(
		logger.NewStringField("table", info.tableName),
		logger.NewIntField("events", int64(len(info.events))),
		logger.NewStringField("offset", offset),
	)
	log.Infon("Sending events to Snowpipe")

	channelResponse, err := m.prepareChannelResponse(ctx, destinationID, destConf, info.tableName, info.eventsSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("creating channel %s: %w", info.tableName, err)
	}
	log.Infon("Prepared channel", logger.NewStringField("channelID", channelResponse.ChannelID))

	formattedTS := m.now().Format(misc.RFC3339Milli)
	discardInfos := lo.FlatMap(info.events, func(tableEvent *event, _ int) []discardInfo {
		return discardedRecords(tableEvent, channelResponse.SnowPipeSchema, info.tableName, formattedTS)
	})

	insertReq := &model.InsertRequest{
		Rows: lo.Map(info.events, func(event *event, _ int) model.Row {
			return event.Message.Data
		}),
		Offset: offset,
	}

	insertRes, err := m.api.Insert(ctx, channelResponse.ChannelID, insertReq)
	defer func() {
		if err != nil || !insertRes.Success {
			if deleteErr := m.deleteChannel(ctx, info.tableName, channelResponse.ChannelID); deleteErr != nil {
				log.Warnn("Failed to delete channel", obskit.Error(deleteErr))
			}
		}
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("inserting data %s: %w", info.tableName, err)
	}
	if !insertRes.Success {
		errorMessages := lo.Map(insertRes.Errors, func(ie model.InsertError, _ int) string {
			return ie.Message
		})
		return nil, nil, fmt.Errorf("inserting data %s failed: %s %v", info.tableName, insertRes.Code, errorMessages)
	}

	var discardImInfo *importInfo
	if len(discardInfos) > 0 {
		discardImInfo, err = m.sendDiscardEventsToSnowpipe(ctx, offset, info.discardChannelResponse.ChannelID, discardInfos)
		if err != nil {
			return nil, nil, fmt.Errorf("sending discard events to Snowpipe: %w", err)
		}
	}
	log.Infon("Sent events to Snowpipe")

	imInfo := &importInfo{
		ChannelID: channelResponse.ChannelID,
		Offset:    offset,
		Table:     info.tableName,
		Count:     len(info.events),
	}
	return imInfo, discardImInfo, nil
}

// schemaFromEvents builds a schema by iterating over events and merging their columns
// using a first-encountered type basis for each column.
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

func (m *Manager) abortJobs(asyncDest *common.AsyncDestinationStruct, abortReason string) common.AsyncUploadOutput {
	m.stats.jobs.aborted.Count(len(asyncDest.ImportingJobIDs))
	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDest.ImportingJobIDs,
		AbortCount:    len(asyncDest.ImportingJobIDs),
		AbortReason:   abortReason,
		DestinationID: asyncDest.Destination.ID,
	}
}
