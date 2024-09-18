package snowpipestreaming

import (
	"bufio"
	"context"
	stdjson "encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/logger"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

func (m *Manager) Upload(asyncDest *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.logger.Infon("Uploading data to snowpipe streaming destination")

	var destConf destConfig
	err := mapstructure.Decode(asyncDest.Destination.Config, &destConf)
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("failed to decode destination config: %v", err).Error())
	}

	events, err := m.eventsFromFile(asyncDest.FileName)
	if err != nil {
		return m.abortJobs(asyncDest, fmt.Errorf("failed to read events from file: %v", err).Error())
	}
	m.logger.Infon("Read events from file", logger.NewIntField("events", int64(len(events))))

	failedJobIDs, successJobIDs, uploadInfos := m.handleEvents(asyncDest, events, destConf)

	var importParameters stdjson.RawMessage
	if len(uploadInfos) > 0 {
		importIDBytes, err := json.Marshal(uploadInfos)
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to marshal import id: %v", err).Error())
		}

		importParameters, err = json.Marshal(common.ImportParameters{
			ImportId: string(importIDBytes),
		})
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to marshal import parameters: %v", err).Error())
		}
	}
	m.logger.Infon("Uploaded data to snowpipe streaming destination")

	m.stats.failedJobCount.Count(len(failedJobIDs))
	m.stats.successJobCount.Count(len(successJobIDs))

	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobIDs,
		ImportingCount:      len(successJobIDs),
		ImportingParameters: importParameters,
		FailedJobIDs:        failedJobIDs,
		FailedCount:         len(failedJobIDs),
		DestinationID:       asyncDest.Destination.ID,
	}
}

func (m *Manager) eventsFromFile(fileName string) ([]event, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fileName, err)
	}
	defer func() {
		_ = file.Close()
	}()

	var events []event

	scanner := bufio.NewScanner(file)
	scanner.Buffer(nil, int(m.config.maxBufferCapacity.Load()))

	for scanner.Scan() {
		var e event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event: %v", err)
		}

		events = append(events, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading from file: %v", err)
	}
	return events, nil
}

func (m *Manager) handleEvents(
	asyncDest *common.AsyncDestinationStruct,
	events []event,
	destConf destConfig,
) (
	failedJobIDs []int64,
	successJobIDs []int64,
	uploadInfos []*uploadInfo,
) {
	var (
		discardInfos []discardInfo
		mu           sync.Mutex
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(m.config.maxConcurrentUploadWorkers.Load())

	groupedEvents := lo.GroupBy(events, func(event event) string {
		return event.Message.Metadata.Table
	})
	for tableName, tableEvents := range groupedEvents {
		g.Go(func() error {
			jobIDs := lo.Map(tableEvents, func(event event, _ int) int64 {
				return event.Metadata.JobID
			})

			uploadTableInfo, discardTableInfo, err := m.loadTableEventsToSnowPipe(
				gCtx, asyncDest, destConf, tableName, tableEvents,
			)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				m.logger.Warnn("Failed to upload events to table",
					logger.NewStringField("table", tableName),
					obskit.Error(err),
				)

				failedJobIDs = append(failedJobIDs, jobIDs...)
				return nil
			}

			successJobIDs = append(successJobIDs, jobIDs...)
			uploadInfos = append(uploadInfos, uploadTableInfo)
			discardInfos = append(discardInfos, discardTableInfo...)
			return nil
		})
	}
	_ = g.Wait()

	if len(discardInfos) > 0 {
		discardUploadInfo, err := m.loadDiscardsToSnowPipe(ctx, asyncDest, destConf, discardInfos)
		if err != nil {
			m.logger.Warnn("Failed to upload events to discards table",
				logger.NewStringField("table", discardsTable()),
				obskit.Error(err),
			)
		} else {
			uploadInfos = append(uploadInfos, discardUploadInfo)
		}
	}
	return failedJobIDs, successJobIDs, uploadInfos
}

func (m *Manager) loadTableEventsToSnowPipe(
	ctx context.Context,
	asyncDest *common.AsyncDestinationStruct,
	destConf destConfig,
	tableName string,
	tableEvents []event,
) (*uploadInfo, []discardInfo, error) {
	log := m.logger.Withn(
		logger.NewStringField("table", tableName),
		logger.NewIntField("events", int64(len(tableEvents))),
	)
	log.Infon("Uploading data to table")

	eventSchema := schemaFromEvents(tableEvents)

	channelResponse, err := m.createChannel(ctx, asyncDest, destConf, tableName, eventSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("creating channel: %v", err)
	}
	snowPipeSchema := channelResponse.SnowPipeSchema()

	columnInfos := findNewColumns(eventSchema, snowPipeSchema)
	if len(columnInfos) > 0 {
		if err := m.addColumns(ctx, destConf.Namespace, tableName, columnInfos); err != nil {
			return nil, nil, fmt.Errorf("adding columns: %v", err)
		}

		channelResponse, err = m.recreateChannel(ctx, asyncDest, destConf, tableName, eventSchema, channelResponse)
		if err != nil {
			return nil, nil, fmt.Errorf("recreating channel: %v", err)
		}
		snowPipeSchema = channelResponse.SnowPipeSchema()
	}

	formattedTS := m.now().Format(misc.RFC3339Milli)
	for _, tableEvent := range tableEvents {
		tableEvent.setUUIDTimestamp(formattedTS)
	}

	discardInfos := lo.FlatMap(tableEvents, func(tableEvent event, _ int) []discardInfo {
		return discardedRecords(tableEvent, snowPipeSchema, tableName, formattedTS)
	})

	oldestEvent := lo.MaxBy(tableEvents, func(a, b event) bool {
		return a.Metadata.JobID > b.Metadata.JobID
	})
	offset := strconv.FormatInt(oldestEvent.Metadata.JobID, 10)

	insertReq := &model.InsertRequest{
		Rows: lo.Map(tableEvents, func(event event, _ int) model.Row {
			return event.Message.Data
		}),
		Offset: offset,
	}
	insertRes, err := m.api.Insert(ctx, channelResponse.ChannelID, insertReq)
	if err != nil {
		if deleteErr := m.deleteChannel(ctx, tableName, channelResponse.ChannelID); deleteErr != nil {
			log.Warnn("Failed to delete channel", obskit.Error(deleteErr))
		}
		return nil, nil, fmt.Errorf("inserting data: %v", err)
	}
	if !insertRes.Success {
		if deleteErr := m.deleteChannel(ctx, tableName, channelResponse.ChannelID); deleteErr != nil {
			log.Warnn("Failed to delete channel", obskit.Error(deleteErr))
		}
		return nil, nil, errInsertingDataFailed
	}
	log.Infon("Successfully uploaded data to table")

	info := &uploadInfo{
		ChannelID: channelResponse.ChannelID,
		Offset:    offset,
		Table:     tableName,
	}
	return info, discardInfos, nil
}

// schemaFromEvents Iterate over events and merge their columns into the final map
// Keeping the first type first serve basis
func schemaFromEvents(events []event) whutils.ModelTableSchema {
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
	m.stats.failedJobCount.Count(len(asyncDest.ImportingJobIDs))
	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDest.ImportingJobIDs,
		AbortCount:    len(asyncDest.ImportingJobIDs),
		AbortReason:   abortReason,
		DestinationID: asyncDest.Destination.ID,
	}
}
