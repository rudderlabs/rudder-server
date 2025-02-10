package snowpipestreaming

import (
	"bufio"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-retryablehttp"
	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stringify"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	snowpipeapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/utils/misc"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(
	conf *config.Config,
	log logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
) *Manager {
	m := &Manager{
		appConfig: conf,
		logger: log.Child("snowpipestreaming").Withn(
			obskit.WorkspaceID(destination.WorkspaceID),
			obskit.DestinationID(destination.ID),
			obskit.DestinationType(destination.DestinationDefinition.Name),
		),
		statsFactory:        statsFactory,
		destination:         destination,
		now:                 timeutil.Now,
		channelCache:        sync.Map{},
		polledImportInfoMap: make(map[string]*importInfo),
		validator:           validations.NewDestinationValidator(),
	}

	m.config.client.url = conf.GetString("SnowpipeStreaming.Client.URL", "http://localhost:9078")
	m.config.client.maxHTTPConnections = conf.GetInt("SnowpipeStreaming.Client.maxHTTPConnections", 10)
	m.config.client.maxHTTPIdleConnections = conf.GetInt("SnowpipeStreaming.Client.maxHTTPIdleConnections", 5)
	m.config.client.maxIdleConnDuration = conf.GetDuration("SnowpipeStreaming.Client.maxIdleConnDuration", 30, time.Second)
	m.config.client.disableKeepAlives = conf.GetBool("SnowpipeStreaming.Client.disableKeepAlives", true)
	m.config.client.timeoutDuration = conf.GetDuration("SnowpipeStreaming.Client.timeout", 300, time.Second)
	m.config.client.retryWaitMin = conf.GetDuration("SnowpipeStreaming.Client.retryWaitMin", 100, time.Millisecond)
	m.config.client.retryWaitMax = conf.GetDuration("SnowpipeStreaming.Client.retryWaitMax", 10, time.Second)
	m.config.client.retryMax = conf.GetInt("SnowpipeStreaming.Client.retryMax", 5)
	m.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	m.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "SnowpipeStreaming.maxBufferCapacity")
	m.config.backoff.initialInterval = conf.GetReloadableDurationVar(1, time.Second, "SnowpipeStreaming.backoffInitialIntervalInSeconds")
	m.config.backoff.multiplier = conf.GetReloadableFloat64Var(2.0, "SnowpipeStreaming.backoffMultiplier")
	m.config.backoff.maxInterval = conf.GetReloadableDurationVar(1, time.Hour, "SnowpipeStreaming.backoffMaxIntervalInHours")

	tags := stats.Tags{
		"module":        "batch_router",
		"workspaceId":   destination.WorkspaceID,
		"destType":      destination.DestinationDefinition.Name,
		"destinationId": destination.ID,
	}
	m.stats.jobs.importing = statsFactory.NewTaggedStat("snowpipe_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "importing",
	}))
	m.stats.jobs.succeeded = statsFactory.NewTaggedStat("snowpipe_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "succeeded",
	}))
	m.stats.jobs.failed = statsFactory.NewTaggedStat("snowpipe_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "failed",
	}))
	m.stats.jobs.aborted = statsFactory.NewTaggedStat("snowpipe_streaming_jobs", stats.CountType, lo.Assign(tags, stats.Tags{
		"status": "aborted",
	}))

	m.stats.discards = statsFactory.NewTaggedStat("snowpipe_streaming_discards", stats.CountType, tags)
	m.stats.pollingInProgress = statsFactory.NewTaggedStat("snowpipe_streaming_polling_in_progress", stats.CountType, tags)

	if m.requestDoer == nil {
		m.requestDoer = m.retryableClient().StandardClient()
	}

	m.api = newApiAdapter(
		m.logger,
		statsFactory,
		snowpipeapi.New(m.appConfig, m.statsFactory, m.config.client.url, m.requestDoer),
		destination,
	)
	m.managerCreator = func(ctx context.Context, modelWarehouse whutils.ModelWarehouse, conf *config.Config, logger logger.Logger, statsFactory stats.Stats) (manager.Manager, error) {
		sf, err := manager.New(whutils.SnowpipeStreaming, conf, logger, statsFactory)
		if err != nil {
			return nil, fmt.Errorf("creating snowflake manager: %w", err)
		}
		err = sf.Setup(ctx, modelWarehouse, whutils.NewNoOpUploader())
		if err != nil {
			return nil, fmt.Errorf("setting up snowflake manager: %w", err)
		}
		return sf, nil
	}
	return m
}

func (m *Manager) retryableClient() *retryablehttp.Client {
	client := retryablehttp.NewClient()
	client.HTTPClient = &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   m.config.client.disableKeepAlives,
			MaxConnsPerHost:     m.config.client.maxHTTPConnections,
			MaxIdleConnsPerHost: m.config.client.maxHTTPIdleConnections,
			IdleConnTimeout:     m.config.client.maxIdleConnDuration,
		},
		Timeout: m.config.client.timeoutDuration,
	}
	client.Logger = nil
	client.RetryWaitMin = m.config.client.retryWaitMin
	client.RetryWaitMax = m.config.client.retryWaitMax
	client.RetryMax = m.config.client.retryMax
	return client
}

func (m *Manager) validateConfig(ctx context.Context, dest *backendconfig.DestinationT) error {
	dest.Config["useKeyPairAuth"] = true // Since we are currently only supporting key pair auth
	response := m.validator.Validate(ctx, dest)
	if response.Success {
		return nil
	}
	return errors.New(response.Error)
}

func (m *Manager) Now() time.Time {
	return m.now()
}

func (m *Manager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

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
	m.logger.Infon("Read events from file",
		logger.NewIntField("events", int64(len(events))),
		logger.NewIntField("size", int64(asyncDest.Size)),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	discardsChannel, err := m.initializeChannelWithSchema(ctx, asyncDest.Destination.ID, &destConf, discardsTable(), discardsSchema())
	if err != nil {
		switch {
		case errors.Is(err, errAuthz):
			m.setBackOff(err)
			validationError := m.validateConfig(ctx, asyncDest.Destination)
			if validationError != nil {
				err = fmt.Errorf("failed to validate snowpipe credentials: %s", validationError.Error())
			}
			return m.failedJobs(asyncDest, err.Error())
		case errors.Is(err, errBackoff):
			return m.failedJobs(asyncDest, err.Error())
		default:
			return m.abortJobs(asyncDest, fmt.Errorf("failed to prepare discards channel: %w", err).Error())
		}
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
		importingJobIDs, failedJobIDs []int64
		importInfos                   []*importInfo
		discardImportInfo             *importInfo
		failedReason                  string
	)
	shouldResetBackoff := true // backoff should be reset if authz error is not encountered for any of the tables
	isBackoffSet := false      // should not be set again if already set
	for _, info := range uploadInfos {
		imInfo, discardImInfo, err := m.sendEventsToSnowpipe(ctx, asyncDest.Destination.ID, &destConf, info)
		if err != nil {
			switch {
			case errors.Is(err, errAuthz):
				shouldResetBackoff = false
				if !isBackoffSet {
					isBackoffSet = true
					m.setBackOff(err)
					validationError := m.validateConfig(ctx, asyncDest.Destination)
					if validationError != nil && failedReason == "" {
						failedReason = fmt.Sprintf("failed to validate snowpipe credentials: %s", validationError.Error())
					}
				}
			case errors.Is(err, errBackoff):
				shouldResetBackoff = false
			}
			m.logger.Warnn("Failed to send events to Snowpipe",
				logger.NewStringField("table", info.tableName),
				obskit.Error(err),
			)
			if failedReason == "" {
				failedReason = err.Error()
			}
			failedJobIDs = append(failedJobIDs, info.jobIDs...)
			continue
		}

		importingJobIDs = append(importingJobIDs, info.jobIDs...)
		importInfos = append(importInfos, imInfo)

		if discardImInfo != nil && discardImportInfo == nil {
			discardImportInfo = discardImInfo
		} else if discardImInfo != nil {
			discardImportInfo.Count += discardImInfo.Count
			discardImportInfo.Offset = discardImInfo.Offset
		}
	}
	if shouldResetBackoff {
		m.resetBackoff()
	}
	if discardImportInfo != nil {
		importInfos = append(importInfos, discardImportInfo)
	}

	var importParameters stdjson.RawMessage
	if len(importInfos) > 0 {
		importIDBytes, err := json.Marshal(importInfos)
		if err != nil {
			return m.abortJobs(asyncDest, fmt.Errorf("failed to marshal import id: %w", err).Error())
		}

		importParameters = stdjson.RawMessage(`{"importId":` + string(importIDBytes) + `}`)
	}
	m.logger.Infon("Uploaded data to snowpipe streaming destination")

	m.stats.jobs.importing.Count(len(importingJobIDs))
	m.stats.jobs.failed.Count(len(failedJobIDs))

	return common.AsyncUploadOutput{
		ImportingJobIDs:     importingJobIDs,
		ImportingCount:      len(importingJobIDs),
		ImportingParameters: importParameters,
		FailedJobIDs:        failedJobIDs,
		FailedReason:        failedReason,
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

	formattedTS := m.Now().Format(misc.RFC3339Milli)
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

	channelResponse, err := m.initializeChannelWithSchema(ctx, destinationID, destConf, info.tableName, info.eventsSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("creating channel %s: %w", info.tableName, err)
	}
	log.Infon("Prepared channel", logger.NewStringField("channelID", channelResponse.ChannelID))

	formattedTS := m.Now().Format(misc.RFC3339Milli)
	var discardInfos []discardInfo
	for _, tableEvent := range info.events {
		discardInfos = append(discardInfos, getDiscardedRecordsFromEvent(tableEvent, channelResponse.SnowpipeSchema, info.tableName, formattedTS)...)
	}

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

func (m *Manager) failedJobs(asyncDest *common.AsyncDestinationStruct, failedReason string) common.AsyncUploadOutput {
	m.stats.jobs.failed.Count(len(asyncDest.ImportingJobIDs))
	return common.AsyncUploadOutput{
		FailedJobIDs:  asyncDest.ImportingJobIDs,
		FailedCount:   len(asyncDest.ImportingJobIDs),
		FailedReason:  failedReason,
		DestinationID: asyncDest.Destination.ID,
	}
}

// Poll checks the status of multiple imports using the import ID from pollInput.
// For the once which have reached the terminal state (success or failure), it caches the import infos in polledImportInfoMap. Later if Poll is called again, it does not need to do the status check again.
// Once all the imports have reached the terminal state, if any imports have failed, it deletes the channels for those imports.
// It returns a PollStatusResponse indicating if any imports are still in progress or if any have failed or succeeded
func (m *Manager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	m.logger.Infon("Polling started")

	var importInfos []*importInfo
	err := json.Unmarshal([]byte(pollInput.ImportId), &importInfos)
	if err != nil {
		return common.PollStatusResponse{
			InProgress: false,
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Errorf("failed to unmarshal import id: %w", err).Error(),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if anyInProgress := m.processPollImportInfos(ctx, importInfos); anyInProgress {
		m.stats.pollingInProgress.Increment()
		return common.PollStatusResponse{InProgress: true}
	}

	updatedImportInfos := lo.Map(importInfos, func(item *importInfo, index int) *importInfo {
		return m.polledImportInfoMap[item.ChannelID]
	})
	failedImports := lo.Filter(updatedImportInfos, func(info *importInfo, index int) bool {
		return info.Failed
	})
	m.cleanupFailedImports(ctx, failedImports)
	m.updateJobStatistics(updatedImportInfos)
	m.polledImportInfoMap = make(map[string]*importInfo)

	return m.buildPollStatusResponse(updatedImportInfos, failedImports)
}

func (m *Manager) processPollImportInfos(ctx context.Context, infos []*importInfo) bool {
	var anyInProgress bool
	for i := range infos {
		info := infos[i]

		if _, alreadyProcessed := m.polledImportInfoMap[info.ChannelID]; alreadyProcessed {
			continue
		}

		inProgress, err := m.getImportStatus(ctx, info)
		if err != nil {
			info.Failed = true
			info.Reason = err.Error()
			m.polledImportInfoMap[info.ChannelID] = info
			continue
		}
		if !inProgress {
			m.polledImportInfoMap[info.ChannelID] = info
		}

		anyInProgress = anyInProgress || inProgress
	}
	return anyInProgress
}

func (m *Manager) getImportStatus(ctx context.Context, info *importInfo) (bool, error) {
	log := m.logger.Withn(
		logger.NewStringField("channelId", info.ChannelID),
		logger.NewStringField("offset", info.Offset),
		logger.NewStringField("table", info.Table),
	)
	log.Infon("Polling for import info")

	statusRes, err := m.api.GetStatus(ctx, info.ChannelID)
	if err != nil {
		return false, fmt.Errorf("getting status: %w", err)
	}
	log.Infon("Polled import info",
		logger.NewBoolField("success", statusRes.Success),
		logger.NewStringField("polledOffset", statusRes.Offset),
		logger.NewBoolField("valid", statusRes.Valid),
		logger.NewBoolField("completed", statusRes.Offset == info.Offset),
	)
	if !statusRes.Valid || !statusRes.Success {
		return false, fmt.Errorf("invalid status response with valid: %t, success: %t", statusRes.Valid, statusRes.Success)
	}
	return statusRes.Offset != info.Offset, nil
}

func (m *Manager) cleanupFailedImports(ctx context.Context, failedInfos []*importInfo) {
	for _, info := range failedInfos {
		m.logger.Warnn("Failed to poll channel offset",
			logger.NewStringField("channelId", info.ChannelID),
			logger.NewStringField("offset", info.Offset),
			logger.NewStringField("table", info.Table),
			logger.NewStringField("reason", info.Reason),
		)

		if err := m.deleteChannel(ctx, info.Table, info.ChannelID); err != nil {
			m.logger.Warnn("Failed to delete channel",
				logger.NewStringField("channelId", info.ChannelID),
				logger.NewStringField("table", info.Table),
				obskit.Error(err),
			)
		}
	}
}

func (m *Manager) updateJobStatistics(importInfos []*importInfo) {
	var successfulCount, failedCount int

	for _, info := range importInfos {
		if info.Failed {
			failedCount += info.Count
		} else {
			successfulCount += info.Count
		}
	}
	m.stats.jobs.failed.Count(failedCount)
	m.stats.jobs.succeeded.Count(successfulCount)
}

func (m *Manager) buildPollStatusResponse(importInfos, failedImports []*importInfo) common.PollStatusResponse {
	response := common.PollStatusResponse{
		InProgress: false,
		StatusCode: http.StatusOK,
		Complete:   true,
	}

	if len(failedImports) > 0 {
		response.HasFailed = true
		response.FailedJobParameters = stringify.Any(importInfos)
	} else {
		response.HasFailed = false
		response.HasWarning = false
	}
	return response
}

// GetUploadStats returns the status of the uploads for the snowpipe streaming destination.
// It returns the status of the uploads for the given job IDs.
// If any of the uploads have failed, it returns the reason for the failure.
func (m *Manager) GetUploadStats(input common.GetUploadStatsInput) common.GetUploadStatsResponse {
	m.logger.Infon("Getting import stats for snowpipe streaming destination")

	var importInfos []*importInfo
	err := json.Unmarshal([]byte(input.FailedJobParameters), &importInfos)
	if err != nil {
		return common.GetUploadStatsResponse{
			StatusCode: http.StatusBadRequest,
			Error:      fmt.Errorf("failed to unmarshal failed job urls: %w", err).Error(),
		}
	}

	succeededTables, failedTables := make(map[string]struct{}), make(map[string]*importInfo)
	for _, info := range importInfos {
		if info.Failed {
			failedTables[info.Table] = info
		} else {
			succeededTables[info.Table] = struct{}{}
		}
	}

	var (
		succeededJobIDs  []int64
		failedJobIDs     []int64
		failedJobReasons = make(map[int64]string)
	)
	for _, job := range input.ImportingList {
		tableName := gjson.GetBytes(job.EventPayload, "metadata.table").String()
		if _, ok := succeededTables[tableName]; ok {
			succeededJobIDs = append(succeededJobIDs, job.JobID)
		}
		if info, ok := failedTables[tableName]; ok {
			failedJobIDs = append(failedJobIDs, job.JobID)
			failedJobReasons[job.JobID] = info.Reason
		}
	}
	return common.GetUploadStatsResponse{
		StatusCode: http.StatusOK,
		Metadata: common.EventStatMeta{
			FailedKeys:    failedJobIDs,
			FailedReasons: failedJobReasons,
			SucceededKeys: succeededJobIDs,
		},
	}
}

func (m *Manager) isInBackoff() bool {
	if m.backoff.next.IsZero() {
		return false
	}
	return m.Now().Before(m.backoff.next)
}

func (m *Manager) resetBackoff() {
	m.backoff.next = time.Time{}
	m.backoff.attempts = 0
	m.backoff.error = ""
}

func (m *Manager) setBackOff(err error) {
	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(m.config.backoff.initialInterval.Load()),
		backoff.WithMultiplier(m.config.backoff.multiplier.Load()),
		backoff.WithClockProvider(m),
		backoff.WithRandomizationFactor(0),
		backoff.WithMaxElapsedTime(0),
		backoff.WithMaxInterval(m.config.backoff.maxInterval.Load()),
	)
	b.Reset()
	m.backoff.attempts++
	m.backoff.error = err.Error()

	var d time.Duration
	for index := int64(0); index < int64(m.backoff.attempts); index++ {
		d = b.NextBackOff()
	}
	m.backoff.next = m.Now().Add(d)
}
