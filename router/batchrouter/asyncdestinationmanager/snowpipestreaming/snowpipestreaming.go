package snowpipestreaming

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jobsdb"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/snowflake"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type requestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type event struct {
	Message struct {
		Metadata struct {
			Table   string            `json:"table"`
			Columns map[string]string `json:"columns"`
		} `json:"metadata"`
		Data map[string]any `json:"data"`
	} `json:"message"`
	Metadata struct {
		JobID int64 `json:"job_id"`
	}
}

type destConfig struct {
	Account              string `mapstructure:"account"`
	Warehouse            string `mapstructure:"warehouse"`
	Database             string `mapstructure:"database"`
	User                 string `mapstructure:"user"`
	Role                 string `mapstructure:"role"`
	PrivateKey           string `mapstructure:"privateKey"`
	PrivateKeyPassphrase string `mapstructure:"privateKeyPassphrase"`
	Namespace            string `mapstructure:"namespace"`
}

type channelIDOffset struct {
	ChannelID string `json:"channelId"`
	Offset    string `json:"offset"`
}

type Manager struct {
	conf         *config.Config
	logger       logger.Logger
	statsFactory stats.Stats
	destination  *backendconfig.DestinationT
	requestDoer  requestDoer

	config struct {
		client struct {
			maxHTTPConnections     int
			maxHTTPIdleConnections int
			maxIdleConnDuration    time.Duration
			disableKeepAlives      bool
			timeoutDuration        time.Duration
		}

		clientURL     string
		instanceID    string
		pollFrequency time.Duration
	}

	stats struct {
		successJobCount stats.Counter
		failedJobCount  stats.Counter
	}
}

func New(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	opts ...Opt,
) *Manager {
	m := &Manager{
		conf:         conf,
		logger:       logger.Child("snowpipestreaming").Withn(obskit.WorkspaceID(destination.WorkspaceID), obskit.DestinationID(destination.ID)),
		statsFactory: statsFactory,
		destination:  destination,
	}
	m.config.client.maxHTTPConnections = conf.GetInt("Snowpipe.Client.maxHTTPConnections", 20)
	m.config.client.maxHTTPIdleConnections = conf.GetInt("Snowpipe.Client.maxHTTPIdleConnections", 10)
	m.config.client.maxIdleConnDuration = conf.GetDuration("Snowpipe.Client.maxIdleConnDuration", 30, time.Second)
	m.config.client.disableKeepAlives = conf.GetBool("Snowpipe.Client.disableKeepAlives", true)
	m.config.client.timeoutDuration = conf.GetDuration("Snowpipe.Client.timeout", 600, time.Second)
	m.config.clientURL = conf.GetString("Snowpipe.Client.URL", "http://localhost:9078")
	m.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	m.config.pollFrequency = conf.GetDuration("Snowpipe.Client.pollFrequency", 1, time.Second)

	m.stats.failedJobCount = statsFactory.NewTaggedStat("snowpipestreaming_failed_jobs_count", stats.CountType, stats.Tags{
		"module":   "batch_router",
		"destType": destination.DestinationDefinition.Name,
	})
	m.stats.successJobCount = statsFactory.NewTaggedStat("snowpipestreaming_success_job_count", stats.CountType, stats.Tags{
		"module":   "batch_router",
		"destType": destination.DestinationDefinition.Name,
	})

	for _, opt := range opts {
		opt(m)
	}
	if m.requestDoer == nil {
		m.requestDoer = &http.Client{
			Transport: &http.Transport{
				DisableKeepAlives:   m.config.client.disableKeepAlives,
				MaxConnsPerHost:     m.config.client.maxHTTPConnections,
				MaxIdleConnsPerHost: m.config.client.maxHTTPIdleConnections,
				IdleConnTimeout:     m.config.client.maxIdleConnDuration,
			},
			Timeout: m.config.client.timeoutDuration,
		}
	}
	return m
}

func (m *Manager) Transform(job *jobsdb.JobT) (string, error) {
	return common.GetMarshalledData(string(job.EventPayload), job.JobID)
}

func (m *Manager) Upload(asyncDestStruct *common.AsyncDestinationStruct) common.AsyncUploadOutput {
	m.logger.Infon("Uploading data to snowpipe streaming destination")

	var destConfig destConfig
	err := mapstructure.Decode(asyncDestStruct.Destination.Config, &destConfig)
	if err != nil {
		return m.abortJobs(asyncDestStruct, fmt.Errorf("failed to decode destination config: %v", err).Error())
	}

	file, err := os.Open(asyncDestStruct.FileName)
	if err != nil {
		return m.abortJobs(asyncDestStruct, fmt.Errorf("failed to open file: %v", err).Error())
	}
	defer func() {
		_ = file.Close()
	}()

	var (
		events                      []event
		failedJobIDs, successJobIDs []int64
		channelIFOffsets            []channelIDOffset
	)

	ctx := context.Background()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var e event
		err = json.Unmarshal(scanner.Bytes(), &e)
		if err != nil {
			return m.abortJobs(asyncDestStruct, fmt.Errorf("failed to unmarshal event: %v", err).Error())
		}

		events = append(events, e)
	}
	m.logger.Infon("Read events from file", logger.NewIntField("events", int64(len(events))))

	eventsByTable := lo.GroupBy(events, func(event event) string {
		return event.Message.Metadata.Table
	})

	for table, tableEvents := range eventsByTable {
		m.logger.Infon("Uploading data to table", logger.NewStringField("table", table), logger.NewIntField("events", int64(len(tableEvents))))

		jobIDs := lo.Map(tableEvents, func(event event, _ int) int64 {
			return event.Metadata.JobID
		})

		channelID, offset, err := m.sendEventsToSnowpipe(ctx, asyncDestStruct, destConfig, table, tableEvents)
		if err != nil {
			m.logger.Warnn("Failed to send events to snowpipe", obskit.Error(err), logger.NewStringField("table", table))
			failedJobIDs = append(failedJobIDs, jobIDs...)
			continue
		}

		successJobIDs = append(successJobIDs, jobIDs...)
		channelIFOffsets = append(channelIFOffsets, channelIDOffset{
			ChannelID: channelID,
			Offset:    offset,
		})

		m.logger.Infon("Successfully uploaded data to table", logger.NewStringField("table", table), logger.NewIntField("events", int64(len(tableEvents))))
	}
	importID, err := json.Marshal(channelIFOffsets)
	if err != nil {
		return m.abortJobs(asyncDestStruct, fmt.Errorf("failed to marshal import id: %v", err).Error())
	}

	importParameters, err := json.Marshal(common.ImportParameters{
		ImportId: string(importID),
	})
	if err != nil {
		return m.abortJobs(asyncDestStruct, fmt.Errorf("failed to marshal import parameters: %v", err).Error())
	}

	m.logger.Infon("Successfully uploaded data to snowpipe streaming destination")

	m.stats.failedJobCount.Count(len(failedJobIDs))
	m.stats.successJobCount.Count(len(successJobIDs))

	return common.AsyncUploadOutput{
		ImportingJobIDs:     successJobIDs,
		ImportingParameters: importParameters,
		FailedJobIDs:        failedJobIDs,
		FailedCount:         len(failedJobIDs),
		DestinationID:       asyncDestStruct.Destination.ID,
	}
}

func (m *Manager) sendEventsToSnowpipe(
	ctx context.Context,
	asyncDestStruct *common.AsyncDestinationStruct,
	destConf destConfig,
	table string,
	events []event,
) (string, string, error) {
	tableSchema := tableSchemaFromEvents(events)

	channelReq := &createChannelRequest{
		RudderIdentifier: asyncDestStruct.Destination.ID,
		Partition:        m.config.instanceID,
		AccountConfig: accountConfig{
			Account:              destConf.Account,
			User:                 destConf.User,
			Role:                 destConf.Role,
			PrivateKey:           whutils.FormatPemContent(destConf.PrivateKey),
			PrivateKeyPassphrase: destConf.PrivateKeyPassphrase,
		},
		TableConfig: tableConfig{
			Database: destConf.Database,
			Schema:   destConf.Namespace,
			Table:    table,
		},
	}
	channelResponse, err := m.createChannelWithRetries(ctx, channelReq, tableSchema)
	if err != nil {
		return "", "", fmt.Errorf("failed to create channel: %v", err)
	}

	oldestEvent := lo.MaxBy(events, func(a, b event) bool {
		return a.Metadata.JobID > b.Metadata.JobID
	})
	offset := strconv.FormatInt(oldestEvent.Metadata.JobID, 10)

	insertReq := &insertRequest{
		Rows: lo.Map(events, func(event event, _ int) Row {
			return event.Message.Data
		}),
		Offset: offset,
	}
	channelID, offset, err := m.insertWithRetries(ctx, channelReq, channelResponse, insertReq, tableSchema)
	if err != nil {
		return "", "", fmt.Errorf("failed to insert data: %v", err)
	}
	return channelID, offset, nil
}

// Iterate over events and merge their columns into the final map
// Keeping the first type first serve basis
func tableSchemaFromEvents(events []event) whutils.ModelTableSchema {
	columnsMap := make(whutils.ModelTableSchema)
	for _, e := range events {
		for col, typ := range e.Message.Metadata.Columns {
			if _, ok := columnsMap[col]; !ok {
				columnsMap[col] = typ
			}
		}
	}
	return columnsMap
}

func (m *Manager) createChannelWithRetries(
	ctx context.Context,
	channelReq *createChannelRequest,
	tableSchema map[string]string,
) (*createChannelResponse, error) {
	res, err := m.createChannel(ctx, channelReq)
	if err == nil {
		return res, nil
	}

	// checking if the errors is around The supplied schema does not exist or is not authorized.
	if strings.Contains(err.Error(), "The supplied schema does not exist or is not authorized") {
		sm, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
		if err != nil {
			return nil, fmt.Errorf("creating snowflake manager: %v", err)
		}

		err = sm.CreateSchema(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating schema: %v", err)
		}

		err = sm.CreateTable(ctx, channelReq.TableConfig.Table, tableSchema)
		if err != nil {
			return nil, fmt.Errorf("creating table: %v", err)
		}

		return m.createChannel(ctx, channelReq)
	}

	// The supplied table does not exist or is not authorized.
	if strings.Contains(err.Error(), "The supplied table does not exist or is not authorized") {
		sm, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
		if err != nil {
			return nil, fmt.Errorf("creating snowflake manager: %v", err)
		}

		err = sm.CreateTable(ctx, channelReq.TableConfig.Table, tableSchema)
		if err != nil {
			return nil, fmt.Errorf("creating table: %v", err)
		}

		return m.createChannel(ctx, channelReq)
	}

	return nil, fmt.Errorf("creating channel: %v", err)
}

func (m *Manager) createSnowflakeManager(ctx context.Context, namespace string) (*snowflake.Snowflake, error) {
	warehouse := whutils.ModelWarehouse{
		Namespace:   namespace,
		Destination: *m.destination,
	}

	// Since currently we are using key pair auth for snowflake, we need to set this flag to true
	warehouse.Destination.Config["useKeyPairAuth"] = true

	sf := snowflake.New(m.conf, m.logger, m.statsFactory)
	err := sf.Setup(ctx, warehouse, &whutils.NopUploader{})
	if err != nil {
		return nil, fmt.Errorf("failed to setup snowflake manager: %v", err)
	}
	return sf, nil
}

func (m *Manager) insertWithRetries(
	ctx context.Context,
	channelReq *createChannelRequest,
	channelRes *createChannelResponse,
	insertReq *insertRequest,
	schemaInEvents whutils.ModelTableSchema,
) (string, string, error) {
	res, err := m.insert(ctx, channelRes.ChannelID, insertReq)
	if err == nil && res.Success {
		return channelRes.ChannelID, insertReq.Offset, nil
	}
	if err != nil {
		return "", "", fmt.Errorf("failed to insert data: %v", err)
	}
	extraColumns := res.extraColumns()

	sm, err := m.createSnowflakeManager(ctx, channelReq.TableConfig.Schema)
	if err != nil {
		return "", "", fmt.Errorf("creating snowflake manager: %v", err)
	}
	columnsInfo := lo.Map(extraColumns, func(col string, _ int) whutils.ColumnInfo {
		return whutils.ColumnInfo{
			Name: col,
			Type: schemaInEvents[col],
		}
	})

	err = sm.AddColumns(ctx, channelReq.TableConfig.Table, columnsInfo)
	if err != nil {
		return "", "", fmt.Errorf("adding columns: %v", err)
	}

	err = m.deleteChannel(ctx, channelReq)
	if err != nil {
		return "", "", fmt.Errorf("deleting channel: %v", err)
	}

	channelRes, err = m.createChannelWithRetries(ctx, channelReq, schemaInEvents)
	if err != nil {
		return "", "", fmt.Errorf("creating channel: %v", err)
	}

	res, err = m.insert(ctx, channelRes.ChannelID, insertReq)
	if err == nil && res.Success {
		return channelRes.ChannelID, insertReq.Offset, nil
	}
	if err != nil {
		return "", "", fmt.Errorf("failed to insert data: %v", err)
	}
	return "", "", fmt.Errorf("failed to insert data, success: %s", strconv.FormatBool(res.Success))
}

func (m *Manager) abortJobs(asyncDestStruct *common.AsyncDestinationStruct, abortReason string) common.AsyncUploadOutput {
	m.stats.failedJobCount.Count(len(asyncDestStruct.ImportingJobIDs))
	return common.AsyncUploadOutput{
		AbortJobIDs:   asyncDestStruct.ImportingJobIDs,
		AbortCount:    len(asyncDestStruct.ImportingJobIDs),
		AbortReason:   abortReason,
		DestinationID: asyncDestStruct.Destination.ID,
	}
}

func (m *Manager) Poll(pollInput common.AsyncPoll) common.PollStatusResponse {
	log := m.logger.Withn(logger.NewStringField("importId", pollInput.ImportId))
	log.Infon("Polling started")

	var channelIDOffsets []channelIDOffset
	err := json.Unmarshal([]byte(pollInput.ImportId), &channelIDOffsets)
	if err != nil {
		return common.PollStatusResponse{
			InProgress: false,
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Sprintf("failed to unmarshal import id: %v", err),
		}
	}

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)

	for _, idOffset := range channelIDOffsets {
		g.Go(func() error {
			for {
				log.Infon("Polling for channel", logger.NewStringField("channelId", idOffset.ChannelID))

				statusRes, err := m.status(ctx, idOffset.ChannelID)
				if err != nil {
					return fmt.Errorf("failed to get status: %v", err)
				}
				if !statusRes.Valid {
					log.Warnn("Invalid status response", logger.NewStringField("channelId", idOffset.ChannelID))
					return errors.New("invalid status response")
				}
				if statusRes.Offset != idOffset.Offset {
					log.Infon("Polling in progress", logger.NewStringField("channelId", idOffset.ChannelID))
					time.Sleep(m.config.pollFrequency)
					continue
				}

				log.Infon("Polling completed", logger.NewStringField("channelId", idOffset.ChannelID))
				break
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return common.PollStatusResponse{
			InProgress: false,
			StatusCode: http.StatusBadRequest,
			Complete:   true,
			HasFailed:  true,
			Error:      fmt.Errorf("failed to get status: %v", err).Error(),
		}
	}
	return common.PollStatusResponse{
		InProgress: false,
		StatusCode: http.StatusOK,
		Complete:   true,
		HasFailed:  false,
		HasWarning: false,
	}
}

func (m *Manager) GetUploadStats(UploadStatsInput common.GetUploadStatsInput) common.GetUploadStatsResponse {
	m.logger.Infon("Getting upload stats for snowpipe streaming destination")
	return common.GetUploadStatsResponse{}
}
