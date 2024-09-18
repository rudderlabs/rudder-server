package snowpipestreaming

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	snowpipeapi "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/api"
	"github.com/rudderlabs/rudder-server/utils/timeutil"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	errInvalidStatusResponse = errors.New("invalid status response")
	errInsertingDataFailed   = errors.New("inserting data failed")
)

func New(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	opts ...Opt,
) *Manager {
	m := &Manager{
		conf: conf,
		logger: logger.Child("snowpipestreaming").Withn(
			obskit.WorkspaceID(destination.WorkspaceID),
			obskit.DestinationID(destination.ID),
			obskit.DestinationType(destination.DestinationDefinition.Name),
		),
		statsFactory: statsFactory,
		destination:  destination,
		now:          timeutil.Now,
		channelCache: sync.Map{},
	}
	for _, opt := range opts {
		opt(m)
	}

	m.config.client.maxHTTPConnections = conf.GetInt("SnowpipeStreaming.Client.maxHTTPConnections", 10)
	m.config.client.maxHTTPIdleConnections = conf.GetInt("SnowpipeStreaming.Client.maxHTTPIdleConnections", 5)
	m.config.client.maxIdleConnDuration = conf.GetDuration("SnowpipeStreaming.Client.maxIdleConnDuration", 30, time.Second)
	m.config.client.disableKeepAlives = conf.GetBool("SnowpipeStreaming.Client.disableKeepAlives", true)
	m.config.client.timeoutDuration = conf.GetDuration("SnowpipeStreaming.Client.timeout", 300, time.Second)
	m.config.client.retryWaitMin = conf.GetDuration("SnowpipeStreaming.Client.retryWaitMin", 100, time.Millisecond)
	m.config.client.retryWaitMax = conf.GetDuration("SnowpipeStreaming.Client.retryWaitMax", 10, time.Second)
	m.config.client.retryMax = conf.GetInt("SnowpipeStreaming.Client.retryWaitMin", 5)
	m.config.clientURL = conf.GetString("SnowpipeStreaming.Client.URL", "http://localhost:9078")
	m.config.instanceID = conf.GetString("INSTANCE_ID", "1")
	m.config.pollFrequency = conf.GetDuration("SnowpipeStreaming.pollFrequency", 300, time.Millisecond)
	m.config.maxBufferCapacity = conf.GetReloadableInt64Var(512*bytesize.KB, bytesize.B, "SnowpipeStreaming.maxBufferCapacity")
	m.config.maxConcurrentPollWorkers = conf.GetReloadableIntVar(10, 1, "SnowpipeStreaming.maxConcurrentPollWorkers")
	m.config.maxConcurrentUploadWorkers = conf.GetReloadableIntVar(8, 1, "SnowpipeStreaming.maxConcurrentUploadWorkers")

	tags := stats.Tags{
		"module":        "batch_router",
		"workspaceId":   destination.WorkspaceID,
		"destType":      destination.DestinationDefinition.Name,
		"destinationId": destination.ID,
	}
	m.stats.successJobCount = statsFactory.NewTaggedStat("snowpipestreaming_success_job_count", stats.CountType, tags)
	m.stats.failedJobCount = statsFactory.NewTaggedStat("snowpipestreaming_failed_jobs_count", stats.CountType, tags)
	m.stats.discardCount = statsFactory.NewTaggedStat("snowpipestreaming_discards_count", stats.CountType, tags)
	m.stats.channelSchemaCreationErrorCount = statsFactory.NewTaggedStat("snowpipestreaming_create_channel_schema_error", stats.CountType, tags)
	m.stats.channelTableCreationErrorCount = statsFactory.NewTaggedStat("snowpipestreaming_create_channel_table_error", stats.CountType, tags)

	if m.requestDoer == nil {
		m.requestDoer = m.retryableClient().StandardClient()
	}

	m.api = newApiAdapter(
		snowpipeapi.New(m.config.clientURL, m.requestDoer),
		statsFactory,
		destination,
	)
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
