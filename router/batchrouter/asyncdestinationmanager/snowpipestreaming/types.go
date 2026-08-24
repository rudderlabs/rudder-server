package snowpipestreaming

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type (
	Manager struct {
		appConfig           *config.Config
		logger              logger.Logger
		statsFactory        stats.Stats
		destination         *backendconfig.DestinationT
		requestDoer         requestDoer
		managerCreator      func(ctx context.Context, modelWarehouse whutils.ModelWarehouse, conf *config.Config, logger logger.Logger, statsFactory stats.Stats) (manager.Manager, error)
		now                 func() time.Time
		api                 api
		channelCache        sync.Map
		polledImportInfoMap map[string]*importInfo
		// Tracks how the committed offset of each in-progress channel moved within the current
		// batch, used at the stuck threshold to tell a stalled channel from a slow one.
		committedOffsets map[string]*committedOffsetProgress

		config struct {
			client struct {
				url                    string
				maxHTTPConnections     int
				maxHTTPIdleConnections int
				maxIdleConnDuration    time.Duration
				disableKeepAlives      bool
				timeoutDuration        time.Duration
				retryWaitMin           time.Duration
				retryWaitMax           time.Duration
				retryMax               int
			}
			instanceID                string
			maxBufferCapacity         config.ValueLoader[int64]
			stuckPipelineThreshold    config.ValueLoader[time.Duration]
			bulkStatusBatchSize       config.ValueLoader[int]
			maxInsertRequestSizeBytes config.ValueLoader[int64]
		}

		stats struct {
			jobs struct {
				importing stats.Counter
				succeeded stats.Counter
				failed    stats.Counter
				aborted   stats.Counter
			}
			discards                   stats.Counter
			pollingInProgress          stats.Counter
			duplicateEventsInBatch     stats.Counter
			duplicateEventsDueToOffset stats.Counter
		}

		// Track batch polling start time for stuck pipeline detection
		pollingStartTime time.Time
	}

	requestDoer interface {
		Do(*http.Request) (*http.Response, error)
	}

	event struct {
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
		MessageDataByteSize int64 `json:"-"` // Added to track the size of message.data field in bytes
	}

	destConfig struct {
		Account              string `mapstructure:"account"`
		Warehouse            string `mapstructure:"warehouse"`
		Database             string `mapstructure:"database"`
		User                 string `mapstructure:"user"`
		Role                 string `mapstructure:"role"`
		PrivateKey           string `mapstructure:"privateKey"`
		PrivateKeyPassphrase string `mapstructure:"privateKeyPassphrase"`
		Namespace            string `mapstructure:"namespace"`
		EnableIceberg        bool   `mapstructure:"enableIceberg"`
	}

	failedJobIds struct {
		Start int64 `json:"start"`
		End   int64 `json:"end"`
	}

	// committedOffsetProgress records how a channel's latest committed offset moved across the polls
	// of a single batch.
	committedOffsetProgress struct {
		lastOffset int64
		samples    int
		advanced   bool
	}

	// stuckChannelCause classifies why a channel was still in progress at the stuck threshold.
	stuckChannelCause string

	importInfo struct {
		ChannelID string `json:"channelId"`
		Offset    string `json:"offset"`
		Table     string `json:"table"`
		// Is set to true if all/some jobs have failed.
		Failed bool   `json:"failed"`
		Reason string `json:"reason"`
		Count  int    `json:"count"`
		// Marks a specific range of failed job IDs (partial failure).
		// If all jobs have failed, this field may be nil.
		FailedJobIds *failedJobIds `json:"failedJobIds,omitempty"`
	}

	discardInfo struct {
		tableName   string
		columnName  string
		columnValue any
		reason      string
		uuidTS      string
		rowID       any
		receivedAt  any
	}

	uploadInfo struct {
		tableName              string
		events                 []*event
		jobIDs                 []int64
		eventsSchema           whutils.ModelTableSchema
		discardChannelResponse *model.ChannelResponse
		latestJobID            int64
	}

	api interface {
		CreateChannel(ctx context.Context, channelReq *model.CreateChannelRequest) (*model.ChannelResponse, error)
		DeleteChannel(ctx context.Context, channelID string, sync bool) error
		Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error)
		GetStatus(ctx context.Context, channelID string) (*model.StatusResponse, error)
		GetBulkStatus(ctx context.Context, channelIDs []string) (*model.BulkStatusResponse, error)
	}

	apiAdapter struct {
		logger       logger.Logger
		statsFactory stats.Stats
		destination  *backendconfig.DestinationT
		api
	}
)

const (
	// causeChannelInvalid: the per-channel status endpoint positively reports the channel as invalid.
	causeChannelInvalid stuckChannelCause = "channel_invalid"
	// causeNotPersisting: the channel is valid and holds every row we sent, but Snowpipe stopped
	// committing them.
	causeNotPersisting stuckChannelCause = "not_persisting"
	// causeStuck: everything else, including an inconclusive status check.
	causeStuck stuckChannelCause = "stuck"
)

func (d *destConfig) Decode(m map[string]any) error {
	if err := mapstructure.Decode(m, d); err != nil {
		return err
	}
	d.Namespace = whutils.ToProviderCase(
		whutils.SnowpipeStreaming,
		whutils.ToSafeNamespace(whutils.SnowpipeStreaming, d.Namespace),
	)
	return nil
}

func (e *event) setUUIDTimestamp(formattedTimestamp string) bool {
	if e.Message.Metadata.Columns == nil {
		return false
	}
	uuidTimestampColumn := whutils.ToProviderCase(whutils.SnowpipeStreaming, "uuid_ts")
	if _, columnExists := e.Message.Metadata.Columns[uuidTimestampColumn]; columnExists {
		e.Message.Data[uuidTimestampColumn] = formattedTimestamp
		return true
	}
	return false
}
