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

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/warehouse/integrations/manager"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
	"github.com/rudderlabs/rudder-server/warehouse/validations"
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
		validator           validations.DestinationValidator

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
			instanceID        string
			maxBufferCapacity config.ValueLoader[int64]
			backoff           struct {
				multiplier      config.ValueLoader[float64]
				initialInterval config.ValueLoader[time.Duration]
				maxInterval     config.ValueLoader[time.Duration]
			}
		}
		backoff struct {
			// If an attempt was made to create a resource but it failed likely due to permission issues,
			// then the next attempt to create a SF connection will be made after "next".
			// This approach prevents repeatedly activating the warehouse even though the permission issue remains unresolved.
			attempts int
			next     time.Time
			error    string
		}

		stats struct {
			jobs struct {
				importing stats.Counter
				succeeded stats.Counter
				failed    stats.Counter
				aborted   stats.Counter
			}
			discards          stats.Counter
			pollingInProgress stats.Counter
		}
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
	}

	importInfo struct {
		ChannelID string `json:"channelId"`
		Offset    string `json:"offset"`
		Table     string `json:"table"`
		Failed    bool   `json:"failed"`
		Reason    string `json:"reason"`
		Count     int    `json:"count"`
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
	}

	apiAdapter struct {
		logger       logger.Logger
		statsFactory stats.Stats
		destination  *backendconfig.DestinationT
		api
	}
)

func (d *destConfig) Decode(m map[string]interface{}) error {
	if err := mapstructure.Decode(m, d); err != nil {
		return err
	}
	d.Namespace = whutils.ToProviderCase(
		whutils.SnowpipeStreaming,
		whutils.ToSafeNamespace(whutils.SnowpipeStreaming, d.Namespace),
	)
	return nil
}

func (e *event) setUUIDTimestamp(formattedTimestamp string) {
	if e.Message.Metadata.Columns == nil {
		return
	}
	uuidTimestampColumn := whutils.ToProviderCase(whutils.SnowpipeStreaming, "uuid_ts")
	if _, columnExists := e.Message.Metadata.Columns[uuidTimestampColumn]; columnExists {
		e.Message.Data[uuidTimestampColumn] = formattedTimestamp
	}
}
