package snowpipestreaming

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/snowpipestreaming/internal/model"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	whutils "github.com/rudderlabs/rudder-server/warehouse/utils"
)

type (
	Manager struct {
		conf         *config.Config
		logger       logger.Logger
		statsFactory stats.Stats
		destination  *backendconfig.DestinationT
		requestDoer  requestDoer
		now          func() time.Time
		api          api
		channelCache sync.Map

		config struct {
			client struct {
				maxHTTPConnections     int
				maxHTTPIdleConnections int
				maxIdleConnDuration    time.Duration
				disableKeepAlives      bool
				timeoutDuration        time.Duration
				retryWaitMin           time.Duration
				retryWaitMax           time.Duration
				retryMax               int
			}

			clientURL                  string
			instanceID                 string
			pollFrequency              time.Duration
			maxBufferCapacity          config.ValueLoader[int64]
			maxConcurrentPollWorkers   config.ValueLoader[int]
			maxConcurrentUploadWorkers config.ValueLoader[int]
		}

		stats struct {
			successJobCount                 stats.Counter
			failedJobCount                  stats.Counter
			discardCount                    stats.Counter
			channelSchemaCreationErrorCount stats.Counter
			channelTableCreationErrorCount  stats.Counter
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

	uploadInfo struct {
		ChannelID string `json:"channelId"`
		Offset    string `json:"offset"`
		Table     string `json:"table"`
		Failed    bool   `json:"failed"`
		Reason    string `json:"reason"`
	}

	discardInfo struct {
		table     string
		colName   string
		eventData map[string]any
		reason    string
		uuidTS    string
	}

	api interface {
		CreateChannel(ctx context.Context, channelReq *model.CreateChannelRequest) (*model.ChannelResponse, error)
		DeleteChannel(ctx context.Context, channelID string, sync bool) error
		Insert(ctx context.Context, channelID string, insertRequest *model.InsertRequest) (*model.InsertResponse, error)
		Status(ctx context.Context, channelID string) (*model.StatusResponse, error)
	}
)

func (e *event) setUUIDTimestamp(formattedTimestamp string) {
	uuidTimestampColumn := whutils.ToProviderCase(whutils.SNOWFLAKE, "uuid_ts")
	if _, columnExists := e.Message.Metadata.Columns[uuidTimestampColumn]; columnExists {
		e.Message.Data[uuidTimestampColumn] = formattedTimestamp
	}
}
