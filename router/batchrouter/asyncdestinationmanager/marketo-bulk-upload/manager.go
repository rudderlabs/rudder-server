package marketobulkupload

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

// MarketoBulkUploaderOptions contains all dependencies needed for the uploader
type MarketoBulkUploaderOptions struct {
	DestinationName   string
	DestinationConfig MarketoConfig
	Logger            logger.Logger
	StatsFactory      stats.Stats
	APIService        MarketoAPIServiceInterface
}

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*MarketoBulkUploader, error) {
	destConfig := MarketoConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	destName := destination.DestinationDefinition.Name

	marketoHttpClient := getDefaultHTTPClient()

	return NewMarketoBulkUploader(destName, logger, statsFactory, marketoHttpClient, destConfig), nil
}

func NewMarketoBulkUploader(destinationName string, log logger.Logger, statsFactory stats.Stats, httpClient *http.Client, destConfig MarketoConfig) *MarketoBulkUploader {
	authService := &MarketoAuthService{
		munchkinId:   destConfig.MunchkinId,
		clientId:     destConfig.ClientId,
		clientSecret: destConfig.ClientSecret,
		httpCLient:   httpClient,
	}

	apiService := &MarketoAPIService{
		logger:       log.Child("batchRouter").Child("AsyncDestinationManager").Child("Marketo").Child("Marketo Builk Upload").Child("API Service"),
		statsFactory: statsFactory,
		httpClient:   httpClient,
		munchkinId:   destConfig.MunchkinId,
		authService:  authService,
		maxRetries:   3,
	}

	return NewMarketoBulkUploaderWithOptions(MarketoBulkUploaderOptions{
		DestinationName:   destinationName,
		Logger:            log.Child("batchRouter").Child("AsyncDestinationManager").Child("Marketo").Child("Marketo Builk Upload"),
		DestinationConfig: destConfig,
		StatsFactory:      statsFactory,
		APIService:        apiService,
	})
}

// NewMarketoBulkUploaderWithOptions creates a new MarketoBulkUploader with the given options
func NewMarketoBulkUploaderWithOptions(options MarketoBulkUploaderOptions) *MarketoBulkUploader {
	return &MarketoBulkUploader{
		destName:          options.DestinationName,
		logger:            options.Logger,
		destinationConfig: options.DestinationConfig,
		statsFactory:      options.StatsFactory,
		apiService:        options.APIService,
		dataHashToJobId:   make(map[string]int64),
	}
}
