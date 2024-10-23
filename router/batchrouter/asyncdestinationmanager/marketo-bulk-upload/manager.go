package marketobulkupload

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

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

	return NewMarketoBulkUploader(destName, statsFactory, destConfig), nil
}

func NewMarketoBulkUploader(destinationName string, statsFactory stats.Stats, destConfig MarketoConfig) *MarketoBulkUploader {
	authService := &MarketoAuthService{
		munchkinId:   destConfig.MunchkinId,
		clientId:     destConfig.ClientId,
		clientSecret: destConfig.ClientSecret,
		httpCLient:   &http.Client{},
	}

	apiService := &MarketoAPIService{
		logger:       logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Marketo").Child("Marketo Builk Upload").Child("API Service"),
		statsFactory: statsFactory,
		httpClient:   &http.Client{},
		munchkinId:   destConfig.MunchkinId,
		authService:  authService,
		maxRetries:   3,
	}

	return &MarketoBulkUploader{
		destName:          destinationName,
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Marketo").Child("Marketo Builk Upload"),
		destinationConfig: destConfig,
		dataHashToJobId:   make(map[string]int64),
		statsFactory:      statsFactory,
		apiService:        apiService,
	}
}
