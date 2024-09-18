package marketobulkupload

import (
	stdjson "encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
)

func NewManager(destination *backendconfig.DestinationT) (*MarketoBulkUploader, error) {
	destConfig := MarketoConfig{}
	jsonConfig, err := stdjson.Marshal(destination.DestinationDefinition.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	destName := destination.DestinationDefinition.Name

	return NewMarketoBulkUploader(destName, destConfig), nil
}

func NewMarketoBulkUploader(destinationName string, destConfig MarketoConfig) *MarketoBulkUploader {
	return &MarketoBulkUploader{
		destName:          destinationName,
		logger:            logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Marketo").Child("Marketo Builk Upload"),
		destinationConfig: destConfig,
		dataHashToJobId:   make(map[string]int64),
	}
}
