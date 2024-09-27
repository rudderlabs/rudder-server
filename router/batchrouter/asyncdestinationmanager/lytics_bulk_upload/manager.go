package lyticsBulkUpload

import (
	"encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewLyticsBulkUploader(logger logger.Logger, statsFactory stats.Stats, destinationName, authorization, endpoint string, lytics LyticsService) common.AsyncUploadAndTransformManager {
	return &LyticsBulkUploader{
		destName:      destinationName,
		logger:        logger.Child("Lytics").Child("LyticsBulkUploader"),
		statsFactory:  statsFactory,
		authorization: authorization,
		baseEndpoint:  endpoint,
		fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 10*bytesize.MB),
		jobToCSVMap:   map[int64]int64{},
		service:       lytics,
	}
}

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := json.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = json.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}
	destName := destination.DestinationDefinition.Name

	lyticsService := &LyticsServiceImpl{}
	lyticsImpl := lyticsService.getBulkApi(destConfig)

	return common.SimpleAsyncDestinationManager{
		UploaderAndTransformer: NewLyticsBulkUploader(logger, statsFactory, destName, destConfig.LyticsApiKey, lyticsImpl.BulkApi, lyticsService),
	}, nil
}
