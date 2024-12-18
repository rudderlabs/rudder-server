package clevertapSegment

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

var jsonFast = jsoniter.ConfigCompatibleWithStandardLibrary

func NewClevertapBulkUploader(
	logger logger.Logger,
	statsFactory stats.Stats,
	destinationName,
	accessToken,
	appKey string,
	clevertapEndpoints *Endpoints,
	clevertap ClevertapService,
	connectionConfig *ConnectionConfig,
) common.AsyncUploadAndTransformManager {
	return &ClevertapBulkUploader{
		destName:                  destinationName,
		logger:                    logger.Child("Clevertap").Child("ClevertapBulkUploader"),
		statsFactory:              statsFactory,
		accessToken:               accessToken,
		appKey:                    appKey,
		presignedURLEndpoint:      clevertapEndpoints.BulkApi,
		notifyEndpoint:            clevertapEndpoints.NotifyApi,
		fileSizeLimit:             common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 5*bytesize.GB),
		jobToCSVMap:               map[int64]int64{},
		service:                   clevertap,
		clevertapConnectionConfig: connectionConfig,
	}
}

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, connection *backendconfig.Connection) (common.AsyncDestinationManager, error) {
	var destConfig DestinationConfig
	destConfig, err := convert[map[string]interface{}, DestinationConfig](destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in converting destination config: %v", err)
	}

	clevertapConnectionConfig, err := convertToConnectionConfig(connection)
	if err != nil {
		return nil, fmt.Errorf("error converting to connection config for clevertap segment: %v", err)
	}
	destName := destination.DestinationDefinition.Name

	clevertapService := &ClevertapServiceImpl{}
	endpoints, err := getBulkApi(destConfig)
	if err != nil {
		return nil, fmt.Errorf("error getting bulk api for clevertap segment: %v", err)
	}

	return common.SimpleAsyncDestinationManager{
		UploaderAndTransformer: NewClevertapBulkUploader(
			logger,
			statsFactory,
			destName,
			destConfig.AccessToken,
			destConfig.AppKey,
			endpoints,
			clevertapService,
			&clevertapConnectionConfig,
		),
	}, nil
}
