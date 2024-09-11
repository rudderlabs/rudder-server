package eloqua

import (
	"encoding/base64"
	stdjson "encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (*EloquaBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}
	authorization := destConfig.CompanyName + "\\" + destConfig.UserName + ":" + destConfig.Password
	destName := destination.DestinationDefinition.Name
	encodedAuthorizationString := "Basic " + base64.StdEncoding.EncodeToString([]byte(authorization))
	eloquaData := HttpRequestData{
		Authorization: encodedAuthorizationString,
	}
	eloqua := NewEloquaServiceImpl("2.0")
	baseEndpoint, err := eloqua.GetBaseEndpoint(&eloquaData)
	unableToGetBaseEndpointStat := statsFactory.NewTaggedStat("unable_to_get_base_endpoint", stats.CountType, map[string]string{
		"module":   "batch_router",
		"destType": destName,
	})
	if err != nil {
		unableToGetBaseEndpointStat.Count(1)
		return nil, fmt.Errorf("error in getting base endpoint: %v", err)
	}

	return NewEloquaBulkUploader(logger, statsFactory, destName, encodedAuthorizationString, baseEndpoint, eloqua), nil
}

func NewEloquaBulkUploader(logger logger.Logger, statsFactory stats.Stats, destinationName, authorization, baseEndpoint string, eloqua EloquaService) *EloquaBulkUploader {
	return &EloquaBulkUploader{
		destName:          destinationName,
		logger:            logger.Child("Eloqua").Child("EloquaBulkUploader"),
		statsFactory:      statsFactory,
		authorization:     authorization,
		baseEndpoint:      baseEndpoint,
		fileSizeLimit:     common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 32*bytesize.MB),
		eventsLimit:       common.GetBatchRouterConfigInt64("MaxEventsLimit", destinationName, 1000000),
		successStatusCode: common.GetBatchRouterConfigStringMap("SuccessStatusCode", destinationName, []string{"ELQ-00040"}),
		service:           eloqua,
		jobToCSVMap:       map[int64]int64{},
	}
}
