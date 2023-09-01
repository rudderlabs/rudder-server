package eloqua

import (
	"encoding/base64"
	stdjson "encoding/json"
	"fmt"

	"github.com/rudderlabs/rudder-go-kit/bytesize"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewManager(destination *backendconfig.DestinationT) (*EloquaBulkUploader, error) {
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
	eloqua := NewEloquaImpl("2.0")
	baseEndpoint, err := eloqua.GetBaseEndpoint(&eloquaData)
	if err != nil {
		return nil, fmt.Errorf("error in getting base endpoint: %v", err)
	}

	return NewEloquaBulkUploader(destName, encodedAuthorizationString, baseEndpoint, eloqua), nil
}

func NewEloquaBulkUploader(destinationName, authorization, baseEndpoint string, eloqua Eloqua) *EloquaBulkUploader {
	return &EloquaBulkUploader{
		destName:      destinationName,
		logger:        logger.NewLogger().Child("batchRouter").Child("AsyncDestinationManager").Child("Eloqua").Child("EloquaBulkUploader"),
		authorization: authorization,
		baseEndpoint:  baseEndpoint,
		fileSizeLimit: common.GetBatchRouterConfigInt64("MaxUploadLimit", destinationName, 32*bytesize.MB),
		eventsLimit:   common.GetBatchRouterConfigInt64("MaxEventsLimit", destinationName, 1000000),
		service:       eloqua,
	}
}
