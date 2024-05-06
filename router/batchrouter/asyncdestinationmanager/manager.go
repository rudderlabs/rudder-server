package asyncdestinationmanager

import (
	"errors"

	jsoniter "github.com/json-iterator/go"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads_audience "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/audience"
	bingads_offline_conversions "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/offline-conversions"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "BINGADS_AUDIENCE":
		return bingads_audience.NewManager(destination, backendConfig)
	case "BINGADS_OFFLINE_CONVERSIONS":
		return bingads_offline_conversions.NewManager(destination, backendConfig)
	case "MARKETO_BULK_UPLOAD":
		return marketobulkupload.NewManager(destination)
	case "ELOQUA":
		return eloqua.NewManager(destination)
	}
	return nil, errors.New("invalid destination type")
}
