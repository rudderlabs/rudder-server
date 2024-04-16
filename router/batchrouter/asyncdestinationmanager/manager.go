package asyncdestinationmanager

import (
	"errors"

	jsoniter "github.com/json-iterator/go"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "BINGADS_AUDIENCE":
		return bingads.NewManager(destination, backendConfig)
	case "MARKETO_BULK_UPLOAD":
		return marketobulkupload.NewManager(destination)
	case "ELOQUA":
		return eloqua.NewManager(destination)
	}
	return nil, errors.New("invalid destination type")
}
