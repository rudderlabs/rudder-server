package asyncdestinationmanager

import (
	"errors"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sftp"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica"
)

func newRegularManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "BINGADS_AUDIENCE":
		return bingads.NewManager(destination, backendConfig)
	case "MARKETO_BULK_UPLOAD":
		return marketobulkupload.NewManager(destination)
	case "ELOQUA":
		return eloqua.NewManager(destination)
	case "YANDEX_METRICA_OFFLINE_EVENTS":
		return yandexmetrica.NewManager(destination, backendConfig)
	}
	return nil, errors.New("invalid destination type")
}

func newSFTPManager(destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "SFTP":
		return sftp.NewManager(destination)
	}
	return nil, errors.New("invalid destination type")
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	switch {
	case common.IsAsyncRegularDestination(destination.DestinationDefinition.Name):
		return newRegularManager(destination, backendConfig)
	case common.IsSFTPDestination(destination.DestinationDefinition.Name):
		return newSFTPManager(destination)
	}
	return nil, errors.New("invalid destination type")
}
