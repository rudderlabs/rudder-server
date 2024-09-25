package asyncdestinationmanager

import (
	"errors"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	bingads_audience "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/audience"
	bingads_offline_conversions "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/offline-conversions"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/eloqua"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/klaviyobulkupload"
	lyticsBulkUpload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/lytics_bulk_upload"
	marketobulkupload "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/marketo-bulk-upload"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/sftp"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/yandexmetrica"
)

func newRegularManager(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "BINGADS_AUDIENCE":
		return bingads_audience.NewManager(conf, logger, statsFactory, destination, backendConfig)
	case "BINGADS_OFFLINE_CONVERSIONS":
		return bingads_offline_conversions.NewManager(conf, logger, statsFactory, destination, backendConfig)
	case "MARKETO_BULK_UPLOAD":
		return marketobulkupload.NewManager(conf, logger, statsFactory, destination)
	case "ELOQUA":
		return eloqua.NewManager(logger, statsFactory, destination)
	case "YANDEX_METRICA_OFFLINE_EVENTS":
		return yandexmetrica.NewManager(logger, statsFactory, destination, backendConfig)
	case "KLAVIYO_BULK_UPLOAD":
		return klaviyobulkupload.NewManager(logger, statsFactory, destination)
	case "LYTICS_BULK_UPLOAD":
		return lyticsBulkUpload.NewManager(logger, statsFactory, destination)
	}
	return nil, errors.New("invalid destination type")
}

func newSFTPManager(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT) (common.AsyncDestinationManager, error) {
	switch destination.DestinationDefinition.Name {
	case "SFTP":
		return sftp.NewManager(logger, statsFactory, destination)
	}
	return nil, errors.New("invalid destination type")
}

func NewManager(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
	switch {
	case common.IsAsyncRegularDestination(destination.DestinationDefinition.Name):
		return newRegularManager(conf, logger, statsFactory, destination, backendConfig)
	case common.IsSFTPDestination(destination.DestinationDefinition.Name):
		return newSFTPManager(logger, statsFactory, destination)
	}
	return nil, errors.New("invalid destination type")
}
