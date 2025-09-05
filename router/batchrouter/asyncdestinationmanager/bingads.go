package asyncdestinationmanager

import (
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	audienceV2 "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/audience"
	offlineConversionsV2 "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads-v2/offline-conversions"
	audienceV1 "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/audience"
	offlineConversionsV1 "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/offline-conversions"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
)

func NewBingAdsAudienceManager(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	if conf.GetBool("router.batchrouter.asyncdestinationmanager.BINGADS_AUDIENCE.v2Enabled", true) {
		return audienceV2.NewManager(conf, logger, statsFactory, destination, backendConfig)
	}

	return audienceV1.NewManager(conf, logger, statsFactory, destination, backendConfig)
}

func NewBingAdsOfflineConversionsManager(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (common.AsyncDestinationManager, error) {
	if conf.GetBool("router.batchrouter.asyncdestinationmanager.BINGADS_OFFLINE_CONVERSIONS.v2Enabled", true) {
		return offlineConversionsV2.NewManager(conf, logger, statsFactory, destination, backendConfig)
	}

	return offlineConversionsV1.NewManager(conf, logger, statsFactory, destination, backendConfig)
}
