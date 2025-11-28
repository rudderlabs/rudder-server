package salesforcebulkupload

import (
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	augmenter "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk-upload/augmenter"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauthv2common "github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	oauthv2httpclient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
)

func NewManager(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (common.AsyncDestinationManager, error) {
	destinationInfo := &oauthv2.DestinationInfo{
		Config:           destination.Config,
		DefinitionConfig: destination.DestinationDefinition.Config,
		WorkspaceID:      destination.WorkspaceID,
		DestType:         destination.DestinationDefinition.Name,
		ID:               destination.ID,
	}

	cache := oauthv2.NewOauthTokenCache()
	optionalArgs := &oauthv2httpclient.HttpClientOptionalArgs{
		Logger:              logger.Withn(obskit.DestinationID(destination.ID), obskit.WorkspaceID(destination.WorkspaceID)),
		Augmenter:           augmenter.RequestAugmenter,
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("BatchRouter.SALESFORCE_BULK_UPLOAD", conf),
	}
	originalHttpClient := &http.Client{Transport: &http.Transport{}, Timeout: 30 * time.Second}
	client := oauthv2httpclient.NewOAuthHttpClient(originalHttpClient, oauthv2common.RudderFlowDelivery, &cache, backendConfig, augmenter.GetAuthErrorCategoryForSalesforce, optionalArgs)
	apiService := NewAPIService(logger, destinationInfo, client)

	return &Uploader{
		destinationInfo: destinationInfo,
		destName:        destName,
		logger:          logger,
		statsFactory:    statsFactory,
		apiService:      apiService,
		dataHashToJobID: make(map[string][]int64),
	}, nil
}
