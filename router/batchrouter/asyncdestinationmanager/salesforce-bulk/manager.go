package salesforcebulk

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"
	augmenter "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/salesforce-bulk/augmenter"
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
		Logger:              logger,
		Augmenter:           augmenter.SalesforceReqAugmenter,
		OAuthBreakerOptions: oauthv2.ConfigToOauthBreakerOptions("BatchRouter.SALESFORCE_BULK_UPLOAD", conf),
	}
	originalHttpClient := &http.Client{Transport: &http.Transport{}}
	client := oauthv2httpclient.NewOAuthHttpClient(originalHttpClient, oauthv2common.RudderFlowDelivery, &cache, backendConfig, augmenter.GetAuthErrorCategoryForSalesforce, optionalArgs)
	apiService := NewSalesforceAPIService(logger, destinationInfo, "v62.0", client)

	return &SalesforceBulkUploader{
		destinationInfo: destinationInfo,
		destName:        destName,
		logger:          logger,
		statsFactory:    statsFactory,
		apiService:      apiService,
		dataHashToJobID: make(map[string][]int64),
	}, nil
}
