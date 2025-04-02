package offline_conversions

import (
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/jsonrs"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/common"
	asynccommon "github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/common"

	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func newManagerInternal(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, oauthClientV2 oauthv2.Authorizer) (*BingAdsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := jsonrs.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = jsonrs.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	token := common.TokenSource{
		WorkspaceID:        destination.WorkspaceID,
		DestinationDefName: destination.DestinationDefinition.Name,
		AccountID:          destConfig.RudderAccountID,
		OauthClientV2:      oauthClientV2,
		DestinationID:      destination.ID,
		CurrentTime:        time.Now,
	}
	secret, err := token.GenerateTokenV2()
	if err != nil {
		return nil, fmt.Errorf("failed to generate oauth token: %v", err)
	}
	sessionConfig := bingads.SessionConfig{
		DeveloperToken: secret.DeveloperToken,
		AccountId:      destConfig.CustomerAccountID,
		CustomerId:     destConfig.CustomerID,
		HTTPClient:     http.DefaultClient,
		TokenSource:    &token,
	}
	session := bingads.NewSession(sessionConfig)
	bingUploader := NewBingAdsBulkUploader(logger, statsFactory, destination.DestinationDefinition.Name, bingads.NewBulkService(session), destConfig.IsHashRequired)
	return bingUploader, nil
}

func NewManager(conf *config.Config, logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (asynccommon.AsyncDestinationManager, error) {
	oauthClientV2 := oauthv2.NewOAuthHandler(backendConfig,
		oauthv2.WithLogger(logger),
		oauthv2.WithCPConnectorTimeout(conf.GetDuration("HttpClient.oauth.timeout", 30, time.Second)),
		oauthv2.WithStats(statsFactory),
	)
	return newManagerInternal(logger, statsFactory, destination, oauthClientV2)
}
