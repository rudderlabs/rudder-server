package audience

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/router/batchrouter/asyncdestinationmanager/bing-ads/common"
	"github.com/rudderlabs/rudder-server/services/oauth"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func newManagerInternal(logger logger.Logger, statsFactory stats.Stats, destination *backendconfig.DestinationT, oauthClient oauth.Authorizer, oauthClientV2 oauthv2.Authorizer) (*BingAdsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	token := common.TokenSource{
		WorkspaceID:     destination.WorkspaceID,
		DestinationName: destination.Name,
		AccountID:       destConfig.RudderAccountID,
		OauthClient:     oauthClient,
		OauthClientV2:   oauthClientV2,
		DestinationID:   destination.ID,
	}
	secret, err := token.GenerateToken()
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

	clientNew := Client{}
	bingUploader := NewBingAdsBulkUploader(logger, statsFactory, destination.DestinationDefinition.Name, bingads.NewBulkService(session), &clientNew)
	return bingUploader, nil
}

func NewManager(
	conf *config.Config,
	logger logger.Logger,
	statsFactory stats.Stats,
	destination *backendconfig.DestinationT,
	backendConfig backendconfig.BackendConfig,
) (*BingAdsBulkUploader, error) {
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	oauthClientV2 := oauthv2.NewOAuthHandler(backendConfig,
		oauthv2.WithLogger(logger),
		oauthv2.WithCPConnectorTimeout(conf.GetDuration("HttpClient.oauth.timeout", 30, time.Second)),
		oauthv2.WithStats(statsFactory),
	)
	return newManagerInternal(logger, statsFactory, destination, oauthClient, oauthClientV2)
}
