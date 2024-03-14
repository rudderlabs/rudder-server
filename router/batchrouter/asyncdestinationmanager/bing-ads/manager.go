package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/logger"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/oauth"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func newManagerInternal(destination *backendconfig.DestinationT, oauthClient oauth.Authorizer, oauthClientV2 oauthv2.Authorizer) (*BingAdsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	token := tokenSource{
		workspaceID:     destination.WorkspaceID,
		destinationName: destination.Name,
		accountID:       destConfig.RudderAccountID,
		oauthClient:     oauthClient,
		oauthClientV2:   oauthClientV2,
		destinationID:   destination.ID,
	}
	secret, err := token.generateToken()
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
	bingUploader := NewBingAdsBulkUploader(destination.DestinationDefinition.Name, bingads.NewBulkService(session), &clientNew)
	return bingUploader, nil
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*BingAdsBulkUploader, error) {
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	oauthClientV2 := oauthv2.NewOAuthHandler(backendConfig, oauthv2.WithLogger(logger.NewLogger().Child("BatchRouter")), oauthv2.WithCPConnectorTimeout(config.GetDuration("HttpClient.oauth.timeout", 30, time.Second)))
	return newManagerInternal(destination, oauthClient, oauthClientV2)
}
