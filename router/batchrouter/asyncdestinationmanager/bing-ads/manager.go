package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"

	bingads "github.com/rudderlabs/bing-ads-go-sdk/bingads"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
)

func newManagerInternal(destination *backendconfig.DestinationT, oauthClient oauth.Authorizer) (*BingAdsBulkUploader, error) {
	destConfig := DestinationConfig{}
	jsonConfig, err := stdjson.Marshal(destination.Config)
	if err != nil {
		return nil, fmt.Errorf("error in marshalling destination config: %v", err)
	}
	err = stdjson.Unmarshal(jsonConfig, &destConfig)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling destination config: %v", err)
	}

	tokenSource := tokenSource{
		workspaceID:     destination.WorkspaceID,
		destinationName: destination.Name,
		accountID:       destConfig.RudderAccountId,
		oauthClient:     oauthClient,
	}
	secret, err := tokenSource.generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate oauth token: %v", err)
	}
	sessionConfig := bingads.SessionConfig{
		DeveloperToken: secret.Developer_token,
		AccountId:      destConfig.CustomerAccountId,
		CustomerId:     destConfig.CustomerId,
		HTTPClient:     http.DefaultClient,
		TokenSource:    &tokenSource,
	}
	session := bingads.NewSession(sessionConfig)

	clientNew := Client{}
	bingads := NewBingAdsBulkUploader(destination.DestinationDefinition.Name, bingads.NewBulkService(session), &clientNew)
	return bingads, nil
}

func NewManager(destination *backendconfig.DestinationT, backendConfig backendconfig.BackendConfig) (*BingAdsBulkUploader, error) {
	oauthClient := oauth.NewOAuthErrorHandler(backendConfig)
	return newManagerInternal(destination, oauthClient)
}
