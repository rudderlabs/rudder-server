package bingads

import (
	stdjson "encoding/json"
	"fmt"
	"net/http"

	"github.com/rudderlabs/bing-ads-go-sdk/bingads"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	"github.com/rudderlabs/rudder-server/services/oauth"
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

	token := tokenSource{
		workspaceID:     destination.WorkspaceID,
		destinationName: destination.Name,
		accountID:       destConfig.RudderAccountID,
		oauthClient:     oauthClient,
		destination:     destination,
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
	return newManagerInternal(destination, oauthClient)
}
