package bingads

import (
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/oauth2"

	"github.com/rudderlabs/rudder-go-kit/config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type secretStruct struct {
	AccessToken    string `json:"accessToken"`
	RefreshToken   string `json:"refreshToken"`
	DeveloperToken string `json:"developer_token"`
	ExpirationDate string `json:"expirationDate"`
}

type tokenSource struct {
	workspaceID     string
	destinationName string
	accountID       string
	oauthClient     oauth.Authorizer
	oauthClientV2   oauthv2.Authorizer
	destinationID   string
}

// authentication related utils

func (ts *tokenSource) generateToken() (*secretStruct, error) {
	refreshTokenParams := oauth.RefreshTokenParams{
		WorkspaceId: ts.workspaceID,
		DestDefName: ts.destinationName,
		AccountId:   ts.accountID,
	}
	statusCode, authResponse := ts.oauthClient.FetchToken(&refreshTokenParams)
	if statusCode != 200 {
		return nil, fmt.Errorf("fetching access token: %v, %d", authResponse.Err, statusCode)
	}

	/*
		How bing-ads works?
		1. It fetches the token using the fetchToken method
		2. It checks if the token is expired or not
		3. If the token is expired, it refreshes the token using the refreshToken method
		4. If the token is not expired, it returns the token


		step 1: fetchToken

		step 2: use the token to make the request and return the response in []byte along with the function to extract the oauthError category
		        func myFunc(string token)(string)


		step 3: depending on the oauthErrorCategory we will refresh the token and return the new token
	*/

	secret := secretStruct{}
	err := json.Unmarshal(authResponse.Account.Secret, &secret)
	if err != nil {
		return nil, fmt.Errorf("error in unmarshalling secret: %w", err)
	}
	currentTime := time.Now()
	expirationTime, err := time.Parse(misc.RFC3339Milli, secret.ExpirationDate)
	if err != nil {
		return nil, fmt.Errorf("error in parsing expirationDate: %w", err)
	}
	if currentTime.After(expirationTime) {
		refreshTokenParams.Secret = authResponse.Account.Secret
		statusCode, authResponse = ts.oauthClient.RefreshToken(&refreshTokenParams)
		if statusCode != 200 {
			return nil, fmt.Errorf("error in refreshing access token")
		}
		err = json.Unmarshal(authResponse.Account.Secret, &secret)
		if err != nil {
			return nil, fmt.Errorf("error in unmarshalling secret: %w", err)
		}
		return &secret, nil
	}
	return &secret, nil
}

func (ts *tokenSource) generateTokenV2() (*secretStruct, error) {
	destination := oauthv2.DestinationInfo{
		WorkspaceID:    ts.workspaceID,
		DefinitionName: ts.destinationName,
		ID:             ts.destinationID,
	}
	refreshTokenParams := oauthv2.RefreshTokenParams{
		WorkspaceID: ts.workspaceID,
		DestDefName: ts.destinationName,
		AccountID:   ts.accountID,
		Destination: &destination,
	}
	statusCode, authResponse, err := ts.oauthClientV2.FetchToken(&refreshTokenParams)
	if err != nil && authResponse != nil {
		return nil, fmt.Errorf("fetching access token: %v, %d", authResponse.Err, statusCode)
	}
	if err != nil {
		return nil, fmt.Errorf("fetching access token resulted in an error: %v,%d", err, statusCode)
	}

	/*
		How bing-ads works?
		1. It fetches the token using the fetchToken method
		2. It checks if the token is expired or not
		3. If the token is expired, it refreshes the token using the refreshToken method
		4. If the token is not expired, it returns the token


		step 1: fetchToken

		step 2: use the token to make the request and return the response in []byte along with the function to extract the oauthError category
		        func myFunc(string token)(string)


		step 3: depending on the oauthErrorCategory we will refresh the token and return the new token
	*/

	var secret secretStruct
	if err = json.Unmarshal(authResponse.Account.Secret, &secret); err != nil {
		return nil, fmt.Errorf("error in unmarshalling secret: %w", err)
	}
	currentTime := time.Now()
	expirationTime, err := time.Parse(misc.RFC3339Milli, secret.ExpirationDate)
	if err != nil {
		return nil, fmt.Errorf("error in parsing expirationDate: %w", err)
	}
	if currentTime.Before(expirationTime) {
		return &secret, nil
	}
	refreshTokenParams.Secret = authResponse.Account.Secret
	statusCode, authResponse, err = ts.oauthClientV2.RefreshToken(&refreshTokenParams)
	// TODO: check if the error is not nil working verify during unit test
	if err != nil {
		return nil, fmt.Errorf("error in refreshing access token with this error: %w. StatusCode: %d", err, statusCode)
	}
	if err = json.Unmarshal(authResponse.Account.Secret, &secret); err != nil {
		return nil, fmt.Errorf("error in unmarshalling secret: %w", err)
	}
	return &secret, nil
}

func (ts *tokenSource) Token() (*oauth2.Token, error) {
	oauthV2Enabled := config.GetReloadableBoolVar(false, "BatchRouter."+ts.destinationName+".oauthV2Enabled", "BatchRouter.oauthV2Enabled")
	var secret *secretStruct
	var err error
	if oauthV2Enabled.Load() {
		secret, err = ts.generateTokenV2()
	} else {
		secret, err = ts.generateToken()
	}
	if err != nil {
		return nil, fmt.Errorf("generating the accessToken: %w", err)
	}

	token := &oauth2.Token{
		AccessToken:  secret.AccessToken,
		RefreshToken: secret.RefreshToken,
		Expiry:       time.Now().Add(time.Hour), // Set the token expiry time
	}
	return token, nil
}
