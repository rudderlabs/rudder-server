package bingads

import (
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/oauth2"

	oauth "github.com/rudderlabs/rudder-server/services/oauth"
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

func (ts *tokenSource) Token() (*oauth2.Token, error) {
	secret, err := ts.generateToken()
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
