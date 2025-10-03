package common

import (
	"fmt"
	"time"

	"golang.org/x/oauth2"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

type SecretStruct struct {
	AccessToken    string `json:"accessToken"`
	RefreshToken   string `json:"refreshToken"`
	DeveloperToken string `json:"developer_token"`
	ExpirationDate string `json:"expirationDate"`
}

type TokenSource struct {
	WorkspaceID        string
	DestinationDefName string
	AccountID          string
	OauthHandler       oauthv2.OAuthHandler
	DestinationID      string
	CurrentTime        func() time.Time
}

// authentication related utils

func (ts *TokenSource) GenerateTokenV2() (*SecretStruct, error) {
	tokenParams := oauthv2.OAuthTokenParams{
		AccountID:     ts.AccountID,
		WorkspaceID:   ts.WorkspaceID,
		DestType:      ts.DestinationDefName,
		DestinationID: ts.DestinationID,
	}
	rawSecret, scErr := ts.OauthHandler.FetchToken(&tokenParams)
	if scErr != nil {
		return nil, fmt.Errorf("fetching access token: %w", scErr)
	}

	var secret SecretStruct
	if err := jsonrs.Unmarshal(rawSecret, &secret); err != nil {
		return nil, fmt.Errorf("error in unmarshalling secret: %w", err)
	}
	return &secret, nil
}

func (ts *TokenSource) Token() (*oauth2.Token, error) {
	var secret *SecretStruct
	var err error
	secret, err = ts.GenerateTokenV2()
	if err != nil {
		return nil, fmt.Errorf("generating the accessToken: %w", err)
	}
	expiryTime, _ := time.Parse(misc.RFC3339Milli, secret.ExpirationDate)
	// skipping error check as similar check is already done on the previous function
	token := &oauth2.Token{
		AccessToken:  secret.AccessToken,
		RefreshToken: secret.RefreshToken,
		Expiry:       expiryTime,
	}
	return token, nil
}
