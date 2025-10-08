package salesforcebulk

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func (s *SalesforceAuthService) GetAccessToken() (string, error) {
	now := time.Now().Unix()
	if s.accessToken != "" && s.tokenExpiry > now+60 {
		return s.accessToken, nil
	}

	refreshTokenParams := oauthv2.RefreshTokenParams{
		WorkspaceID:   s.workspaceID,
		DestDefName:   destName,
		AccountID:     s.accountID,
		DestinationID: s.destID,
	}

	statusCode, authResponse, err := s.oauthClient.FetchToken(&refreshTokenParams)
	if err != nil && authResponse != nil {
		return "", fmt.Errorf("fetching access token: %v, status: %d", authResponse.Err, statusCode)
	}
	if err != nil {
		return "", fmt.Errorf("fetching access token: %w, status: %d", err, statusCode)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
		IssuedAt    string `json:"issued_at"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := jsonrs.Unmarshal(authResponse.Account.Secret, &tokenResp); err != nil {
		return "", fmt.Errorf("unmarshalling OAuth secret: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("access token is empty in OAuth response")
	}
	if tokenResp.InstanceURL == "" {
		return "", fmt.Errorf("instance URL is empty in OAuth response")
	}

	s.accessToken = tokenResp.AccessToken
	s.instanceURL = tokenResp.InstanceURL

	if tokenResp.ExpiresIn > 0 {
		s.tokenExpiry = now + int64(tokenResp.ExpiresIn)
	} else {
		s.tokenExpiry = now + 7200 // Default to 2 hours
	}

	s.logger.Debugf("Fetched new Salesforce OAuth token, expires in %d seconds", tokenResp.ExpiresIn)
	return s.accessToken, nil
}

func (s *SalesforceAuthService) GetInstanceURL() string {
	return s.instanceURL
}

