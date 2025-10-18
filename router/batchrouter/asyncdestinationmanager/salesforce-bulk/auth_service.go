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

	tokenParams := &oauthv2.OAuthTokenParams{
		AccountID:     s.accountID,
		WorkspaceID:   s.workspaceID,
		DestType:      destName,
		DestinationID: s.destID,
	}

	rawSecret, scErr := s.oauthClient.FetchToken(tokenParams)
	if scErr != nil {
		return "", fmt.Errorf("fetching access token: %w", scErr)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
		IssuedAt    string `json:"issued_at"`
		ExpiresIn   int    `json:"expires_in"`
	}

	if err := jsonrs.Unmarshal(rawSecret, &tokenResp); err != nil {
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

func (s *SalesforceAuthService) GetInstanceURL() (string, error) {
	if s.instanceURL == "" {
		_, err := s.GetAccessToken()
		if err != nil {
			return "", err
		}
	}
	return s.instanceURL, nil
}

func (s *SalesforceAuthService) clearToken() {
	s.accessToken = ""
	s.instanceURL = ""
	s.tokenExpiry = 0
}
