package salesforcebulk

import (
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	oauthv2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

// GetAccessToken fetches OAuth access token from RudderStack's OAuth v2 service
// Implements caching to avoid repeated calls to Control Plane
func (s *SalesforceAuthService) GetAccessToken() (string, error) {
	// Check if cached token is still valid (with 1 minute buffer)
	now := time.Now().Unix()
	if s.accessToken != "" && s.tokenExpiry > now+60 {
		return s.accessToken, nil
	}

	// Fetch token from OAuth v2 service
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

	// Parse OAuth response
	var tokenResp struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
		IssuedAt    string `json:"issued_at"`
		ExpiresIn   int    `json:"expires_in"` // Seconds until expiration
	}

	if err := jsonrs.Unmarshal(authResponse.Account.Secret, &tokenResp); err != nil {
		return "", fmt.Errorf("unmarshalling OAuth secret: %w", err)
	}

	// Validate required fields
	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("access token is empty in OAuth response")
	}
	if tokenResp.InstanceURL == "" {
		return "", fmt.Errorf("instance URL is empty in OAuth response")
	}

	// Cache token and instance URL
	s.accessToken = tokenResp.AccessToken
	s.instanceURL = tokenResp.InstanceURL

	// Calculate expiry time
	if tokenResp.ExpiresIn > 0 {
		s.tokenExpiry = now + int64(tokenResp.ExpiresIn)
	} else {
		// Default to 2 hours if not provided
		s.tokenExpiry = now + 7200
	}

	s.logger.Debugf("Fetched new Salesforce OAuth token, expires in %d seconds", tokenResp.ExpiresIn)

	return s.accessToken, nil
}

// GetInstanceURL returns the Salesforce instance URL from OAuth response
func (s *SalesforceAuthService) GetInstanceURL() string {
	return s.instanceURL
}

