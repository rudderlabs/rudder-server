package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

var (
	ErrorCategoriesMap          = map[string]struct{}{common.CategoryRefreshToken: {}, common.CategoryAuthStatusInactive: {}}
	ErrPermissionOrTokenRevoked = errors.New("problem with user permission or access/refresh token have been revoked")
)

// isOauthTokenExpired checks if the token is expired or is about to expire within the refreshBeforeExpiry duration.
// If the token is not expired, but its secret is the same as the previous secret, it is also considered to be expired.
func isOauthTokenExpired(oauthToken OAuthToken, previousSecret json.RawMessage, refreshBeforeExpiry time.Duration, statsHandler OAuthStatsHandler) bool {
	expires, err := oauthToken.Expires(refreshBeforeExpiry)
	if err != nil {
		statsHandler.Increment("proactive_token_refresh", stats.Tags{"errorMessage": "parsing failed"})
	}
	return expires || bytes.Equal(oauthToken.Secret, previousSecret)
}

func IsValidAuthErrorCategory(category string) bool {
	_, ok := ErrorCategoriesMap[category]
	return ok
}
