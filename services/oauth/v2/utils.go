package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var (
	ErrorCategoriesMap          = map[string]struct{}{common.CategoryRefreshToken: {}, common.CategoryAuthStatusInactive: {}}
	ErrPermissionOrTokenRevoked = errors.New("problem with user permission or access/refresh token have been revoked")
)

func GetOAuthActionStatName(stat string) string {
	return fmt.Sprintf("oauth_action_%v", stat)
}

func checkIfTokenExpired(secret AccountSecret, oldSecret json.RawMessage, expiryTimeDiff time.Duration, statsHandler OAuthStatsHandler) bool {
	if secret.ExpirationDate != "" && isTokenExpired(secret.ExpirationDate, expiryTimeDiff, &statsHandler) {
		return true
	}
	if !routerutils.IsNotEmptyString(string(oldSecret)) {
		return false
	}
	return bytes.Equal(secret.Secret, oldSecret)
}

func isTokenExpired(expirationDate string, expirationTimeDiff time.Duration, statsHandler *OAuthStatsHandler) bool {
	date, err := time.Parse(misc.RFC3339Milli, expirationDate)
	if err != nil {
		statsHandler.Increment("proactive_token_refresh", stats.Tags{
			"errorMessage": "parsing failed",
		})
		return false
	}
	return date.Before(time.Now().Add(expirationTimeDiff))
}

func IsValidAuthErrorCategory(category string) bool {
	_, ok := ErrorCategoriesMap[category]
	return ok
}
