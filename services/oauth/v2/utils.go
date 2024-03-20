package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"

	routerutils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

var (
	ErrorCategoriesMap          = map[string]struct{}{common.CategoryRefreshToken: {}, common.CategoryAuthStatusInactive: {}}
	ErrPermissionOrTokenRevoked = errors.New("problem with user permission or access/refresh token have been revoked")
)

func GetOAuthActionStatName(stat string) string {
	return fmt.Sprintf("oauth_action_%v", stat)
}

func checkIfTokenExpired(secret AccountSecret, oldSecret json.RawMessage, expiryTimeDiff time.Duration, stats *OAuthStats) bool {
	if secret.ExpirationDate != "" && isTokenExpired(secret.ExpirationDate, expiryTimeDiff, stats) {
		return true
	}
	if !routerutils.IsNotEmptyString(string(oldSecret)) {
		return false
	}
	return bytes.Equal(secret.Secret, oldSecret)
}

func isTokenExpired(expirationDate string, expirationTimeDiff time.Duration, stats *OAuthStats) bool {
	date, err := time.Parse(misc.RFC3339Milli, expirationDate)
	if err != nil {
		stats.errorMessage = "parsing failed"
		stats.statName = GetOAuthActionStatName("proactive_token_refresh")
		stats.SendCountStat()
		return false
	}
	return date.Before(time.Now().Add(expirationTimeDiff))
}

func IsValidAuthErrorCategory(category string) bool {
	_, ok := ErrorCategoriesMap[category]
	return ok
}
