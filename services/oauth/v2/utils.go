package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	CategoryRefreshToken = "REFRESH_TOKEN"
	// CategoryAuthStatusInactive Identifier to be sent from destination(during transformation/delivery)
	CategoryAuthStatusInactive = "AUTH_STATUS_INACTIVE"
	// RefTokenInvalidGrant Identifier for invalid_grant or access_denied errors(during refreshing the token)
	RefTokenInvalidGrant = "ref_token_invalid_grant"
	TimeOutError         = "timeout"
	NetworkError         = "network_error"
	None                 = "none"
)

var (
	ErrorCategories = []string{CategoryRefreshToken, CategoryAuthStatusInactive}
	ErrTypMap       = map[syscall.Errno]string{
		syscall.ECONNRESET:   "econnreset",
		syscall.ECONNREFUSED: "econnrefused",
		syscall.ECONNABORTED: "econnaborted",
		syscall.ECANCELED:    "ecanceled",
	}
	ErrPermissionOrTokenRevoked = errors.New("Problem with user permission or access/refresh token have been revoked")
)

func GetOAuthActionStatName(stat string) string {
	return fmt.Sprintf("oauth_action_%v", stat)
}

// {input: [{}]}
// {input: [{}, {}, {}, {}]}
// {input: [{}, {}, {}, {}]} -> {output: [{200}, {200}, {401,authErr}, {401,authErr}]}
func GetAuthErrorCategoryFromTransformResponse(respData []byte) (string, error) {
	var transformedJobs []transformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	tfJob, found := lo.Find(transformedJobs, func(item transformerResponse) bool {
		return IsValidAuthErrorCategory(item.AuthErrorCategory)
	})
	if !found {
		// can be a valid scenario
		return "", nil
	}
	return tfJob.AuthErrorCategory, nil
}

func GetAuthErrorCategoryFromTransformProxyResponse(respData []byte) (string, error) {
	var transformedJobs transformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

func checkIfTokenExpired(secret AccountSecret, oldSecret json.RawMessage, expiryTimeDiff time.Duration, stats *OAuthStats) bool {
	var expirationDate expirationDate
	if err := jsonfast.Unmarshal(secret.Secret, &expirationDate); err != nil {
		stats.errorMessage = "unmarshal failed"
		stats.statName = GetOAuthActionStatName("proactive_token_refresh")
		stats.SendCountStat()
		return false
	}
	if expirationDate.ExpirationDate != "" && isTokenExpired(expirationDate.ExpirationDate, expiryTimeDiff, stats) {
		return true
	}
	if !router_utils.IsNotEmptyString(string(oldSecret)) {
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

func GetErrorType(err error) string {
	if os.IsTimeout(err) {
		return TimeOutError
	}
	for errno, errTyp := range ErrTypMap {
		if ok := errors.Is(err, errno); ok {
			return errTyp
		}
	}
	var e net.Error
	if errors.As(err, &e) {
		return NetworkError
	}
	return None
}

func IsValidAuthErrorCategory(category string) bool {
	return lo.Contains(ErrorCategories, category)
}
