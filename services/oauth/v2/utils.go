package v2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"

	"github.com/rudderlabs/rudder-go-kit/stats"
	router_utils "github.com/rudderlabs/rudder-server/router/utils"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

const (
	CategoryRefreshToken = "REFRESH_TOKEN"
	// Identifier to be sent from destination(during transformation/delivery)
	CategoryAuthStatusInactive = "AUTH_STATUS_INACTIVE"
	// Identifier for invalid_grant or access_denied errors(during refreshing the token)
	RefTokenInvalidGrant = "ref_token_invalid_grant"
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

func (s *OAuthStats) SendTimerStats(startTime time.Time) {
	statsTags := stats.Tags{
		"id":              s.id,
		"workspaceId":     s.workspaceId,
		"rudderCategory":  s.rudderCategory,
		"isCallToCpApi":   strconv.FormatBool(s.isCallToCpApi),
		"authErrCategory": s.authErrCategory,
		"destType":        s.destDefName,
		"flowType":        string(s.flowType),
		"action":          s.action,
		"oauthVersion":    "v2",
	}
	stats.Default.NewTaggedStat(s.statName, stats.TimerType, statsTags).SendTiming(time.Since(startTime))
}

// Send count type stats related to OAuth(Destination)
func (s *OAuthStats) SendCountStat() {
	statsTags := stats.Tags{
		"oauthVersion":    "v2",
		"id":              s.id,
		"workspaceId":     s.workspaceId,
		"rudderCategory":  s.rudderCategory,
		"errorMessage":    s.errorMessage,
		"isCallToCpApi":   strconv.FormatBool(s.isCallToCpApi),
		"authErrCategory": s.authErrCategory,
		"destType":        s.destDefName,
		"isTokenFetch":    strconv.FormatBool(s.isTokenFetch),
		"flowType":        string(s.flowType),
		"action":          s.action,
	}
	stats.Default.NewTaggedStat(s.statName, stats.CountType, statsTags).Increment()
}

// {input: [{}]}
// {input: [{}, {}, {}, {}]}
// {input: [{}, {}, {}, {}]} -> {output: [{200}, {200}, {401,authErr}, {401,authErr}]}
func GetAuthErrorCategoryFromTransformResponse(respData []byte) (string, error) {
	var transformedJobs []TransformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	tfJob, found := lo.Find(transformedJobs, func(item TransformerResponse) bool {
		return IsValidAuthErrorCategory(item.AuthErrorCategory)
	})
	if !found {
		// can be a valid scenario
		return "", nil
	}
	return tfJob.AuthErrorCategory, nil
}

func GetAuthErrorCategoryFromTransformProxyResponse(respData []byte) (string, error) {
	var transformedJobs TransformerResponse
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, "output").Raw), &transformedJobs)
	if err != nil {
		return "", err
	}
	return transformedJobs.AuthErrorCategory, nil
}

func checkIfTokenExpired(secret AccountSecret, oldSecret json.RawMessage, expiryTimeDiff time.Duration, stats *OAuthStats) bool {
	var expirationDate expirationDate
	if err := json.Unmarshal(secret.Secret, &expirationDate); err != nil {
		stats.errorMessage = "unmarshal failed"
		stats.statName = GetOAuthActionStatName("proActive_token_refresh")
		stats.SendCountStat()
		return false
	}
	if expirationDate.ExpirationDate != "" && isTokenExpired(expirationDate.ExpirationDate, expiryTimeDiff, stats) {
		return true
	}
	if router_utils.IsNotEmptyString(string(oldSecret)) {
		if bytes.Equal(secret.Secret, oldSecret) {
			return true
		}
	}
	return false
}

func isTokenExpired(expirationDate string, expirationTimeDiff time.Duration, stats *OAuthStats) bool {
	date, err := time.Parse(misc.RFC3339Milli, expirationDate)
	if err != nil {
		stats.errorMessage = "parsing failed"
		stats.statName = GetOAuthActionStatName("proActive_token_refresh")
		stats.SendCountStat()
		return false
	}
	if date.Before(time.Now().Add(expirationTimeDiff)) {
		return true
	}
	return false
}

func GetErrorType(err error) string {
	if os.IsTimeout(err) {
		return "timeout"
	}
	for errno, errTyp := range ErrTypMap {
		if ok := errors.Is(err, errno); ok {
			return errTyp
		}
	}
	var e net.Error
	if errors.As(err, &e) {
		return "network_error"
	}
	return "none"
}

func IsValidAuthErrorCategory(category string) bool {
	return lo.Contains(ErrorCategories, category)
}
