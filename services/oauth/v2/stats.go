package v2

import (
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

type OAuthStats struct {
	stats           stats.Stats
	id              string // destinationId -> for action == auth_status_inactive, accountId -> for action == refresh_token/fetch_token
	workspaceID     string
	errorMessage    string
	rudderCategory  string // destination
	statName        string
	isCallToCpApi   bool   // is a call being made to control-plane APIs
	authErrCategory string // for action=refresh_token -> REFRESH_TOKEN, for action=fetch_token -> "", for action=auth_status_inactive -> auth_status_inactive
	destDefName     string
	isTokenFetch    bool              // This stats field is used to identify if a request to get token is arising from processor
	flowType        common.RudderFlow // delivery, delete
	action          string            // refresh_token, fetch_token, auth_status_inactive
}

func (s *OAuthStats) SendTimerStats(startTime time.Time) {
	statsTags := stats.Tags{
		"id":              s.id,
		"workspaceId":     s.workspaceID,
		"rudderCategory":  s.rudderCategory,
		"isCallToCpApi":   strconv.FormatBool(s.isCallToCpApi),
		"authErrCategory": s.authErrCategory,
		"destType":        s.destDefName,
		"flowType":        string(s.flowType),
		"action":          s.action,
		"oauthVersion":    "v2",
	}
	s.stats.NewTaggedStat(s.statName, stats.TimerType, statsTags).SendTiming(time.Since(startTime))
}

// SendCountStat Send count type stats related to OAuth(Destination)
func (s *OAuthStats) SendCountStat() {
	statsTags := stats.Tags{
		"oauthVersion":    "v2",
		"id":              s.id,
		"workspaceId":     s.workspaceID,
		"rudderCategory":  s.rudderCategory,
		"errorMessage":    s.errorMessage,
		"isCallToCpApi":   strconv.FormatBool(s.isCallToCpApi),
		"authErrCategory": s.authErrCategory,
		"destType":        s.destDefName,
		"isTokenFetch":    strconv.FormatBool(s.isTokenFetch),
		"flowType":        string(s.flowType),
		"action":          s.action,
	}
	s.stats.NewTaggedStat(s.statName, stats.CountType, statsTags).Increment()
}
