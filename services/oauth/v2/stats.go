package v2

import (
	"strconv"
	"time"

	"github.com/rudderlabs/rudder-go-kit/stats"
)

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
	stats.Default.NewTaggedStat(s.statName, stats.TimerType, statsTags).SendTiming(time.Since(startTime))
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
	stats.Default.NewTaggedStat(s.statName, stats.CountType, statsTags).Increment()
}
