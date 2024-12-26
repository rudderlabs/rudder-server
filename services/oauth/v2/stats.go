package v2

import (
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

const OAUTH_V2_STAT_PREFIX = "oauth_action"

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
	flowType        common.RudderFlow // delivery, delete
	action          string            // refresh_token, fetch_token, auth_status_inactive
}

type OAuthStatsHandler struct {
	stats       stats.Stats
	defaultTags stats.Tags
}

func GetDefaultTagsFromOAuthStats(oauthStats *OAuthStats) stats.Tags {
	return stats.Tags{
		"id":              oauthStats.id,
		"workspaceId":     oauthStats.workspaceID,
		"rudderCategory":  "destination",
		"isCallToCpApi":   strconv.FormatBool(oauthStats.isCallToCpApi),
		"authErrCategory": oauthStats.authErrCategory,
		"destType":        oauthStats.destDefName,
		"flowType":        string(oauthStats.flowType),
		"action":          oauthStats.action,
		"oauthVersion":    "v2",
	}
}

func NewStatsHandlerFromOAuthStats(oauthStats *OAuthStats) OAuthStatsHandler {
	defaultTags := GetDefaultTagsFromOAuthStats(oauthStats)
	return OAuthStatsHandler{
		stats:       oauthStats.stats,
		defaultTags: defaultTags,
	}
}

func (m *OAuthStatsHandler) Increment(statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{OAUTH_V2_STAT_PREFIX, statSuffix}, "_")
	allTags := lo.Assign(m.defaultTags, tags)
	m.stats.NewTaggedStat(statName, stats.CountType, allTags).Increment()
}

func (m *OAuthStatsHandler) SendTiming(startTime time.Time, statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{OAUTH_V2_STAT_PREFIX, statSuffix}, "_")
	allTags := lo.Assign(m.defaultTags, tags)
	m.stats.NewTaggedStat(statName, stats.TimerType, allTags).SendTiming(time.Since(startTime))
}
