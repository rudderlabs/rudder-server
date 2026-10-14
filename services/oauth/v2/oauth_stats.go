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
	id              string // accountId (one account can back several destinations)
	destinationID   string // destinationId: carried by the counters only (see OAuthStatsHandler.counterTags); the alerting pipeline scopes rules, resolves the resource and renders $destinationName by this tag
	workspaceID     string
	errorMessage    string
	rudderCategory  string // destination
	statName        string
	isCallToCpApi   bool   // is a call being made to control-plane APIs
	authErrCategory string // for action=refresh_token -> REFRESH_TOKEN, for action=fetch_token -> "", for action=auth_status_inactive -> auth_status_inactive
	destType        string
	flowType        common.RudderFlow // delivery, delete
	action          string            // refresh_token, fetch_token, auth_status_inactive
}

type OAuthStatsHandler struct {
	stats       stats.Stats
	defaultTags stats.Tags // the oauth_action_* histograms, keyed by account (id) as before; also the base of counterTags
	counterTags stats.Tags // defaultTags plus destinationId: the alert rule, the resource lookup and $destinationName key on it
}

func (oauthStats *OAuthStats) ToStatsTags() stats.Tags {
	return stats.Tags{
		"id":              oauthStats.id,
		"workspaceId":     oauthStats.workspaceID,
		"rudderCategory":  "destination",
		"isCallToCpApi":   strconv.FormatBool(oauthStats.isCallToCpApi),
		"authErrCategory": oauthStats.authErrCategory,
		"destType":        oauthStats.destType,
		"flowType":        string(oauthStats.flowType),
		"action":          oauthStats.action,
		"oauthVersion":    "v2",
	}
}

func NewStatsHandlerFromOAuthStats(oauthStats *OAuthStats) OAuthStatsHandler {
	defaultTags := oauthStats.ToStatsTags()
	return OAuthStatsHandler{
		stats:       oauthStats.stats,
		defaultTags: defaultTags,
		counterTags: lo.Assign(defaultTags, stats.Tags{"destinationId": oauthStats.destinationID}),
	}
}

// Increment counts on oauth_action_<suffix>. Counters carry destinationId, one series per destination: the
// alerting pipeline reads oauth_action_request by it.
func (m *OAuthStatsHandler) Increment(statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{OAUTH_V2_STAT_PREFIX, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.CountType, lo.Assign(m.counterTags, tags)).Increment()
}

// SendTiming records the oauth_action_<suffix> histograms with the per-account label set, as before. A token
// call and its lock are per account, so a per-destination latency series would only repeat the account's
// numbers, 18 series (16 buckets, count, sum) per label set.
func (m *OAuthStatsHandler) SendTiming(startTime time.Time, statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{OAUTH_V2_STAT_PREFIX, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.TimerType, lo.Assign(m.defaultTags, tags)).SendTiming(time.Since(startTime))
}

// RecordAuthStatusInactive counts an AUTH_STATUS_INACTIVE outcome on oauth_action_request. This class is
// classified in the transport (postRoundTrip), never inside FetchToken/RefreshToken, so until now it
// emitted nothing. It uses the same metric name and tag set as the token paths so a single errorMessage
// selector covers both failure classes.
func RecordAuthStatusInactive(s stats.Stats, params *OAuthTokenParams, flow common.RudderFlow) {
	authStats := &OAuthStats{
		stats:           s,
		id:              params.AccountID,
		destinationID:   params.DestinationID,
		workspaceID:     params.WorkspaceID,
		rudderCategory:  "destination",
		isCallToCpApi:   false,
		authErrCategory: common.CategoryAuthStatusInactive,
		destType:        params.DestType,
		flowType:        flow,
		action:          "auth_status_inactive",
	}
	handler := NewStatsHandlerFromOAuthStats(authStats)
	handler.Increment("request", stats.Tags{
		"errorMessage":  common.CategoryAuthStatusInactive,
		"isCallToCpApi": "false",
	})
}
