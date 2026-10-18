package v2

import (
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/rudderlabs/rudder-go-kit/stats"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

const oauthV2StatPrefix = "oauth_action"

type oauthStats struct {
	stats           stats.Stats
	id              string // accountId (one account can back several destinations)
	destinationID   string // destinationId: carried by the counters only (see oauthStatsHandler.counterTags); the alerting pipeline scopes rules, resolves the resource and renders $destinationName by this tag
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

type oauthStatsHandler struct {
	stats       stats.Stats
	defaultTags stats.Tags // the oauth_action_* histograms, keyed by account (id) as before; also the base of counterTags
	counterTags stats.Tags // defaultTags plus destinationId: the alert rule, the resource lookup and $destinationName key on it
}

func (s *oauthStats) ToStatsTags() stats.Tags {
	return stats.Tags{
		"id":              s.id,
		"workspaceId":     s.workspaceID,
		"rudderCategory":  "destination",
		"isCallToCpApi":   strconv.FormatBool(s.isCallToCpApi),
		"authErrCategory": s.authErrCategory,
		"destType":        s.destType,
		"flowType":        string(s.flowType),
		"action":          s.action,
		"oauthVersion":    "v2",
	}
}

func newOAuthStatsHandler(s *oauthStats) oauthStatsHandler {
	defaultTags := s.ToStatsTags()
	return oauthStatsHandler{
		stats:       s.stats,
		defaultTags: defaultTags,
		counterTags: lo.Assign(defaultTags, stats.Tags{"destinationId": s.destinationID}),
	}
}

// Increment counts on oauth_action_<suffix>. Counters carry destinationId, one series per destination: the
// alerting pipeline reads oauth_action_request by it.
func (m *oauthStatsHandler) Increment(statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{oauthV2StatPrefix, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.CountType, lo.Assign(m.counterTags, tags)).Increment()
}

// SendTiming records the oauth_action_<suffix> histograms with the per-account label set, as before. A token
// call and its lock are per account, so a per-destination latency series would only repeat the account's
// numbers, 18 series (16 buckets, count, sum) per label set.
func (m *oauthStatsHandler) SendTiming(startTime time.Time, statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{oauthV2StatPrefix, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.TimerType, lo.Assign(m.defaultTags, tags)).SendTiming(time.Since(startTime))
}
