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
	accountID       string
	destinationID   string
	workspaceID     string
	errorMessage    string
	rudderCategory  string // destination
	statName        string
	isCallToCpApi   bool
	authErrCategory string
	destType        string
	flowType        common.RudderFlow // delivery, delete
	action          string            // refresh_token, fetch_token, auth_status_inactive
}

type oauthStatsHandler struct {
	stats         stats.Stats
	histogramTags stats.Tags
	counterTags   stats.Tags
}

func (s *oauthStats) ToStatsTags() stats.Tags {
	return stats.Tags{
		"id":              s.accountID, // keep the historical tag name: dashboards and breaker counters key on id
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
	histogramTags := s.ToStatsTags()
	return oauthStatsHandler{
		stats:         s.stats,
		histogramTags: histogramTags,
		counterTags:   lo.Assign(histogramTags, stats.Tags{"destinationId": s.destinationID}),
	}
}

func (m *oauthStatsHandler) Increment(statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{oauthV2StatPrefix, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.CountType, lo.Assign(m.counterTags, tags)).Increment()
}

func (m *oauthStatsHandler) SendTiming(startTime time.Time, statSuffix string, tags stats.Tags) {
	statName := strings.Join([]string{oauthV2StatPrefix, statSuffix}, "_")
	m.stats.NewTaggedStat(statName, stats.TimerType, lo.Assign(m.histogramTags, tags)).SendTiming(time.Since(startTime))
}
