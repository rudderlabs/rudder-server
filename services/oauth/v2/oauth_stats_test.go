package v2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

func TestOAuthStatsHandlerCarriesDestinationIDOnCountersOnly(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)

	s := &oauthStats{
		stats:         statsStore,
		id:            "acct-1",
		destinationID: "dest-1",
		workspaceID:   "ws-1",
		destType:      "GOOGLE_ADWORDS_ENHANCED_CONVERSIONS",
		flowType:      common.RudderFlowDelivery,
		action:        "fetch_token",
	}

	tags := s.ToStatsTags()
	require.NotContains(t, tags, "destinationId", "the shared tag set stays per account: it is what the latency histograms carry")
	require.Equal(t, "acct-1", tags["id"], "id keeps carrying the accountId; one account can back several destinations")

	handler := newOAuthStatsHandler(s)
	for _, suffix := range []string{"request", "request_sent", "proactive_token_refresh"} {
		handler.Increment(suffix, stats.Tags{"errorMessage": "ref_token_invalid_grant", "isCallToCpApi": "true"})

		counters := statsStore.GetByName("oauth_action_" + suffix)
		require.Len(t, counters, 1)
		require.Equal(t, "dest-1", counters[0].Tags["destinationId"], `destinationId must be a first-class tag on the counter: the alert rule splices destinationId != "" into its selector`)
		require.Equal(t, "acct-1", counters[0].Tags["id"])
		require.Equal(t, "ref_token_invalid_grant", counters[0].Tags["errorMessage"])
		require.Equal(t, "true", counters[0].Tags["isCallToCpApi"])
		require.Equal(t, float64(1), counters[0].Value)
	}

	for _, suffix := range []string{"total_latency", "request_latency"} {
		handler.SendTiming(time.Now(), suffix, stats.Tags{"isCallToCpApi": "false"})

		timers := statsStore.GetByName("oauth_action_" + suffix)
		require.Len(t, timers, 1)
		require.NotContains(t, timers[0].Tags, "destinationId", "the latency histograms keep today's label set (18 series per label set in production)")
		require.Equal(t, "acct-1", timers[0].Tags["id"])
		require.Equal(t, "false", timers[0].Tags["isCallToCpApi"])
		require.Equal(t, tags, timers[0].Tags)
	}
}

func TestOAuthHandlerReportAuthStatusInactive(t *testing.T) {
	testReportAuthStatusInactive(t, common.RudderFlowDelivery)
}

func TestOAuthHandlerReportAuthStatusInactiveDelete(t *testing.T) {
	testReportAuthStatusInactive(t, common.RudderFlowDelete, WithRudderFlow(common.RudderFlowDelete))
}

func testReportAuthStatusInactive(t *testing.T, flow common.RudderFlow, options ...func(*oauthHandler)) {
	t.Helper()

	statsStore, err := memstats.New()
	require.NoError(t, err)

	options = append(options, WithStats(statsStore))
	handler := NewOAuthHandler(nil, options...)
	handler.ReportAuthStatusInactive(&OAuthTokenParams{
		AccountID:     "acct-1",
		WorkspaceID:   "ws-1",
		DestType:      "SALESFORCE_OAUTH",
		DestinationID: "dest-1",
	})

	// comparing the whole store proves that nothing else (a timer, another counter) is emitted
	require.Equal(t, []memstats.Metric{
		{
			Name: "oauth_action_request",
			Tags: stats.Tags{
				"id":              "acct-1",
				"destinationId":   "dest-1",
				"workspaceId":     "ws-1",
				"rudderCategory":  "destination",
				"isCallToCpApi":   "false",
				"authErrCategory": common.CategoryAuthStatusInactive,
				"destType":        "SALESFORCE_OAUTH",
				"flowType":        string(flow),
				"action":          "auth_status_inactive",
				"oauthVersion":    "v2",
				"errorMessage":    common.CategoryAuthStatusInactive,
			},
			Value: 1,
		},
	}, statsStore.GetAll())
}

func TestNewOAuthHandlerWithNilStats(t *testing.T) {
	handler := NewOAuthHandler(nil, WithStats(nil)).(*oauthHandler)

	require.NotNil(t, handler.stats)
	require.Equal(t, stats.Default, handler.stats)
	require.NotPanics(t, func() {
		handler.ReportAuthStatusInactive(&OAuthTokenParams{
			AccountID:     "acct-nil-stats",
			WorkspaceID:   "ws-nil-stats",
			DestType:      "SALESFORCE_OAUTH",
			DestinationID: "dest-nil-stats",
		})
	})
}
