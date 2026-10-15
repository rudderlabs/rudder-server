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

	s := &OAuthStats{
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

	handler := NewStatsHandlerFromOAuthStats(s)
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

func TestRecordAuthStatusInactiveEmitsOAuthActionRequest(t *testing.T) {
	statsStore, err := memstats.New()
	require.NoError(t, err)

	RecordAuthStatusInactive(statsStore, &OAuthTokenParams{
		AccountID:     "acct-1",
		WorkspaceID:   "ws-1",
		DestType:      "SALESFORCE_OAUTH",
		DestinationID: "dest-1",
	}, common.RudderFlowDelivery)

	metrics := statsStore.GetByName("oauth_action_request")
	require.Len(t, metrics, 1)
	m := metrics[0]
	require.Equal(t, float64(1), m.Value)
	require.Equal(t, common.CategoryAuthStatusInactive, m.Tags["errorMessage"], "one errorMessage regex must cover both failure classes")
	require.Equal(t, common.CategoryAuthStatusInactive, m.Tags["authErrCategory"])
	require.Equal(t, "auth_status_inactive", m.Tags["action"])
	require.Equal(t, "dest-1", m.Tags["destinationId"])
	require.Equal(t, "acct-1", m.Tags["id"])
	require.Equal(t, "ws-1", m.Tags["workspaceId"])
	require.Equal(t, "SALESFORCE_OAUTH", m.Tags["destType"])
	require.Equal(t, "delivery", m.Tags["flowType"])
	require.Equal(t, "false", m.Tags["isCallToCpApi"])
	require.Equal(t, "v2", m.Tags["oauthVersion"])
	require.Equal(t, "destination", m.Tags["rudderCategory"])
}
