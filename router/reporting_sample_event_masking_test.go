package router

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/config"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"

	mockfeatures "github.com/rudderlabs/rudder-server/mocks/services/transformer"
)

const reportingSampleEventMaskingMetric = "router_reporting_sample_event_masking"

func TestMaskReportingSampleEvent(t *testing.T) {
	t.Run("flag off leaves payload byte-identical", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		features := mockfeatures.NewMockFeaturesService(ctrl)
		rt, statsStore := newReportingSampleEventMaskingTestHandle(t, features, false)
		payload := json.RawMessage("{ \"headers\" : { \"Authorization\" : \"secret\" } }")

		masked := rt.maskReportingSampleEvent(payload)

		require.Equal(t, payload, masked)
		require.Empty(t, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
	})

	for _, tc := range []struct {
		name       string
		paths      []string
		ok         bool
		input      string
		want       string
		wantReason string
	}{
		{
			name:       "missing secret paths leaves payload unchanged",
			ok:         false,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "no_paths",
		},
		{
			name:       "empty secret paths leave payload unchanged",
			paths:      []string{},
			ok:         true,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "non-object payload is never blanket masked",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `["secret"]`,
			want:       `["secret"]`,
			wantReason: "not_object",
		},
		{
			name:       "listed path is masked with siblings intact",
			paths:      []string{"headers.Authorization"},
			ok:         true,
			input:      `{"endpoint":"visible","headers":{"Authorization":"secret","Content-Type":"application/json"},"body":{"name":"Ada"}}`,
			want:       `{"endpoint":"visible","headers":{"Authorization":"******","Content-Type":"application/json"},"body":{"name":"Ada"}}`,
			wantReason: "listed",
		},
		{
			name:       "absent listed path leaves payload unchanged",
			paths:      []string{"params.api_key"},
			ok:         true,
			input:      `{ "headers" : { "Authorization" : "secret" } }`,
			want:       `{ "headers" : { "Authorization" : "secret" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "unsettable path retains successful masks",
			paths:      []string{"headers.Authorization", "", "body.token|@reverse"},
			ok:         true,
			input:      `{"headers":{"Authorization":"secret"},"body":{"token":"also-secret","name":"Ada"}}`,
			want:       `{"headers":{"Authorization":"******"},"body":{"token":"also-secret","name":"Ada"}}`,
			wantReason: "partial",
		},
		{
			name:       "pre-transform event has no delivery-shaped secrets",
			paths:      []string{"headers.Authorization", "params.api_key", "body.access_token", "payload.apiKey"},
			ok:         true,
			input:      `{ "type" : "track", "event" : "Order Completed", "userId" : "user-1", "properties" : { "order_id" : "order-1" } }`,
			want:       `{ "type" : "track", "event" : "Order Completed", "userId" : "user-1", "properties" : { "order_id" : "order-1" } }`,
			wantReason: "listed_noop",
		},
		{
			name:       "endpoint is never masked",
			paths:      []string{"endpoint"},
			ok:         true,
			input:      `{ "endpoint" : "https://example.test/path?token=visible" }`,
			want:       `{ "endpoint" : "https://example.test/path?token=visible" }`,
			wantReason: "listed_noop",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			features := mockfeatures.NewMockFeaturesService(ctrl)
			features.EXPECT().SecretPaths("TEST_DEST").Return(tc.paths, tc.ok)
			rt, statsStore := newReportingSampleEventMaskingTestHandle(t, features, true)
			payload := json.RawMessage(tc.input)

			masked := rt.maskReportingSampleEvent(payload)

			require.Equal(t, tc.want, string(masked))
			require.Equal(t, tc.input, string(payload), "caller payload must not be modified")
			require.Equal(t, map[string]float64{tc.wantReason: 1}, maskingCounts(statsStore, reportingSampleEventMaskingMetric))
		})
	}
}

func newReportingSampleEventMaskingTestHandle(
	t *testing.T,
	features *mockfeatures.MockFeaturesService,
	enabled bool,
) (*Handle, *memstats.Store) {
	t.Helper()
	statsStore, err := memstats.New()
	require.NoError(t, err)
	return &Handle{
		destType:                   "TEST_DEST",
		transformerFeaturesService: features,
		reloadableConfig: &reloadableConfig{
			enableReportingSampleEventMasking: config.SingleValueLoader(enabled),
		},
		reportingSampleEventMaskingStat: func(reason string) stats.Counter {
			return statsStore.NewTaggedStat(reportingSampleEventMaskingMetric, stats.CountType, stats.Tags{"destType": "TEST_DEST", "reason": reason})
		},
	}, statsStore
}
