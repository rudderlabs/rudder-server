package v2_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/jsonrs"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	mockoauthv2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	cntx "github.com/rudderlabs/rudder-server/services/oauth/v2/context"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	httpClient "github.com/rudderlabs/rudder-server/services/oauth/v2/http"
	"github.com/rudderlabs/rudder-server/utils/misc"
)

// The AUTH_STATUS_INACTIVE outcome is counted on oauth_action_request, through the handler, with the full tag set.
func TestTransportReportsAuthStatusInactive(t *testing.T) {
	cases := []struct {
		name            string
		flow            common.RudderFlow
		withAugmenter   bool
		category        string
		classifierError bool
		wantStatus      int
		wantInactive    bool
	}{
		{"reports delivery inactive", common.RudderFlowDelivery, true, common.CategoryAuthStatusInactive, false, http.StatusBadRequest, true},
		{"reports inactive without an augmenter", common.RudderFlowDelivery, false, common.CategoryAuthStatusInactive, false, http.StatusBadRequest, true},
		{"does not report inactive for REFRESH_TOKEN", common.RudderFlowDelivery, true, common.CategoryRefreshToken, false, http.StatusInternalServerError, false},
		{"does not report inactive when classification fails", common.RudderFlowDelivery, true, common.CategoryAuthStatusInactive, true, http.StatusInternalServerError, false},
		{"reports delete inactive", common.RudderFlowDelete, true, common.CategoryAuthStatusInactive, false, http.StatusBadRequest, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			statsStore, err := memstats.New()
			require.NoError(t, err)

			const (
				accountID     = "test-account-id"
				destinationID = "test-destination-id"
				workspaceID   = "test-workspace-id"
				destType      = "test-definition-name"
				cachedSecret  = `{"access_token":"cached-token"}`
			)

			// a valid cached token: with an augmenter, preRoundTrip serves it without touching the control plane
			cache := v2.NewOauthTokenCache()
			var augmenter extensions.Augmenter
			if tc.withAugmenter {
				augmenter = extensions.RouterBodyAugmenter
				cache.Store(accountID, v2.OAuthToken{
					Secret:         []byte(cachedSecret),
					ExpirationDate: time.Now().Add(24 * time.Hour).UTC().Format(misc.RFC3339Milli),
				})
			}

			// only the REFRESH_TOKEN outcome reaches the control plane: one refresh call, answered with a new token
			mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
			mockAuthIdentityProvider := mockoauthv2.NewMockAuthIdentityProvider(ctrl)
			if tc.category == common.CategoryRefreshToken && !tc.classifierError {
				mockAuthIdentityProvider.EXPECT().Identity().Return(nil)
				mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(
					http.StatusOK,
					`{"secret":{"access_token":"refreshed-token"}}`,
				)
			}

			locker := kitsync.NewPartitionRWLocker()
			// breaker options make NewOAuthHandler return the breaker wrapper, the production shape
			handler := v2.NewOAuthHandler(mockAuthIdentityProvider,
				v2.WithCache(cache),
				v2.WithLocker(locker),
				v2.WithStats(statsStore),
				v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
				v2.WithCpClient(mockCpConnector),
				v2.WithOauthBreakerOptions(&v2.OAuthBreakerOptions{}),
				v2.WithRudderFlow(tc.flow),
			)

			// the destination answers with a body the injected classifier maps to the category under test
			responseBody := []byte(`{"authErrorCategory":"` + tc.category + `"}`)
			mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
			mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).
				Do(func(req *http.Request) {
					secret, ok := cntx.SecretFromCtx(req.Context())
					require.Equal(t, tc.withAugmenter, ok)
					if tc.withAugmenter {
						require.JSONEq(t, cachedSecret, string(secret))
					}
				}).
				Return(&http.Response{
					StatusCode: http.StatusForbidden,
					Body:       io.NopCloser(bytes.NewReader(responseBody)),
				}, nil)

			classifierCalls := 0
			transport := httpClient.NewOAuthTransport(&httpClient.TransportArgs{
				FlowType:   tc.flow,
				TokenCache: &cache,
				Locker:     locker,
				GetAuthErrorCategory: func(body []byte) (string, error) {
					classifierCalls++
					require.Equal(t, responseBody, body)
					if tc.classifierError {
						// an error must take precedence over the returned category
						return tc.category, errors.New("classifier error")
					}
					return tc.category, nil
				},
				Augmenter:         augmenter,
				OAuthHandler:      handler,
				OriginalTransport: mockRoundTrip,
			})

			// the auth config is what makes the transport treat this as an OAuth destination;
			// without it RoundTrip passes straight through to the original transport
			destination := &backendconfig.DestinationT{
				ID:          destinationID,
				WorkspaceID: workspaceID,
				Config:      map[string]any{},
				DestinationDefinition: backendconfig.DestinationDefinitionT{
					Name: destType,
					Config: map[string]any{
						"auth": map[string]any{
							"type":         "OAuth",
							"rudderScopes": []any{string(tc.flow)},
						},
					},
				},
			}
			if tc.flow == common.RudderFlowDelete {
				destination.Config["rudderDeleteAccountId"] = accountID
				destination.DeleteAccount = &backendconfig.Account{ID: accountID}
			} else {
				destination.Config["rudderAccountId"] = accountID
				destination.DeliveryAccount = &backendconfig.Account{ID: accountID}
			}

			req, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader([]byte(`{}`)))
			require.NoError(t, err)
			req = req.WithContext(cntx.CtxWithDestination(req.Context(), destination))

			res, err := transport.RoundTrip(req)
			require.NoError(t, err)
			require.NotNil(t, res)
			defer func() { _ = res.Body.Close() }()

			// the destination's own status is preserved; the verdict travels in the interceptor response
			require.Equal(t, http.StatusForbidden, res.StatusCode)
			body, err := io.ReadAll(res.Body)
			require.NoError(t, err)
			var response v2.TransportResponse
			require.NoError(t, jsonrs.Unmarshal(body, &response))
			require.Equal(t, string(responseBody), response.OriginalResponse)
			require.Equal(t, tc.wantStatus, response.InterceptorResponse.StatusCode)
			require.Equal(t, 1, classifierCalls)

			metrics := statsStore.GetByName("oauth_action_request")
			if tc.wantInactive {
				require.Len(t, metrics, 1)
				require.Equal(t, stats.Tags{
					"id":              accountID,
					"destinationId":   destinationID,
					"workspaceId":     workspaceID,
					"rudderCategory":  "destination",
					"isCallToCpApi":   "false",
					"authErrCategory": common.CategoryAuthStatusInactive,
					"destType":        destType,
					"flowType":        string(tc.flow),
					"action":          "auth_status_inactive",
					"oauthVersion":    "v2",
					"errorMessage":    common.CategoryAuthStatusInactive,
				}, metrics[0].Tags)
				require.EqualValues(t, 1, metrics[0].Value)
				require.Empty(t, statsStore.GetByName("oauth_action_request_sent"))
			} else {
				for _, metric := range metrics {
					require.NotEqual(t, "auth_status_inactive", metric.Tags["action"])
					require.NotEqual(t, common.CategoryAuthStatusInactive, metric.Tags["authErrCategory"])
					require.NotEqual(t, common.CategoryAuthStatusInactive, metric.Tags["errorMessage"])
				}
				if tc.classifierError {
					require.Empty(t, metrics)
				} else {
					require.Len(t, metrics, 1)
					require.Equal(t, "refresh_token", metrics[0].Tags["action"])
					require.Empty(t, metrics[0].Tags["errorMessage"])
					require.EqualValues(t, 1, metrics[0].Value)
				}
			}

			require.Empty(t, statsStore.GetByName("oauth_action_proactive_token_refresh"))
			if !tc.withAugmenter {
				require.Empty(t, statsStore.GetByName("oauth_action_total_latency"))
			}
			// the latency histograms never learn about the inactive class and never carry destinationId
			for _, name := range []string{"oauth_action_total_latency", "oauth_action_request_latency"} {
				for _, metric := range statsStore.GetByName(name) {
					require.NotEqual(t, "auth_status_inactive", metric.Tags["action"])
					require.NotContains(t, metric.Tags, "destinationId")
				}
			}
		})
	}
}
