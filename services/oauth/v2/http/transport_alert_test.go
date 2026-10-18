package v2_test

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = DescribeTable("AUTH_STATUS_INACTIVE metric (auth alert)",
	func(
		flow common.RudderFlow,
		withAugmenter bool,
		category string,
		classifierError bool,
		wantStatus int,
		wantInactive bool,
	) {
		ctrl := gomock.NewController(GinkgoT())
		statsStore, err := memstats.New()
		Expect(err).To(BeNil())

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
		if withAugmenter {
			augmenter = extensions.RouterBodyAugmenter
			cache.Store(accountID, v2.OAuthToken{
				Secret:         []byte(cachedSecret),
				ExpirationDate: time.Now().Add(24 * time.Hour).UTC().Format(misc.RFC3339Milli),
			})
		}

		// only the REFRESH_TOKEN outcome reaches the control plane: one refresh call, answered with a new token
		mockCpConnector := mockoauthv2.NewMockConnector(ctrl)
		mockAuthIdentityProvider := mockoauthv2.NewMockAuthIdentityProvider(ctrl)
		if category == common.CategoryRefreshToken && !classifierError {
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
			v2.WithRudderFlow(flow),
		)

		// the destination answers with a body the injected classifier maps to the category under test
		responseBody := []byte(`{"authErrorCategory":"` + category + `"}`)
		mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
		mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).
			Do(func(req *http.Request) {
				secret, ok := cntx.SecretFromCtx(req.Context())
				Expect(ok).To(Equal(withAugmenter))
				if withAugmenter {
					Expect(string(secret)).To(MatchJSON(cachedSecret))
				}
			}).
			Return(&http.Response{
				StatusCode: http.StatusForbidden,
				Body:       io.NopCloser(bytes.NewReader(responseBody)),
			}, nil)

		classifierCalls := 0
		transport := httpClient.NewOAuthTransport(&httpClient.TransportArgs{
			FlowType:   flow,
			TokenCache: &cache,
			Locker:     locker,
			GetAuthErrorCategory: func(body []byte) (string, error) {
				classifierCalls++
				Expect(body).To(Equal(responseBody))
				if classifierError {
					// an error must take precedence over the returned category
					return category, errors.New("classifier error")
				}
				return category, nil
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
						"rudderScopes": []any{string(flow)},
					},
				},
			},
		}
		if flow == common.RudderFlowDelete {
			destination.Config["rudderDeleteAccountId"] = accountID
			destination.DeleteAccount = &backendconfig.Account{ID: accountID}
		} else {
			destination.Config["rudderAccountId"] = accountID
			destination.DeliveryAccount = &backendconfig.Account{ID: accountID}
		}

		req, err := http.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader([]byte(`{}`)))
		Expect(err).To(BeNil())
		req = req.WithContext(cntx.CtxWithDestination(req.Context(), destination))

		res, err := transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		defer res.Body.Close()

		// the destination's own status is preserved; the verdict travels in the interceptor response
		Expect(res.StatusCode).To(Equal(http.StatusForbidden))
		body, err := io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		var response v2.TransportResponse
		Expect(jsonrs.Unmarshal(body, &response)).To(Succeed())
		Expect(response.OriginalResponse).To(Equal(string(responseBody)))
		Expect(response.InterceptorResponse.StatusCode).To(Equal(wantStatus))
		Expect(classifierCalls).To(Equal(1))

		metrics := statsStore.GetByName("oauth_action_request")
		if wantInactive {
			Expect(metrics).To(HaveLen(1))
			Expect(metrics[0].Tags).To(Equal(stats.Tags{
				"id":              accountID,
				"destinationId":   destinationID,
				"workspaceId":     workspaceID,
				"rudderCategory":  "destination",
				"isCallToCpApi":   "false",
				"authErrCategory": common.CategoryAuthStatusInactive,
				"destType":        destType,
				"flowType":        string(flow),
				"action":          "auth_status_inactive",
				"oauthVersion":    "v2",
				"errorMessage":    common.CategoryAuthStatusInactive,
			}))
			Expect(metrics[0].Value).To(BeNumerically("==", 1))
			Expect(statsStore.GetByName("oauth_action_request_sent")).To(BeEmpty())
		} else {
			for _, metric := range metrics {
				Expect(metric.Tags["action"]).NotTo(Equal("auth_status_inactive"))
				Expect(metric.Tags["authErrCategory"]).NotTo(Equal(common.CategoryAuthStatusInactive))
				Expect(metric.Tags["errorMessage"]).NotTo(Equal(common.CategoryAuthStatusInactive))
			}
			if classifierError {
				Expect(metrics).To(BeEmpty())
			} else {
				Expect(metrics).To(HaveLen(1))
				Expect(metrics[0].Tags["action"]).To(Equal("refresh_token"))
				Expect(metrics[0].Tags["errorMessage"]).To(BeEmpty())
				Expect(metrics[0].Value).To(BeNumerically("==", 1))
			}
		}

		Expect(statsStore.GetByName("oauth_action_proactive_token_refresh")).To(BeEmpty())
		if !withAugmenter {
			Expect(statsStore.GetByName("oauth_action_total_latency")).To(BeEmpty())
		}
		// the latency histograms never learn about the inactive class and never carry destinationId
		for _, name := range []string{"oauth_action_total_latency", "oauth_action_request_latency"} {
			for _, metric := range statsStore.GetByName(name) {
				Expect(metric.Tags["action"]).NotTo(Equal("auth_status_inactive"))
				Expect(metric.Tags).NotTo(HaveKey("destinationId"))
			}
		}
	},
	Entry("reports delivery inactive",
		common.RudderFlowDelivery, true, common.CategoryAuthStatusInactive,
		false, http.StatusBadRequest, true),
	Entry("reports inactive without an augmenter",
		common.RudderFlowDelivery, false, common.CategoryAuthStatusInactive,
		false, http.StatusBadRequest, true),
	Entry("does not report inactive for REFRESH_TOKEN",
		common.RudderFlowDelivery, true, common.CategoryRefreshToken,
		false, http.StatusInternalServerError, false),
	Entry("does not report inactive when classification fails",
		common.RudderFlowDelivery, true, common.CategoryAuthStatusInactive,
		true, http.StatusInternalServerError, false),
	Entry("reports delete inactive",
		common.RudderFlowDelete, true, common.CategoryAuthStatusInactive,
		false, http.StatusBadRequest, true),
)
