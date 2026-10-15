package v2_test

import (
	"bytes"
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
)

var _ = Describe("AUTH_STATUS_INACTIVE metric (auth alert)", func() {
	It("counts AUTH_STATUS_INACTIVE on oauth_action_request with the destinationId tag", func() {
		ctrl := gomock.NewController(GinkgoT())

		// the destination answers with a body the injected classifier maps to AUTH_STATUS_INACTIVE
		mockRoundTrip := mockoauthv2.NewMockRoundTripper(ctrl)
		mockRoundTrip.EXPECT().RoundTrip(gomock.Any()).Return(&http.Response{
			StatusCode: http.StatusForbidden,
			Body:       io.NopCloser(bytes.NewReader([]byte(`{"authErrorCategory":"AUTH_STATUS_INACTIVE"}`))),
		}, nil)

		mockAuthIdentityProvider := mockoauthv2.NewMockAuthIdentityProvider(ctrl)
		mockAuthIdentityProvider.EXPECT().Identity().Return(nil).AnyTimes()

		// a valid cached token so preRoundTrip never needs the control plane
		handlerCache := v2.NewOauthTokenCache()
		handlerCache.Store("test-account-id", v2.OAuthToken{
			Secret:         []byte(`{"access_token":"cached-token"}`),
			ExpirationDate: time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
		})
		oauthHandler := v2.NewOAuthHandler(mockAuthIdentityProvider,
			v2.WithCache(handlerCache),
			v2.WithLocker(kitsync.NewPartitionRWLocker()),
			v2.WithStats(stats.Default),
			v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
		)

		statsStore, statsErr := memstats.New()
		Expect(statsErr).To(BeNil())

		cache := v2.NewOauthTokenCache()
		transport := httpClient.NewOAuthTransport(&httpClient.TransportArgs{
			FlowType:             common.RudderFlowDelivery,
			TokenCache:           &cache,
			Locker:               kitsync.NewPartitionRWLocker(),
			GetAuthErrorCategory: func([]byte) (string, error) { return common.CategoryAuthStatusInactive, nil },
			Augmenter:            extensions.RouterBodyAugmenter,
			OAuthHandler:         oauthHandler,
			OriginalTransport:    mockRoundTrip,
			Stats:                statsStore,
		})

		// the auth config is what makes the transport treat this as an OAuth destination;
		// without it RoundTrip passes straight through to the original transport
		req, err := http.NewRequest("POST", "http://example.com", bytes.NewReader([]byte(`{}`)))
		Expect(err).To(BeNil())
		req = req.WithContext(cntx.CtxWithDestination(req.Context(), &backendconfig.DestinationT{
			ID:              "test-destination-id",
			WorkspaceID:     "test-workspace-id",
			Config:          map[string]any{"rudderAccountId": "test-account-id"},
			DeliveryAccount: &backendconfig.Account{ID: "test-account-id"},
			DestinationDefinition: backendconfig.DestinationDefinitionT{
				Name: "test-definition-name",
				Config: map[string]any{
					"auth": map[string]any{
						"type":         "OAuth",
						"rudderScopes": []any{"delivery"},
					},
				},
			},
		}))

		res, err := transport.RoundTrip(req)
		Expect(err).To(BeNil())
		Expect(res).NotTo(BeNil())
		defer res.Body.Close()

		respBody, err := io.ReadAll(res.Body)
		Expect(err).To(BeNil())
		var transportResponse v2.TransportResponse
		Expect(jsonrs.Unmarshal(respBody, &transportResponse)).To(Succeed())
		Expect(transportResponse.InterceptorResponse.StatusCode).To(Equal(http.StatusBadRequest))

		metrics := statsStore.GetByName("oauth_action_request")
		Expect(metrics).To(HaveLen(1))
		Expect(metrics[0].Tags["errorMessage"]).To(Equal(common.CategoryAuthStatusInactive))
		Expect(metrics[0].Tags["destinationId"]).To(Equal("test-destination-id"))
		Expect(metrics[0].Tags["id"]).To(Equal("test-account-id"))
		Expect(metrics[0].Tags["workspaceId"]).To(Equal("test-workspace-id"))
		Expect(metrics[0].Tags["destType"]).To(Equal("test-definition-name"))
		Expect(metrics[0].Value).To(BeNumerically("==", 1))
	})
})
