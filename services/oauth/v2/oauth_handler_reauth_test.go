package v2_test

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/logger/mock_logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	mock_backendconfig "github.com/rudderlabs/rudder-server/mocks/backend-config"
	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
)

var _ = Describe("OAuth reauthorization eviction", func() {
	const (
		expirationDate = "2099-01-01T00:00:00.000Z"
		cachedSecret   = `{"access_token":"cached","expirationDate":"2099-01-01T00:00:00.000Z"}`
		freshSecret    = `{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"}`
		tokenResponse  = `{"secret":{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"},"secretVersion":8}`
		supersededLog  = "cached OAuth token superseded by a newer secret in the control plane"
	)
	var (
		bc              *mock_backendconfig.MockBackendConfig
		mockCpConnector *mock_oauthV2.MockConnector
		mockLogger      *mock_logger.MockLogger
		cache           v2.OauthTokenCache
		handler         v2.OAuthHandler
		params          *v2.OAuthTokenParams
	)

	BeforeEach(func() {
		ctrl := gomock.NewController(GinkgoT())
		bc, _, _ = mockSecretVersions(ctrl)
		mockCpConnector = mock_oauthV2.NewMockConnector(ctrl)
		mockLogger = mock_logger.NewMockLogger(ctrl)
		mockLogger.EXPECT().Child("OAuthHandler").Return(mockLogger)
		mockLogger.EXPECT().Withn(gomock.Any()).Return(mockLogger).AnyTimes()
		mockLogger.EXPECT().Debugn(gomock.Any(), gomock.Any()).AnyTimes()
		cache = v2.NewOauthTokenCache()
		handler = v2.NewOAuthHandler(bc,
			v2.WithCache(cache),
			v2.WithLocker(kitsync.NewPartitionRWLocker()),
			v2.WithCpClient(mockCpConnector),
			v2.WithReauthEviction(bc),
			v2.WithLogger(mockLogger),
			v2.WithStats(stats.NOP),
		)
		params = &v2.OAuthTokenParams{
			AccountID:     "acct-1",
			WorkspaceID:   "ws-1",
			DestinationID: "dest-1",
			DestType:      "testDest",
		}
	})

	expectFetch := func() {
		bc.EXPECT().Identity().Return(nil).Times(1)
		mockCpConnector.EXPECT().CpApiCall(gomock.Any()).DoAndReturn(func(request *controlplane.Request) (int, string) {
			Expect(request.Method).To(Equal(http.MethodPost))
			Expect(request.Body).To(MatchJSON(`{"hasExpired":true,"expiredSecret":null}`))
			return http.StatusOK, tokenResponse
		}).Times(1)
	}

	It("A7b: fetches a newer stored secret once and then serves the cached response", func() {
		cache.Store(params.AccountID, v2.OAuthToken{
			Secret:         []byte(cachedSecret),
			ExpirationDate: expirationDate,
			SecretVersion:  7,
		})
		expectFetch()
		mockLogger.EXPECT().Infon(supersededLog,
			logger.NewIntField("cachedSecretVersion", int64(7)),
			logger.NewIntField("configSecretVersion", int64(8)),
		).Times(1)

		for range 2 {
			secret, err := handler.FetchToken(params)
			Expect(err).To(BeNil())
			Expect(secret).To(MatchJSON(freshSecret))
		}
		token, ok := cache.Load(params.AccountID)
		Expect(ok).To(BeTrue())
		Expect(token.Secret).To(MatchJSON(freshSecret))
		Expect(token.SecretVersion).To(Equal(8))
		Expect(token.ExpirationDate).To(Equal(expirationDate))
	})

	DescribeTable("A7c: keeps cached secrets that are not superseded", func(version int) {
		cached := v2.OAuthToken{
			Secret:         []byte(cachedSecret),
			ExpirationDate: expirationDate,
			SecretVersion:  version,
		}
		cache.Store(params.AccountID, cached)
		mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)
		mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

		secret, err := handler.FetchToken(params)
		Expect(err).To(BeNil())
		Expect(secret).To(MatchJSON(cachedSecret))
		token, ok := cache.Load(params.AccountID)
		Expect(ok).To(BeTrue())
		Expect(token).To(Equal(cached))
	},
		Entry("the pod that just refreshed holds the config version", 8),
		Entry("the config is older than the cached token", 9),
		Entry("the cached token has no version", 0),
	)

	It("A7d: fetches normally when nothing is cached", func() {
		expectFetch()
		mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

		secret, err := handler.FetchToken(params)
		Expect(err).To(BeNil())
		Expect(secret).To(MatchJSON(freshSecret))
		token, ok := cache.Load(params.AccountID)
		Expect(ok).To(BeTrue())
		Expect(token.SecretVersion).To(Equal(8))
		Expect(token.ExpirationDate).To(Equal(expirationDate))
	})
})
