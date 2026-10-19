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

	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/controlplane"
)

type fakeSecretVersions map[string]int

func (f fakeSecretVersions) SecretVersion(accountID string) int { return f[accountID] }

var _ = Describe("OAuth secret version check", func() {
	const (
		expirationDate = "2099-01-01T00:00:00.000Z"
		cachedSecret   = `{"access_token":"cached","expirationDate":"2099-01-01T00:00:00.000Z"}`
		freshSecret    = `{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"}`
		tokenResponse  = `{"secret":{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"},"secretVersion":8}`
		supersededLog  = "cached OAuth token superseded by a newer secret in the control plane"
	)
	var (
		identity        *mock_oauthV2.MockAuthIdentityProvider
		mockCpConnector *mock_oauthV2.MockConnector
		mockLogger      *mock_logger.MockLogger
		cache           v2.OauthTokenCache
		params          *v2.OAuthTokenParams
	)

	handlerWith := func(provider v2.SecretVersionProvider) v2.OAuthHandler {
		return v2.NewOAuthHandler(identity,
			v2.WithCache(cache),
			v2.WithLocker(kitsync.NewPartitionRWLocker()),
			v2.WithCpClient(mockCpConnector),
			v2.WithSecretVersions(provider),
			v2.WithLogger(mockLogger),
			v2.WithStats(stats.NOP),
		)
	}

	BeforeEach(func() {
		ctrl := gomock.NewController(GinkgoT())
		identity = mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
		mockCpConnector = mock_oauthV2.NewMockConnector(ctrl)
		mockLogger = mock_logger.NewMockLogger(ctrl)
		mockLogger.EXPECT().Child("OAuthHandler").Return(mockLogger)
		mockLogger.EXPECT().Withn(gomock.Any()).Return(mockLogger).AnyTimes()
		mockLogger.EXPECT().Debugn(gomock.Any(), gomock.Any()).AnyTimes()
		cache = v2.NewOauthTokenCache()
		params = &v2.OAuthTokenParams{
			AccountID:     "acct-1",
			WorkspaceID:   "ws-1",
			DestinationID: "dest-1",
			DestType:      "testDest",
		}
	})

	cacheVersion := func(version int) v2.OAuthToken {
		token := v2.OAuthToken{Secret: []byte(cachedSecret), ExpirationDate: expirationDate, SecretVersion: version}
		cache.Store(params.AccountID, token)
		return token
	}
	expectFetch := func() {
		identity.EXPECT().Identity().Return(nil).Times(1)
		mockCpConnector.EXPECT().CpApiCall(gomock.Any()).DoAndReturn(func(request *controlplane.Request) (int, string) {
			// the default body: the control plane's stored secret is fresh, so no provider refresh is forced
			Expect(request.Body).To(MatchJSON(`{"hasExpired":true,"expiredSecret":null}`))
			return http.StatusOK, tokenResponse
		}).Times(1)
	}
	expectSuperseded := func(cached, latest int) {
		mockLogger.EXPECT().Infon(supersededLog,
			logger.NewIntField("cachedSecretVersion", int64(cached)),
			logger.NewIntField("configSecretVersion", int64(latest)),
		).Times(1)
	}

	DescribeTable("fetches a newer stored secret once, then serves it from the cache",
		func(paramsVersion int, provider v2.SecretVersionProvider) {
			cacheVersion(7)
			params.SecretVersion = paramsVersion
			expectFetch()
			expectSuperseded(7, 8)

			handler := handlerWith(provider)
			for range 2 {
				secret, err := handler.FetchToken(params)
				Expect(err).To(BeNil())
				Expect(secret).To(MatchJSON(freshSecret))
			}
			token, ok := cache.Load(params.AccountID)
			Expect(ok).To(BeTrue())
			Expect(token.SecretVersion).To(Equal(8))
			Expect(token.ExpirationDate).To(Equal(expirationDate))
		},
		Entry("the caller's destination carries the newer version (router)", 8, nil),
		Entry("the provider holds the newer version (async destinations)", 0, fakeSecretVersions{"acct-1": 8}),
		Entry("the caller's snapshot is stale and the provider is newer", 7, fakeSecretVersions{"acct-1": 8}),
		Entry("the caller's version is newer than the provider's", 8, fakeSecretVersions{"acct-1": 7}),
	)

	DescribeTable("serves cached secrets that are not superseded",
		func(cachedVersion, paramsVersion int, provider v2.SecretVersionProvider) {
			cached := cacheVersion(cachedVersion)
			params.SecretVersion = paramsVersion
			mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)
			mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

			secret, err := handlerWith(provider).FetchToken(params)
			Expect(err).To(BeNil())
			Expect(secret).To(MatchJSON(cachedSecret))
			token, _ := cache.Load(params.AccountID)
			Expect(token).To(Equal(cached))
		},
		Entry("the pod that just refreshed holds the latest version", 8, 8, fakeSecretVersions{"acct-1": 8}),
		Entry("the config is older than the cached token", 9, 8, nil),
		Entry("the cached token has no version", 0, 8, nil),
		Entry("no version is known", 7, 0, nil),
	)

	It("fetches normally when nothing is cached and keeps the response's version", func() {
		params.SecretVersion = 7 // the config poll lags the response, which carries 8: the response wins
		expectFetch()
		mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

		secret, err := handlerWith(nil).FetchToken(params)
		Expect(err).To(BeNil())
		Expect(secret).To(MatchJSON(freshSecret))
		token, ok := cache.Load(params.AccountID)
		Expect(ok).To(BeTrue())
		Expect(token.SecretVersion).To(Equal(8))
		Expect(token.ExpirationDate).To(Equal(expirationDate))
	})
})
