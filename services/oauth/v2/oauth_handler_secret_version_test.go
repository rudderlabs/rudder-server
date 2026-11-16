package v2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
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

const (
	svExpirationDate = "2099-01-01T00:00:00.000Z"
	svCachedSecret   = `{"access_token":"cached","expirationDate":"2099-01-01T00:00:00.000Z"}`
	svFreshSecret    = `{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"}`
	svTokenResponse  = `{"secret":{"access_token":"t","expirationDate":"2099-01-01T00:00:00.000Z"},"secretVersion":8}`
	svSupersededLog  = "cached OAuth token superseded by a newer secret in the control plane"
)

// secretVersionFixture is the state every secret version check starts from.
type secretVersionFixture struct {
	t               *testing.T
	identity        *mock_oauthV2.MockAuthIdentityProvider
	mockCpConnector *mock_oauthV2.MockConnector
	mockLogger      *mock_logger.MockLogger
	cache           v2.OauthTokenCache
	params          *v2.OAuthTokenParams
}

func newSecretVersionFixture(t *testing.T) *secretVersionFixture {
	ctrl := gomock.NewController(t)
	f := &secretVersionFixture{
		t:               t,
		identity:        mock_oauthV2.NewMockAuthIdentityProvider(ctrl),
		mockCpConnector: mock_oauthV2.NewMockConnector(ctrl),
		mockLogger:      mock_logger.NewMockLogger(ctrl),
		cache:           v2.NewOauthTokenCache(),
		params: &v2.OAuthTokenParams{
			AccountID:     "acct-1",
			WorkspaceID:   "ws-1",
			DestinationID: "dest-1",
			DestType:      "testDest",
		},
	}
	f.mockLogger.EXPECT().Child("OAuthHandler").Return(f.mockLogger)
	f.mockLogger.EXPECT().Withn(gomock.Any()).Return(f.mockLogger).AnyTimes()
	f.mockLogger.EXPECT().Debugn(gomock.Any(), gomock.Any()).AnyTimes()
	return f
}

func (f *secretVersionFixture) handlerWith(provider v2.SecretVersionProvider) v2.OAuthHandler {
	return v2.NewOAuthHandler(f.identity,
		v2.WithCache(f.cache),
		v2.WithLocker(kitsync.NewPartitionRWLocker()),
		v2.WithCpClient(f.mockCpConnector),
		v2.WithSecretVersions(provider),
		v2.WithLogger(f.mockLogger),
		v2.WithStats(stats.NOP),
	)
}

func (f *secretVersionFixture) cacheVersion(version int) v2.OAuthToken {
	token := v2.OAuthToken{Secret: []byte(svCachedSecret), ExpirationDate: svExpirationDate, SecretVersion: version}
	f.cache.Store(f.params.AccountID, token)
	return token
}

func (f *secretVersionFixture) expectFetch() {
	f.identity.EXPECT().Identity().Return(nil).Times(1)
	f.mockCpConnector.EXPECT().CpApiCall(gomock.Any()).DoAndReturn(func(request *controlplane.Request) (int, string) {
		// the default body: the control plane's stored secret is fresh, so no provider refresh is forced
		require.JSONEq(f.t, `{"hasExpired":true,"expiredSecret":null}`, request.Body)
		return http.StatusOK, svTokenResponse
	}).Times(1)
}

func (f *secretVersionFixture) expectSuperseded(cached, latest int) {
	f.mockLogger.EXPECT().Infon(svSupersededLog,
		logger.NewIntField("cachedSecretVersion", int64(cached)),
		logger.NewIntField("configSecretVersion", int64(latest)),
	).Times(1)
}

func TestSecretVersionFetchesNewerSecretOnce(t *testing.T) {
	cases := []struct {
		name          string
		paramsVersion int
		provider      v2.SecretVersionProvider
	}{
		{"the caller's destination carries the newer version (router)", 8, nil},
		{"the provider holds the newer version (async destinations)", 0, fakeSecretVersions{"acct-1": 8}},
		{"the caller's snapshot is stale and the provider is newer", 7, fakeSecretVersions{"acct-1": 8}},
		{"the caller's version is newer than the provider's", 8, fakeSecretVersions{"acct-1": 7}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSecretVersionFixture(t)
			f.cacheVersion(7)
			f.params.SecretVersion = tc.paramsVersion
			f.expectFetch()
			f.expectSuperseded(7, 8)

			handler := f.handlerWith(tc.provider)
			for range 2 {
				secret, err := handler.FetchToken(f.params)
				require.Nil(t, err)
				require.JSONEq(t, svFreshSecret, string(secret))
			}
			token, ok := f.cache.Load(f.params.AccountID)
			require.True(t, ok)
			require.Equal(t, 8, token.SecretVersion)
			require.Equal(t, svExpirationDate, token.ExpirationDate)
		})
	}
}

func TestSecretVersionServesCachedWhenNotSuperseded(t *testing.T) {
	cases := []struct {
		name          string
		cachedVersion int
		paramsVersion int
		provider      v2.SecretVersionProvider
	}{
		{"the pod that just refreshed holds the latest version", 8, 8, fakeSecretVersions{"acct-1": 8}},
		{"the config is older than the cached token", 9, 8, nil},
		{"the cached token has no version", 0, 8, nil},
		{"no version is known", 7, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newSecretVersionFixture(t)
			cached := f.cacheVersion(tc.cachedVersion)
			f.params.SecretVersion = tc.paramsVersion
			f.mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Times(0)
			f.mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

			secret, err := f.handlerWith(tc.provider).FetchToken(f.params)
			require.Nil(t, err)
			require.JSONEq(t, svCachedSecret, string(secret))
			token, _ := f.cache.Load(f.params.AccountID)
			require.Equal(t, cached, token)
		})
	}
}

func TestSecretVersionColdCacheKeepsResponseVersion(t *testing.T) {
	f := newSecretVersionFixture(t)
	f.params.SecretVersion = 7 // the config poll lags the response, which carries 8: the response wins
	f.expectFetch()
	f.mockLogger.EXPECT().Infon(gomock.Any(), gomock.Any()).Times(0)

	secret, err := f.handlerWith(nil).FetchToken(f.params)
	require.Nil(t, err)
	require.JSONEq(t, svFreshSecret, string(secret))
	token, ok := f.cache.Load(f.params.AccountID)
	require.True(t, ok)
	require.Equal(t, 8, token.SecretVersion)
	require.Equal(t, svExpirationDate, token.ExpirationDate)
}
