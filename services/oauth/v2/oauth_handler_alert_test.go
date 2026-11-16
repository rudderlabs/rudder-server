package v2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats/memstats"
	kitsync "github.com/rudderlabs/rudder-go-kit/sync"

	mock_oauthV2 "github.com/rudderlabs/rudder-server/mocks/services/oauthV2"
	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
)

// A refused refresh (ref_token_invalid_grant) is counted on oauth_action_request with the destinationId tag.
func TestRefreshFailureTagsDestinationID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockCpConnector := mock_oauthV2.NewMockConnector(ctrl)
	mockCpConnector.EXPECT().CpApiCall(gomock.Any()).Return(http.StatusBadRequest, `{
		"body":{
		  "code":"ref_token_invalid_grant",
		  "message":"invalid_grant error, refresh token has expired or revoked"
		}
	  }`)
	mockAuthIdentityProvider := mock_oauthV2.NewMockAuthIdentityProvider(ctrl)
	mockAuthIdentityProvider.EXPECT().Identity().Return(nil)

	statsStore, err := memstats.New()
	require.NoError(t, err)

	params := &v2.OAuthTokenParams{
		AccountID:     "123",
		WorkspaceID:   "456",
		DestType:      "testDest",
		DestinationID: "dest-1",
	}
	previousSecret := []byte(`{"access_token":"storedAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)
	cache := v2.NewOauthTokenCache()
	cache.Store(params.AccountID, v2.OAuthToken{Secret: previousSecret})
	handler := v2.NewOAuthHandler(mockAuthIdentityProvider,
		v2.WithCache(cache),
		v2.WithLocker(kitsync.NewPartitionRWLocker()),
		v2.WithStats(statsStore),
		v2.WithLogger(logger.NewLogger().Child("MockOAuthHandler")),
		v2.WithCpClient(mockCpConnector),
	)

	_, scErr := handler.RefreshToken(params, previousSecret)
	require.NotNil(t, scErr)

	metrics := statsStore.GetByName("oauth_action_request")
	require.Len(t, metrics, 1)
	require.Equal(t, common.RefTokenInvalidGrant, metrics[0].Tags["errorMessage"])
	require.Equal(t, "dest-1", metrics[0].Tags["destinationId"])
	require.Equal(t, "123", metrics[0].Tags["id"])
	require.Equal(t, "456", metrics[0].Tags["workspaceId"])
	require.EqualValues(t, 1, metrics[0].Value)
}
