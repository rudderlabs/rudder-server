package v2_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	v2 "github.com/rudderlabs/rudder-server/services/oauth/v2"
)

func TestOAuthTokenCache(t *testing.T) {
	t.Run("Test Cache", func(t *testing.T) {
		cache := v2.NewOauthTokenCache()
		require.NotNil(t, cache)
	})
	t.Run("Test Get and Set", func(t *testing.T) {
		cache := v2.NewOauthTokenCache()
		authToken := v2.OAuthToken{
			ExpirationDate: "2022-06-29T15:34:47.758Z",
			Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
		}
		cache.Store("test", authToken)
		value, ok := cache.Load("test")
		require.True(t, ok)
		require.Equal(t, authToken, value)
	})
	t.Run("Test Delete", func(t *testing.T) {
		cache := v2.NewOauthTokenCache()
		authToken := v2.OAuthToken{
			ExpirationDate: "2022-06-29T15:34:47.758Z",
			Secret:         json.RawMessage([]byte(`{"access_token":"validAccessToken","refresh_token":"dummyRefreshToken","developer_token":"dummyDeveloperToken"}`)),
		}
		cache.Store("test", authToken)
		value, ok := cache.Load("test")
		require.True(t, ok)
		require.Equal(t, authToken, value)
		cache.Delete("test")
		value, ok = cache.Load("test")
		require.False(t, ok)
		var zero v2.OAuthToken
		require.Equal(t, zero, value)
	})
}
