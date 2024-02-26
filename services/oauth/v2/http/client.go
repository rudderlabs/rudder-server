package v2

import (
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.
func OAuthHttpClient(client *http.Client, augmenter oauth_exts.Augmenter, flowType oauth.RudderFlow, tokenCache *oauth.Cache, locker *sync.PartitionRWLocker, backendConfig backendconfig.BackendConfig, getAuthErrorCategory func([]byte) (string, error), transport http.RoundTripper, oauthHandler *oauth.OAuthHandler) *http.Client {
	transportArgs := &TransportArgs{
		BackendConfig:        backendConfig,
		FlowType:             flowType,
		TokenCache:           tokenCache,
		Locker:               locker,
		GetAuthErrorCategory: getAuthErrorCategory,
		Augmenter:            augmenter,
		OAuthHandler:         oauthHandler,
		OriginalTransport:    transport,
	}
	if transportArgs.OAuthHandler == nil {
		transportArgs.OAuthHandler = oauth.NewOAuthHandler(backendConfig, oauth.WithCache(*tokenCache), oauth.WithLocker(locker))
	}
	if transportArgs.OriginalTransport == nil {
		transportArgs.OriginalTransport = client.Transport
	}
	client.Transport = NewOauthTransport(transportArgs)
	return client
}
