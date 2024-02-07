package v2

import (
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

// type DestinationJobs []DestinationJobT

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.

func OAuthHttpClient(client *http.Client, augmenter oauth_exts.Augmenter, flowType oauth.RudderFlow, tokenCache *oauth.Cache, locker *sync.PartitionRWLocker, backendConfig backendconfig.BackendConfig, getAuthErrorCategory func([]byte) (string, error)) *http.Client {
	client.Transport = NewOauthTransport(&TransportArgs{
		BackendConfig:        backendConfig,
		FlowType:             flowType,
		TokenCache:           tokenCache,
		Locker:               locker,
		GetAuthErrorCategory: getAuthErrorCategory,
		Augmenter:            augmenter,
		OAuthHandler:         oauth.NewOAuthHandler(backendConfig, oauth.WithCache(*tokenCache), oauth.WithLocker(locker)),
		OriginalTransport:    client.Transport,
	})
	return client
}
