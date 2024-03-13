package v2

import (
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	"github.com/rudderlabs/rudder-server/services/oauth/v2/common"
	oauthexts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

type HttpClientOptionalArgs struct {
	Transport          http.RoundTripper
	Augmenter          oauthexts.Augmenter
	Locker             *sync.PartitionRWLocker
	OAuthHandler       *oauth.OAuthHandler
	ExpirationTimeDiff time.Duration
	Logger             logger.Logger
}

// OAuthHttpClient returns a http client that will add the appropriate authorization information to oauth requests.
func OAuthHttpClient(client *http.Client, flowType common.RudderFlow, tokenCache *oauth.Cache, backendConfig backendconfig.BackendConfig, getAuthErrorCategory func([]byte) (string, error), opArgs *HttpClientOptionalArgs) *http.Client {
	transportArgs := &TransportArgs{
		BackendConfig:        backendConfig,
		FlowType:             flowType,
		TokenCache:           tokenCache,
		Locker:               opArgs.Locker,
		GetAuthErrorCategory: getAuthErrorCategory,
		Augmenter:            opArgs.Augmenter,
		OAuthHandler:         opArgs.OAuthHandler,
		OriginalTransport:    opArgs.Transport,
	}
	if transportArgs.OAuthHandler == nil {
		transportArgs.OAuthHandler = oauth.NewOAuthHandler(backendConfig,
			oauth.WithCache(*tokenCache),
			oauth.WithLocker(opArgs.Locker),
			oauth.WithExpirationTimeDiff(opArgs.ExpirationTimeDiff),
			oauth.WithLogger(opArgs.Logger),
		)
	}
	if transportArgs.OriginalTransport == nil {
		transportArgs.OriginalTransport = client.Transport
	}
	client.Transport = NewOauthTransport(transportArgs)
	return client
}
