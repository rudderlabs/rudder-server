package v2

import (
	"net/http"
	"time"

	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/stats"
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

// NewOAuthHttpClient returns a http client that will add the appropriate authorization information to oauth requests.
func NewOAuthHttpClient(client *http.Client, flowType common.RudderFlow, tokenCache *oauth.Cache, backendConfig backendconfig.BackendConfig, getAuthErrorCategory func([]byte) (string, error), opArgs *HttpClientOptionalArgs) *http.Client {
	oauthHandler := opArgs.OAuthHandler
	originalTransport := opArgs.Transport
	if oauthHandler == nil {
		oauthHandler = oauth.NewOAuthHandler(backendConfig,
			oauth.WithCache(*tokenCache),
			oauth.WithLocker(opArgs.Locker),
			oauth.WithExpirationTimeDiff(opArgs.ExpirationTimeDiff),
			oauth.WithLogger(opArgs.Logger),
			oauth.WithStats(stats.Default),
		)
	}
	if originalTransport == nil {
		originalTransport = client.Transport
	}
	client.Transport = NewOAuthTransport(&TransportArgs{
		BackendConfig:        backendConfig,
		FlowType:             flowType,
		TokenCache:           tokenCache,
		Locker:               opArgs.Locker,
		GetAuthErrorCategory: getAuthErrorCategory,
		Augmenter:            opArgs.Augmenter,
		OAuthHandler:         oauthHandler,
		OriginalTransport:    originalTransport,
		logger:               opArgs.Logger,
	})
	return client
}
