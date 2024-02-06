package v2

import (
	"fmt"
	"net/http"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

// type DestinationJobs []DestinationJobT

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// Oauth2Transport is an http.RoundTripper that adds the appropriate authorization information to oauth requests.
type Oauth2Transport struct {
	oauthHandler OAuthHandler
	oauth_exts.Augmenter
	Transport            http.RoundTripper
	log                  logger.Logger
	flow                 RudderFlow
	getAuthErrorCategory func([]byte) (string, error)
	destination          *backendconfig.DestinationT
	refreshTokenParams   *RefreshTokenParams
	accountId            string
}

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.

func OAuthHttpClient(client *http.Client, augmenter oauth_exts.Augmenter, flowType RudderFlow, tokenCache *Cache, locker *sync.PartitionRWLocker, backendConfig backendconfig.BackendConfig, getAuthErrorCategory func([]byte) (string, error)) *http.Client {
	transport := &Oauth2Transport{
		oauthHandler:         *NewOAuthHandler(backendConfig, WithCache(*tokenCache), WithLocker(locker)),
		Augmenter:            augmenter,
		Transport:            client.Transport,
		log:                  logger.NewLogger().Child("OAuthHttpClient"),
		flow:                 flowType,
		getAuthErrorCategory: getAuthErrorCategory,
	}
	client.Transport = transport
	return client
}

// TO DO: move the common part on respective to client move to a common function

func (t *Oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.destination = req.Context().Value("destination").(*backendconfig.DestinationT)
	if t.destination == nil {
		return nil, fmt.Errorf("no destination found in context of the request")
	}
	if !t.destination.IsOAuthDestination() {
		return t.Transport.RoundTrip(req)
	}
	err := t.preTransformerCallHandling(req)
	if err != nil {
		return nil, err
	}

	res, err := t.Transport.RoundTrip(req)

	if err != nil {
		return res, err
	}

	return t.postTransformerCallHandling(req, res)

}
