package v2

import (
	"fmt"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

// Oauth2Transport is an http.RoundTripper that adds the appropriate authorization information to oauth requests.
type Oauth2Transport struct {
	oauthHandler OAuthHandler
	oauth_exts.Augmenter
	Transport  http.RoundTripper
	baseURL    string
	keyLocker  *sync.PartitionLocker
	tokenCache *cachettl.Cache[CacheKey, *AccessToken]
	log        logger.Logger
	flow       RudderFlow
}

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.
func OAuthHttpClient(client *http.Client, baseURL string, augmenter oauth_exts.Augmenter, flowType RudderFlow) *http.Client {
	client.Transport = &Oauth2Transport{
		oauthHandler: *NewOAuthHandler(backendconfig.DefaultBackendConfig),
		Augmenter:    augmenter,
		Transport:    client.Transport,
		baseURL:      baseURL,
		tokenCache:   cachettl.New[CacheKey, *AccessToken](),
		keyLocker:    sync.NewPartitionLocker(),
		log:          logger.NewLogger().Child("OAuthHttpClient"),
		flow:         flowType,
	}
	return client
}

func (t *Oauth2Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	// fmt.Println(r.Context().Value("destination"))
	destination := r.Context().Value("destination").(*backendconfig.DestinationT)
	if destination == nil {
		return nil, fmt.Errorf("no destination found in context")
	}
	if !destination.IsOAuthDestination() {
		return t.Transport.RoundTrip(r)
	}
	accountId := destination.GetAccountID("rudderAccountId")
	refreshTokenParams := &RefreshTokenParams{}
	fetchStatus, response := t.oauthHandler.FetchToken(refreshTokenParams)
	body, _ := io.ReadAll(r.Body)
	if fetchStatus == 200 {
		t.Augment(r, body, response.Account.Secret)
	}
	t.keyLocker.Lock(accountId)
	t.keyLocker.Unlock(accountId)
	return nil, nil
}
