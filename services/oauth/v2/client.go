package v2

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rudderlabs/rudder-go-kit/cachettl"
	"github.com/rudderlabs/rudder-go-kit/logger"
	"github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
	"github.com/tidwall/gjson"
)

// type DestinationJobs []DestinationJobT

var jsonfast = jsoniter.ConfigCompatibleWithStandardLibrary

// Oauth2Transport is an http.RoundTripper that adds the appropriate authorization information to oauth requests.
type Oauth2Transport struct {
	oauthHandler OAuthHandler
	oauth_exts.Augmenter
	Transport        http.RoundTripper
	baseURL          string
	keyLocker        *sync.PartitionLocker
	log              logger.Logger
	flow             RudderFlow
	httpResponsePath string
}

// OAuthHttpClient returns an http client that will add the appropriate authorization information to oauth requests.
// TODO: update the cache argument as an interface
func OAuthHttpClient(client *http.Client, baseURL string, augmenter oauth_exts.Augmenter, flowType RudderFlow, httpResponsePath string, tokenCache *cachettl.Cache[CacheKey, *AccessToken], locker *sync.PartitionLocker) *http.Client {
	client.Transport = &Oauth2Transport{
		oauthHandler:     *NewOAuthHandler(backendconfig.DefaultBackendConfig),
		Augmenter:        augmenter,
		Transport:        client.Transport,
		baseURL:          baseURL,
		log:              logger.NewLogger().Child("OAuthHttpClient"),
		flow:             flowType,
		httpResponsePath: httpResponsePath,
	}
	return client
}

type TransformerResponse struct {
	AuthErrorCategory string `json:"authErrorCategory"`
}

func (t *Oauth2Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	destination := r.Context().Value("destination").(*backendconfig.DestinationT)
	if destination == nil {
		return nil, fmt.Errorf("no destination found in context")
	}
	if !destination.IsOAuthDestination() {
		return t.Transport.RoundTrip(r)
	}
	var accountId string
	if t.flow == RudderFlow_Delivery {
		accountId = destination.GetAccountID("rudderAccountId")
	} else if t.flow == RudderFlow_Delete {
		accountId = destination.GetAccountID("rudderDeleteAccountId")
	}

	refreshTokenParams := &RefreshTokenParams{
		AccountId:   accountId,
		WorkspaceId: destination.WorkspaceID,
		DestDefName: destination.DestinationDefinition.Name,
	}
	body, _ := io.ReadAll(r.Body)
	fmt.Println("body", string(body))
	defer io.ReadCloser.Close(r.Body)
	matched, _ := regexp.MatchString(r.URL.Path, "/routerTransform")
	cacheKey := CacheKey{
		WorkspaceID: destination.WorkspaceID,
		AccountID:   accountId,
	}
	if matched {
		t.Augmenter.Augment(r, body, func() json.RawMessage {
			globalLock.Lock(accountId)
			defer globalLock.Unlock(accountId)
			if globalTokenCache.Get(cacheKey) != nil {
				return globalTokenCache.Get(cacheKey).Token
			} else {
				statusCode, authResponse := t.oauthHandler.FetchToken(refreshTokenParams)
				if statusCode == http.StatusOK {
					expirationDate, _ := time.Parse("", authResponse.Account.ExpirationDate)
					globalTokenCache.Put(cacheKey, &AccessToken{
						Token:          authResponse.Account.Secret,
						ExpirationDate: expirationDate,
					}, time.Hour)
					return authResponse.Account.Secret
				}
			}
			return nil
		})
	} else {
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	res, _ := t.Transport.RoundTrip(r)
	respData, _ := io.ReadAll(res.Body)
	defer res.Body.Close()
	res.Body = io.NopCloser(bytes.NewReader(respData))
	transformedJobs := &TransformerResponse{}
	err := jsonfast.Unmarshal([]byte(gjson.GetBytes(respData, t.httpResponsePath).Raw), &transformedJobs)
	if err != nil {
		fmt.Println(err)
		return res, err
	}

	if transformedJobs.AuthErrorCategory == REFRESH_TOKEN {
		globalLock.Lock(accountId)
		storedToken := globalTokenCache.Get(cacheKey)
		if storedToken != nil {
			refreshTokenParams.Secret = storedToken.Token
		} else {
			refreshTokenParams.Secret = t.oauthHandler.destAuthInfoMap[accountId].Account.Secret
			accessToken := &AccessToken{
				Token: t.oauthHandler.destAuthInfoMap[accountId].Account.Secret,
			}
			globalTokenCache.Put(cacheKey, accessToken, time.Hour)
		}
		globalLock.Unlock(accountId)
		t.oauthHandler.logger.Info("refreshing token")
		statusCode, refreshedToken := t.oauthHandler.RefreshToken(refreshTokenParams)
		refreshedExpirationDate, _ := time.Parse("", refreshedToken.Account.ExpirationDate)
		globalLock.Lock(accountId)
		if statusCode == http.StatusOK {
			globalTokenCache.Put(cacheKey, &AccessToken{
				Token:          refreshedToken.Account.Secret,
				ExpirationDate: refreshedExpirationDate}, time.Hour)
		}
		globalLock.Unlock(accountId)
		res.StatusCode = 500
	} else if transformedJobs.AuthErrorCategory == AUTH_STATUS_INACTIVE {
		res.StatusCode = t.oauthHandler.updateAuthStatusToInactive(destination, destination.WorkspaceID, accountId)
	}
	return res, err
}
