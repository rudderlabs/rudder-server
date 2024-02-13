package v2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/rudderlabs/rudder-go-kit/logger"
	rudderSync "github.com/rudderlabs/rudder-go-kit/sync"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

/*
TransportArgs is a struct that contains the required parameters to create a new Oauth2Transport.
*/
type TransportArgs struct {
	BackendConfig        backendconfig.BackendConfig
	FlowType             oauth.RudderFlow
	TokenCache           *oauth.Cache
	Locker               *rudderSync.PartitionRWLocker
	GetAuthErrorCategory func([]byte) (string, error)
	oauth_exts.Augmenter
	OAuthHandler      *oauth.OAuthHandler
	OriginalTransport http.RoundTripper
}

/*
Oauth2Transport is an http.RoundTripper that adds the appropriate authorization information to oauth requests.
Also, it makes the calls to the actual endpoint and handles the response by refreshing the token if required or by disabling the authStatus.
*/
type Oauth2Transport struct {
	oauthHandler oauth.OAuthHandler
	oauth_exts.Augmenter
	Transport            http.RoundTripper
	log                  logger.Logger
	flow                 oauth.RudderFlow
	getAuthErrorCategory func([]byte) (string, error)
	destination          *backendconfig.DestinationT
	refreshTokenParams   *oauth.RefreshTokenParams
	accountId            string
}

func NewOauthTransport(args *TransportArgs) *Oauth2Transport {
	return &Oauth2Transport{
		oauthHandler:         *args.OAuthHandler,
		Augmenter:            args.Augmenter,
		Transport:            args.OriginalTransport,
		log:                  logger.NewLogger().Child("OAuthHttpClient"),
		flow:                 args.FlowType,
		getAuthErrorCategory: args.GetAuthErrorCategory,
	}
}

func (t *Oauth2Transport) preRoundTrip(req *http.Request) error {
	if t.flow == oauth.RudderFlow_Delivery {
		t.accountId = t.destination.GetAccountID(oauth.DeliveryAccountIdKey)
	} else if t.flow == oauth.RudderFlow_Delete {
		t.accountId = t.destination.GetAccountID(oauth.DeleteAccountIdKey)
	}

	t.refreshTokenParams = &oauth.RefreshTokenParams{
		AccountId:   t.accountId,
		WorkspaceId: t.destination.WorkspaceID,
		DestDefName: t.destination.DestinationDefinition.Name,
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return fmt.Errorf("failed to read request body: %w", err)
	}
	if t.Augmenter != nil {
		fetchErr := t.Augmenter.Augment(req, body, func() (json.RawMessage, error) {
			statusCode, authResponse, err := t.oauthHandler.FetchToken(t.refreshTokenParams)
			if statusCode == http.StatusOK {
				req = req.WithContext(context.WithValue(req.Context(), "secret", authResponse.Account.Secret))
				return authResponse.Account.Secret, nil
			}
			return nil, err
		})
		if fetchErr != nil {
			return fmt.Errorf("failed to fetch token: %w", fetchErr)
		}
	} else {
		req.Body = io.NopCloser(bytes.NewReader(body))
	}
	return nil
}

func (t *Oauth2Transport) postRoundTrip(req *http.Request, res *http.Response) (*http.Response, error) {
	respData, _ := io.ReadAll(res.Body)
	res.Body = io.NopCloser(bytes.NewReader(respData))
	authErrorCategory, err := t.getAuthErrorCategory(respData)
	if err != nil {
		return res, err
	}
	if authErrorCategory == oauth.REFRESH_TOKEN {
		// since same token that was used to make the http call needs to be refreshed, we need the current token information
		var oldSecret json.RawMessage
		if req.Context().Value("secret") != nil {
			oldSecret = req.Context().Value("secret").(json.RawMessage)
		}
		t.refreshTokenParams.Secret = oldSecret
		t.refreshTokenParams.Destination = t.destination
		t.log.Info("refreshing token")
		statusCode, _, refErr := t.oauthHandler.RefreshToken(t.refreshTokenParams)
		if statusCode == http.StatusOK {
			// refresh token successful --> retry the event
			res.StatusCode = http.StatusInternalServerError
		} else {
			// refresh token failed --> abort the event
			// It can be failed due to the following reasons
			// 1. invalid grant
			// 2. control plan api call failed
			res.StatusCode = statusCode
		}
		if refErr != nil {
			err = fmt.Errorf("failed to refresh token: %w", refErr)
		}
		// // When expirationDate is available, only then parse
		// refer this: router/handle.go ---> handleOAuthDestResponse & make relevant changes
	} else if authErrorCategory == oauth.AUTH_STATUS_INACTIVE {
		res.StatusCode = t.oauthHandler.UpdateAuthStatusToInactive(t.destination, t.destination.WorkspaceID, t.accountId)
	}
	return res, err
}

func (t *Oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.destination = req.Context().Value("destination").(*backendconfig.DestinationT)
	if t.destination == nil {
		return nil, fmt.Errorf("no destination found in context of the request")
	}
	if !t.destination.IsOAuthDestination() {
		return t.Transport.RoundTrip(req)
	}
	err := t.preRoundTrip(req)
	if err != nil {
		return nil, err
	}
	res, err := t.Transport.RoundTrip(req)
	if err != nil {
		return res, err
	}
	return t.postRoundTrip(req, res)
}
