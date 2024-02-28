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
Also, it makes the calls to the actual endpoint and handles the response by refreshing the token if required or by updating the authStatus to "inactive".
*/
type Oauth2Transport struct {
	oauthHandler oauth.OAuthHandler
	oauth_exts.Augmenter
	Transport            http.RoundTripper
	log                  logger.Logger
	flow                 oauth.RudderFlow
	getAuthErrorCategory func([]byte) (string, error)
}

/*
This struct is used to transport common information across the pre and post round trip methods.
*/
type roundTripState struct {
	destination        *oauth.DestinationInfo
	accountId          string
	refreshTokenParams *oauth.RefreshTokenParams
	res                *http.Response
	req                *http.Request
}

func httpResponseCreator(statusCode int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(bytes.NewReader(body)),
		Header:     http.Header{"apiVersion": []string{"2"}},
	}
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

func (t *Oauth2Transport) preRoundTrip(rts *roundTripState) *http.Response {
	if t.Augmenter != nil {
		body, err := io.ReadAll(rts.req.Body)
		if err != nil {
			return httpResponseCreator(http.StatusInternalServerError, []byte(fmt.Errorf("failed to read request body pre roundTrip: %w", err).Error()))
		}
		statusCode, authResponse, err := t.oauthHandler.FetchToken(rts.refreshTokenParams)
		if statusCode == http.StatusOK {
			rts.req = rts.req.WithContext(context.WithValue(rts.req.Context(), oauth.SecretKey, authResponse.Account.Secret))
			err = t.Augmenter.Augment(rts.req, body, authResponse.Account.Secret)
			if err != nil {
				t.log.Debugn("failed to augment the secret",
					logger.NewErrorField(err))
				return httpResponseCreator(http.StatusInternalServerError, []byte(fmt.Errorf("failed to augment the secret pre roundTrip: %w", err).Error()))
			}
			return nil
		} else if authResponse != nil && authResponse.Err == oauth.RefTokenInvalidGrant {
			t.oauthHandler.AuthStatusToggle(&oauth.AuthStatusToggleParams{
				Destination:     rts.destination,
				WorkspaceId:     rts.destination.WorkspaceID,
				RudderAccountId: rts.accountId,
				StatPrefix:      oauth.AuthStatusInactive,
				AuthStatus:      oauth.CategoryAuthStatusInactive,
			})
			return httpResponseCreator(http.StatusBadRequest, []byte(err.Error()))
		}
		return httpResponseCreator(statusCode, []byte(err.Error()))
	}
	return nil
}

func (t *Oauth2Transport) postRoundTrip(rts *roundTripState) (*http.Response, error) {
	respData, err := io.ReadAll(rts.res.Body)
	if err != nil {
		return rts.res, fmt.Errorf("failed to read response body post RoundTrip: %w", err)
	}
	rts.res.Body = io.NopCloser(bytes.NewReader(respData))
	authErrorCategory, err := t.getAuthErrorCategory(respData)
	if err != nil {
		return rts.res, fmt.Errorf("failed to get auth error category: %w", err)
	}
	if authErrorCategory == oauth.CategoryRefreshToken {
		// since same token that was used to make the http call needs to be refreshed, we need the current token information
		var oldSecret json.RawMessage
		if rts.req.Context().Value(oauth.SecretKey) != nil {
			oldSecret = rts.req.Context().Value(oauth.SecretKey).(json.RawMessage)
		}
		rts.refreshTokenParams.Secret = oldSecret
		rts.refreshTokenParams.Destination = rts.destination
		t.log.Infon("refreshing token")
		statusCode, authResponse, refErr := t.oauthHandler.RefreshToken(rts.refreshTokenParams)
		if refErr != nil {
			err = refErr
		}
		if authResponse != nil && authResponse.Err == oauth.RefTokenInvalidGrant {
			// Setting the response we obtained from trying to Refresh the token
			errorInRefToken := io.NopCloser(bytes.NewBufferString(authResponse.ErrorMessage))
			rts.res.StatusCode = http.StatusBadRequest
			t.oauthHandler.AuthStatusToggle(&oauth.AuthStatusToggleParams{
				Destination:     rts.destination,
				WorkspaceId:     rts.destination.WorkspaceID,
				RudderAccountId: rts.accountId,
				StatPrefix:      oauth.AuthStatusInactive,
				AuthStatus:      oauth.CategoryAuthStatusInactive,
			})
			rts.res.Body = errorInRefToken
			return rts.res, nil
		}
		// refresh token failed --> abort the event
		// It can be failed due to the following reasons
		// 1. invalid grant
		// 2. control plan api call failed
		rts.res.StatusCode = statusCode
		if statusCode == http.StatusOK {
			// refresh token successful --> retry the event
			rts.res.StatusCode = http.StatusInternalServerError
		}

	} else if authErrorCategory == oauth.CategoryAuthStatusInactive {
		t.oauthHandler.AuthStatusToggle(&oauth.AuthStatusToggleParams{
			Destination:     rts.destination,
			WorkspaceId:     rts.destination.WorkspaceID,
			RudderAccountId: rts.accountId,
			StatPrefix:      oauth.AuthStatusInactive,
			AuthStatus:      oauth.CategoryAuthStatusInactive,
		})
		rts.res.StatusCode = http.StatusBadRequest
	}
	return rts.res, err
}

func (t *Oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Context().Value(oauth.DestKey) == nil {
		return httpResponseCreator(http.StatusInternalServerError, []byte("no destination found in context of the request")), nil
	}
	destination := req.Context().Value(oauth.DestKey).(*oauth.DestinationInfo)
	if destination == nil {
		return httpResponseCreator(http.StatusInternalServerError, []byte("no destination found in context of the request")), nil
	}
	if !destination.IsOAuthDestination() {
		return t.Transport.RoundTrip(req)
	}
	rts := &roundTripState{}
	rts.destination = destination
	if t.flow == oauth.RudderFlow_Delivery {
		rts.accountId = rts.destination.GetAccountID(oauth.DeliveryAccountIdKey)
	} else if t.flow == oauth.RudderFlow_Delete {
		rts.accountId = rts.destination.GetAccountID(oauth.DeleteAccountIdKey)
	}
	if rts.accountId == "" {
		return httpResponseCreator(http.StatusInternalServerError, []byte(fmt.Sprintf("accountId not found for destination(%s) in %s flow", destination.DestinationId, t.flow))), nil
	}
	rts.refreshTokenParams = &oauth.RefreshTokenParams{
		AccountId:   rts.accountId,
		WorkspaceId: rts.destination.WorkspaceID,
		DestDefName: rts.destination.DestDefName,
	}
	rts.req = req
	res := t.preRoundTrip(rts)
	if res != nil {
		return res, nil
	}
	res, err := t.Transport.RoundTrip(rts.req)
	if err != nil {
		return res, err
	}
	rts.res = res
	return t.postRoundTrip(rts)
}
