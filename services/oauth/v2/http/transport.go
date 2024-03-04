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
	obskit "github.com/rudderlabs/rudder-observability-kit/go/labels"
	backendconfig "github.com/rudderlabs/rudder-server/backend-config"
	oauth "github.com/rudderlabs/rudder-server/services/oauth/v2"
	oauth_exts "github.com/rudderlabs/rudder-server/services/oauth/v2/extensions"
)

/*
TransportArgs is a struct that contains the required parameters to create a new Oauth2Transport.
*/
type TransportArgs struct {
	BackendConfig backendconfig.BackendConfig
	FlowType      oauth.RudderFlow
	// TokenCache is a cache for storing OAuth tokens.
	TokenCache *oauth.Cache
	// Locker provides synchronization mechanisms.
	Locker *rudderSync.PartitionRWLocker
	// GetAuthErrorCategory is a function to get the auth error category from the response body. It can be REFRESH_TOKEN or AUTH_STATUS_INACTIVE.
	GetAuthErrorCategory func([]byte) (string, error)
	// Augmenter is an interface for augmenting requests with OAuth tokens.
	oauth_exts.Augmenter
	// OAuthHandler handles refreshToken and fetchToken requests.
	OAuthHandler *oauth.OAuthHandler
	// OriginalTransport is the underlying HTTP transport.
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
			t.log.Errorn("failed to read request body",
				obskit.DestinationID(rts.destination.DestinationId),
				obskit.WorkspaceID(rts.destination.WorkspaceID),
				obskit.DestinationType(rts.destination.DestDefName),
				logger.NewStringField("flow", string(t.flow)))
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
	// t.log.Infon("response data", logger.NewStringField("response", string(respData)))
	if err != nil {
		return nil, fmt.Errorf("failed to read response body post RoundTrip: %w", err)
	}
	interceptorResp := oauth.OAuthInterceptorResponse{}
	// internal function
	applyInterceptorRespToHttpResp := func() {
		var interceptorRespBytes []byte
		transResp := oauth.TransportResponse{
			OriginalResponse:    string(respData),
			InterceptorResponse: interceptorResp,
		}
		interceptorRespBytes, _ = json.Marshal(transResp)
		rts.res.Body = io.NopCloser(bytes.NewReader(interceptorRespBytes))
	}
	authErrorCategory, err := t.getAuthErrorCategory(respData)
	if err != nil {
		return nil, fmt.Errorf("failed to get auth error category: %s", string(respData))
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
		_, authResponse, refErr := t.oauthHandler.RefreshToken(rts.refreshTokenParams)
		if refErr != nil {
			interceptorResp.Response = refErr.Error()
		}
		// refresh token success(retry)
		// refresh token failed
		// It can be failed due to the following reasons
		// 1. invalid grant(abort)
		// 2. control plan api call failed(retry)
		// 3. some error happened while returning from RefreshToken function(retry)
		if authResponse != nil && authResponse.Err == oauth.RefTokenInvalidGrant {
			// Setting the response we obtained from trying to Refresh the token
			interceptorResp.StatusCode = http.StatusBadRequest
			t.oauthHandler.AuthStatusToggle(&oauth.AuthStatusToggleParams{
				Destination:     rts.destination,
				WorkspaceId:     rts.destination.WorkspaceID,
				RudderAccountId: rts.accountId,
				StatPrefix:      oauth.AuthStatusInactive,
				AuthStatus:      oauth.CategoryAuthStatusInactive,
			})
			// rts.res.Body = errorInRefToken
			interceptorResp.Response = authResponse.ErrorMessage
			applyInterceptorRespToHttpResp()
			return rts.res, nil
		}
		interceptorResp.StatusCode = http.StatusInternalServerError
	} else if authErrorCategory == oauth.CategoryAuthStatusInactive {
		t.oauthHandler.AuthStatusToggle(&oauth.AuthStatusToggleParams{
			Destination:     rts.destination,
			WorkspaceId:     rts.destination.WorkspaceID,
			RudderAccountId: rts.accountId,
			StatPrefix:      oauth.AuthStatusInactive,
			AuthStatus:      oauth.CategoryAuthStatusInactive,
		})
		interceptorResp.StatusCode = http.StatusBadRequest
	}
	applyInterceptorRespToHttpResp()
	// when error is not nil, the response sent will be ignored(downstream)
	return rts.res, nil
}

func (t *Oauth2Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	// TODO: Remove later
	t.log.Infon("Inside RoundTrip")
	contextData := req.Context().Value(oauth.DestKey)
	if contextData == nil {
		return httpResponseCreator(http.StatusInternalServerError, []byte("no destination found in context of the request")), nil
	}
	var destination *oauth.DestinationInfo
	switch contextData.(type) {
	case *oauth.DestinationInfo:
		destination = req.Context().Value(oauth.DestKey).(*oauth.DestinationInfo)
	default:
		return httpResponseCreator(http.StatusInternalServerError, []byte("the consent data is not of destinationInfo type")), nil
	}
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
		t.log.Errorn("accountId not found for destination",
			obskit.DestinationID(rts.destination.DestinationId),
			obskit.WorkspaceID(rts.destination.WorkspaceID),
			obskit.DestinationType(rts.destination.DestDefName),
			logger.NewStringField("flow", string(t.flow)))
		return httpResponseCreator(http.StatusInternalServerError, []byte(fmt.Sprintf("accountId not found for destination(%s) in %s flow", destination.DestinationId, t.flow))), nil
	}
	rts.refreshTokenParams = &oauth.RefreshTokenParams{
		AccountId:   rts.accountId,
		WorkspaceId: rts.destination.WorkspaceID,
		DestDefName: rts.destination.DestDefName,
		Destination: rts.destination,
	}
	rts.req = req
	errorHttpResponse := t.preRoundTrip(rts)
	if errorHttpResponse != nil {
		return errorHttpResponse, nil
	}
	res, err := t.Transport.RoundTrip(rts.req)
	if err != nil {
		return res, err
	}
	rts.res = res
	return t.postRoundTrip(rts)
}
