// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package oauth2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// TokenRetriever implements AuthTokenExchanger in order to facilitate getting
// Tokens
type TokenRetriever struct {
	transport HTTPAuthTransport
}

// AuthorizationTokenResponse is the HTTP response when asking for a new token.
// Note that not all fields will contain data based on what kind of request was
// sent
type AuthorizationTokenResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	IDToken      string `json:"id_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
}

// AuthorizationCodeExchangeRequest is used to request the exchange of an
// authorization code for a token
type AuthorizationCodeExchangeRequest struct {
	TokenEndpoint string
	ClientID      string
	CodeVerifier  string
	Code          string
	RedirectURI   string
}

// RefreshTokenExchangeRequest is used to request the exchange of a refresh
// token for a refreshed token
type RefreshTokenExchangeRequest struct {
	TokenEndpoint string
	ClientID      string
	RefreshToken  string
}

// ClientCredentialsExchangeRequest is used to request the exchange of
// client credentials for a token
type ClientCredentialsExchangeRequest struct {
	TokenEndpoint string
	ClientID      string
	ClientSecret  string
	Audience      string
	Scopes        []string
}

// DeviceCodeExchangeRequest is used to request the exchange of
// a device code for a token
type DeviceCodeExchangeRequest struct {
	TokenEndpoint string
	ClientID      string
	DeviceCode    string
	PollInterval  time.Duration
}

// TokenErrorResponse is used to parse error responses from the token endpoint
type TokenErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type TokenError struct {
	ErrorCode        string
	ErrorDescription string
}

func (e *TokenError) Error() string {
	if e.ErrorDescription != "" {
		return fmt.Sprintf("%s (%s)", e.ErrorDescription, e.ErrorCode)
	}
	return e.ErrorCode
}

// HTTPAuthTransport abstracts how an HTTP exchange request is sent and received
type HTTPAuthTransport interface {
	Do(request *http.Request) (*http.Response, error)
}

// NewTokenRetriever allows a TokenRetriever the internal of a new
// TokenRetriever to be easily set up
func NewTokenRetriever(authTransport HTTPAuthTransport) *TokenRetriever {
	return &TokenRetriever{
		transport: authTransport,
	}
}

// newExchangeCodeRequest builds a new AuthTokenRequest wrapped in an
// http.Request
func (ce *TokenRetriever) newExchangeCodeRequest(
	req AuthorizationCodeExchangeRequest) (*http.Request, error) {
	uv := url.Values{}
	uv.Set("grant_type", "authorization_code")
	uv.Set("client_id", req.ClientID)
	uv.Set("code_verifier", req.CodeVerifier)
	uv.Set("code", req.Code)
	uv.Set("redirect_uri", req.RedirectURI)

	euv := uv.Encode()

	request, err := http.NewRequest("POST",
		req.TokenEndpoint,
		strings.NewReader(euv),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(euv)))

	return request, nil
}

// newDeviceCodeExchangeRequest builds a new DeviceCodeExchangeRequest wrapped in an
// http.Request
func (ce *TokenRetriever) newDeviceCodeExchangeRequest(
	req DeviceCodeExchangeRequest) (*http.Request, error) {
	uv := url.Values{}
	uv.Set("grant_type", "urn:ietf:params:oauth:grant-type:device_code")
	uv.Set("client_id", req.ClientID)
	uv.Set("device_code", req.DeviceCode)
	euv := uv.Encode()

	request, err := http.NewRequest("POST",
		req.TokenEndpoint,
		strings.NewReader(euv),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(euv)))

	return request, nil
}

// newRefreshTokenRequest builds a new RefreshTokenRequest wrapped in an
// http.Request
func (ce *TokenRetriever) newRefreshTokenRequest(req RefreshTokenExchangeRequest) (*http.Request, error) {
	uv := url.Values{}
	uv.Set("grant_type", "refresh_token")
	uv.Set("client_id", req.ClientID)
	uv.Set("refresh_token", req.RefreshToken)

	euv := uv.Encode()

	request, err := http.NewRequest("POST",
		req.TokenEndpoint,
		strings.NewReader(euv),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(euv)))

	return request, nil
}

// newClientCredentialsRequest builds a new ClientCredentialsExchangeRequest wrapped in an
// http.Request
func (ce *TokenRetriever) newClientCredentialsRequest(req ClientCredentialsExchangeRequest) (*http.Request, error) {
	uv := url.Values{}
	uv.Set("grant_type", "client_credentials")
	uv.Set("client_id", req.ClientID)
	uv.Set("client_secret", req.ClientSecret)
	if len(req.Scopes) > 0 {
		uv.Set("scope", strings.Join(req.Scopes, " "))
	}
	if req.Audience != "" {
		// Audience is an Auth0 extension; other providers use scopes to similar effect.
		uv.Set("audience", req.Audience)
	}

	euv := uv.Encode()

	request, err := http.NewRequest("POST",
		req.TokenEndpoint,
		strings.NewReader(euv),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Add("Content-Length", strconv.Itoa(len(euv)))

	return request, nil
}

// ExchangeCode uses the AuthCodeExchangeRequest to exchange an authorization
// code for tokens
func (ce *TokenRetriever) ExchangeCode(req AuthorizationCodeExchangeRequest) (*TokenResult, error) {
	request, err := ce.newExchangeCodeRequest(req)
	if err != nil {
		return nil, err
	}

	response, err := ce.transport.Do(request)
	if err != nil {
		return nil, err
	}

	return ce.handleAuthTokensResponse(response)
}

// handleAuthTokensResponse takes care of checking an http.Response that has
// auth tokens for errors and parsing the raw body to a TokenResult struct
func (ce *TokenRetriever) handleAuthTokensResponse(resp *http.Response) (*TokenResult, error) {
	if resp.Body != nil {
		defer resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		cth := resp.Header.Get("Content-Type")
		if cth == "" {
			cth = "application/json"
		}
		ct, _, err := mime.ParseMediaType(cth)
		if err != nil {
			return nil, fmt.Errorf("unprocessable content type: %s: %w", cth, err)
		}
		if ct == "application/json" {
			er := TokenErrorResponse{}
			err := json.NewDecoder(resp.Body).Decode(&er)
			if err != nil {
				return nil, err
			}
			return nil, &TokenError{ErrorCode: er.Error, ErrorDescription: er.ErrorDescription}
		}
		return nil, fmt.Errorf("a non-success status code was received: %d", resp.StatusCode)
	}

	atr := AuthorizationTokenResponse{}
	err := json.NewDecoder(resp.Body).Decode(&atr)
	if err != nil {
		return nil, err
	}

	return &TokenResult{
		AccessToken:  atr.AccessToken,
		IDToken:      atr.IDToken,
		RefreshToken: atr.RefreshToken,
		ExpiresIn:    atr.ExpiresIn,
	}, nil
}

// ExchangeDeviceCode uses the DeviceCodeExchangeRequest to exchange a device
// code for tokens
func (ce *TokenRetriever) ExchangeDeviceCode(ctx context.Context, req DeviceCodeExchangeRequest) (*TokenResult, error) {
	for {
		request, err := ce.newDeviceCodeExchangeRequest(req)
		if err != nil {
			return nil, err
		}

		response, err := ce.transport.Do(request)
		if err != nil {
			return nil, err
		}
		token, err := ce.handleAuthTokensResponse(response)
		if err == nil {
			return token, nil
		}
		terr, ok := err.(*TokenError)
		if !ok {
			return nil, err
		}
		switch terr.ErrorCode {
		case "expired_token":
			// The user has not authorized the device quickly enough, so the device_code has expired.
			return nil, fmt.Errorf("the device code has expired")
		case "access_denied":
			// The user refused to authorize the device
			return nil, fmt.Errorf("the device was not authorized")
		case "authorization_pending":
			// Still waiting for the user to take action
		case "slow_down":
			// You are polling too fast
		}

		select {
		case <-time.After(req.PollInterval):
			continue
		case <-ctx.Done():
			return nil, errors.New("cancelled")
		}
	}
}

// ExchangeRefreshToken uses the RefreshTokenExchangeRequest to exchange a
// refresh token for refreshed tokens
func (ce *TokenRetriever) ExchangeRefreshToken(req RefreshTokenExchangeRequest) (*TokenResult, error) {
	request, err := ce.newRefreshTokenRequest(req)
	if err != nil {
		return nil, err
	}

	response, err := ce.transport.Do(request)
	if err != nil {
		return nil, err
	}

	return ce.handleAuthTokensResponse(response)
}

// ExchangeClientCredentials uses the ClientCredentialsExchangeRequest to exchange
// client credentials for tokens
func (ce *TokenRetriever) ExchangeClientCredentials(req ClientCredentialsExchangeRequest) (*TokenResult, error) {
	request, err := ce.newClientCredentialsRequest(req)
	if err != nil {
		return nil, err
	}

	response, err := ce.transport.Do(request)
	if err != nil {
		return nil, err
	}

	return ce.handleAuthTokensResponse(response)
}
