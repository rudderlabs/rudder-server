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
	"fmt"
	"net/http"
	"time"

	"github.com/apache/pulsar-client-go/oauth2/clock"

	"github.com/pkg/errors"
)

// DeviceCodeFlow takes care of the mechanics needed for getting an access
// token using the OAuth 2.0 "Device Code Flow"
type DeviceCodeFlow struct {
	options                DeviceCodeFlowOptions
	oidcWellKnownEndpoints OIDCWellKnownEndpoints
	codeProvider           DeviceCodeProvider
	exchanger              DeviceTokenExchanger
	callback               DeviceCodeCallback
	clock                  clock.Clock
}

// AuthorizationCodeProvider abstracts getting an authorization code
type DeviceCodeProvider interface {
	GetCode(audience string, additionalScopes ...string) (*DeviceCodeResult, error)
}

// DeviceTokenExchanger abstracts exchanging for tokens
type DeviceTokenExchanger interface {
	ExchangeDeviceCode(ctx context.Context, req DeviceCodeExchangeRequest) (*TokenResult, error)
	ExchangeRefreshToken(req RefreshTokenExchangeRequest) (*TokenResult, error)
}

type DeviceCodeCallback func(code *DeviceCodeResult) error

type DeviceCodeFlowOptions struct {
	IssuerEndpoint   string
	ClientID         string
	AdditionalScopes []string
	AllowRefresh     bool
}

func newDeviceCodeFlow(
	options DeviceCodeFlowOptions,
	oidcWellKnownEndpoints OIDCWellKnownEndpoints,
	codeProvider DeviceCodeProvider,
	exchanger DeviceTokenExchanger,
	callback DeviceCodeCallback,
	clock clock.Clock) *DeviceCodeFlow {
	return &DeviceCodeFlow{
		options:                options,
		oidcWellKnownEndpoints: oidcWellKnownEndpoints,
		codeProvider:           codeProvider,
		exchanger:              exchanger,
		callback:               callback,
		clock:                  clock,
	}
}

// NewDefaultDeviceCodeFlow provides an easy way to build up a default
// device code flow with all the correct configuration. If refresh tokens should
// be allowed pass in true for <allowRefresh>
func NewDefaultDeviceCodeFlow(options DeviceCodeFlowOptions,
	callback DeviceCodeCallback) (*DeviceCodeFlow, error) {
	wellKnownEndpoints, err := GetOIDCWellKnownEndpointsFromIssuerURL(options.IssuerEndpoint)
	if err != nil {
		return nil, err
	}

	codeProvider := NewLocalDeviceCodeProvider(
		LocalDeviceCodeProviderOptions{
			ClientID: options.ClientID,
		},
		*wellKnownEndpoints,
		&http.Client{},
	)

	tokenRetriever := NewTokenRetriever(&http.Client{})

	return newDeviceCodeFlow(
		options,
		*wellKnownEndpoints,
		codeProvider,
		tokenRetriever,
		callback,
		clock.RealClock{}), nil
}

var _ Flow = &DeviceCodeFlow{}

func (p *DeviceCodeFlow) Authorize(audience string) (*AuthorizationGrant, error) {

	var additionalScopes []string
	additionalScopes = append(additionalScopes, p.options.AdditionalScopes...)
	if p.options.AllowRefresh {
		additionalScopes = append(additionalScopes, "offline_access")
	}

	codeResult, err := p.codeProvider.GetCode(audience, additionalScopes...)
	if err != nil {
		return nil, err
	}

	if p.callback != nil {
		err := p.callback(codeResult)
		if err != nil {
			return nil, err
		}
	}

	exchangeRequest := DeviceCodeExchangeRequest{
		TokenEndpoint: p.oidcWellKnownEndpoints.TokenEndpoint,
		ClientID:      p.options.ClientID,
		DeviceCode:    codeResult.DeviceCode,
		PollInterval:  time.Duration(codeResult.Interval) * time.Second,
	}

	tr, err := p.exchanger.ExchangeDeviceCode(context.Background(), exchangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "could not exchange code")
	}

	token := convertToOAuth2Token(tr, p.clock)
	grant := &AuthorizationGrant{
		Type:          GrantTypeDeviceCode,
		Audience:      audience,
		ClientID:      p.options.ClientID,
		TokenEndpoint: p.oidcWellKnownEndpoints.TokenEndpoint,
		Token:         &token,
		Scopes:        additionalScopes,
	}
	return grant, nil
}

type DeviceAuthorizationGrantRefresher struct {
	exchanger DeviceTokenExchanger
	clock     clock.Clock
}

// NewDefaultDeviceAuthorizationGrantRefresher constructs a grant refresher based on the result
// of the device authorization flow.
func NewDefaultDeviceAuthorizationGrantRefresher(clock clock.Clock) (*DeviceAuthorizationGrantRefresher, error) {
	tokenRetriever := NewTokenRetriever(&http.Client{})
	return &DeviceAuthorizationGrantRefresher{
		exchanger: tokenRetriever,
		clock:     clock,
	}, nil
}

var _ AuthorizationGrantRefresher = &DeviceAuthorizationGrantRefresher{}

func (g *DeviceAuthorizationGrantRefresher) Refresh(grant *AuthorizationGrant) (*AuthorizationGrant, error) {
	if grant.Type != GrantTypeDeviceCode {
		return nil, errors.New("unsupported grant type")
	}
	if grant.Token == nil || grant.Token.RefreshToken == "" {
		return nil, fmt.Errorf("the authorization grant has expired (no refresh token); please re-login")
	}

	exchangeRequest := RefreshTokenExchangeRequest{
		TokenEndpoint: grant.TokenEndpoint,
		ClientID:      grant.ClientID,
		RefreshToken:  grant.Token.RefreshToken,
	}
	tr, err := g.exchanger.ExchangeRefreshToken(exchangeRequest)
	if err != nil {
		return nil, errors.Wrap(err, "could not exchange refresh token")
	}

	// RFC 6749 Section 1.5 - token exchange MAY issue a new refresh token (otherwise the result is blank).
	// also see: https://tools.ietf.org/html/draft-ietf-oauth-security-topics-13#section-4.12
	if tr.RefreshToken == "" {
		tr.RefreshToken = grant.Token.RefreshToken
	}

	token := convertToOAuth2Token(tr, g.clock)
	grant = &AuthorizationGrant{
		Type:          GrantTypeDeviceCode,
		Audience:      grant.Audience,
		ClientID:      grant.ClientID,
		Token:         &token,
		TokenEndpoint: grant.TokenEndpoint,
		Scopes:        grant.Scopes,
	}
	return grant, nil
}
