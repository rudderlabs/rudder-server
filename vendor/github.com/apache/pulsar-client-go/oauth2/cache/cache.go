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

package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/oauth2"
	"github.com/apache/pulsar-client-go/oauth2/store"

	"github.com/apache/pulsar-client-go/oauth2/clock"
	xoauth2 "golang.org/x/oauth2"
)

// A CachingTokenSource is anything that can return a token, and is backed by a cache.
type CachingTokenSource interface {
	xoauth2.TokenSource

	// InvalidateToken is called when the token is rejected by the resource server.
	InvalidateToken() error
}

const (
	// expiryDelta adjusts the token TTL to avoid using tokens which are almost expired
	expiryDelta = time.Duration(60) * time.Second
)

// tokenCache implements a cache for the token associated with a specific audience.
// it interacts with the store when the access token is near expiration or invalidated.
// it is advisable to use a token cache instance per audience.
type tokenCache struct {
	clock     clock.Clock
	lock      sync.Mutex
	store     store.Store
	audience  string
	refresher oauth2.AuthorizationGrantRefresher
	token     *xoauth2.Token
}

func NewDefaultTokenCache(store store.Store, audience string,
	refresher oauth2.AuthorizationGrantRefresher) (CachingTokenSource, error) {
	cache := &tokenCache{
		clock:     clock.RealClock{},
		store:     store,
		audience:  audience,
		refresher: refresher,
	}
	return cache, nil
}

var _ CachingTokenSource = &tokenCache{}

// Token returns a valid access token, if available.
func (t *tokenCache) Token() (*xoauth2.Token, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// use the cached access token if it isn't expired
	if t.token != nil && t.validateAccessToken(*t.token) {
		return t.token, nil
	}

	// load from the store and use the access token if it isn't expired
	grant, err := t.store.LoadGrant(t.audience)
	if err != nil {
		return nil, fmt.Errorf("LoadGrant: %v", err)
	}
	t.token = grant.Token
	if t.token != nil && t.validateAccessToken(*t.token) {
		return t.token, nil
	}

	// obtain and cache a fresh access token
	grant, err = t.refresher.Refresh(grant)
	if err != nil {
		return nil, fmt.Errorf("RefreshGrant: %v", err)
	}
	t.token = grant.Token
	err = t.store.SaveGrant(t.audience, *grant)
	if err != nil {
		// TODO log rather than throw
		return nil, fmt.Errorf("SaveGrant: %v", err)
	}

	return t.token, nil
}

// InvalidateToken clears the access token (likely due to a response from the resource server).
// Note that the token within the grant may contain a refresh token which should survive.
func (t *tokenCache) InvalidateToken() error {
	t.lock.Lock()
	defer t.lock.Unlock()

	previous := t.token
	t.token = nil

	// clear from the store the access token that was returned earlier (unless the store has since been updated)
	if previous == nil || previous.AccessToken == "" {
		return nil
	}
	grant, err := t.store.LoadGrant(t.audience)
	if err != nil {
		return fmt.Errorf("LoadGrant: %v", err)
	}
	if grant.Token != nil && grant.Token.AccessToken == previous.AccessToken {
		grant.Token.Expiry = time.Unix(0, 0).Add(expiryDelta)
		err = t.store.SaveGrant(t.audience, *grant)
		if err != nil {
			// TODO log rather than throw
			return fmt.Errorf("SaveGrant: %v", err)
		}
	}
	return nil
}

// validateAccessToken checks the validity of the cached access token
func (t *tokenCache) validateAccessToken(token xoauth2.Token) bool {
	if token.AccessToken == "" {
		return false
	}
	if !token.Expiry.IsZero() && t.clock.Now().After(token.Expiry.Round(0).Add(-expiryDelta)) {
		return false
	}
	return true
}
